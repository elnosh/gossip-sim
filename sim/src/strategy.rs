//! Sync strategies, plugged into LDK as a `RoutingMessageHandler` wrapper.
//!
//! [`StrategyHandler`] delegates graph updates and LDK's responder side to a real
//! `P2PGossipSync`, and routes the sync-driving hooks (`peer_connected`, query replies) to a
//! [`Strategy`]. `PeerManager` already serializes the query events a strategy emits.

use bitcoin::secp256k1::PublicKey;
use lightning::ln::msgs::{
	BaseMessageHandler, ChannelAnnouncement, ChannelUpdate, Init, LightningError, MessageSendEvent,
	NodeAnnouncement, QueryChannelRange, QueryShortChannelIds, ReplyChannelRange,
	ReplyShortChannelIdsEnd, RoutingMessageHandler,
};
use lightning::routing::gossip::NodeId;
use lightning::types::features::{InitFeatures, NodeFeatures};
use serde::Deserialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::ldk::{Gossip, Graph};

mod baseline;
mod filter_since;
mod range_scids;

pub use baseline::Baseline;
pub use filter_since::FilterSinceLastSeen;
pub use range_scids::RangeThenScids;

#[derive(Debug, Clone, Copy)]
pub enum Applied {
	Chan(u64),
	Upd { scid: u64, dir: u8, timestamp: u32 },
	Node(NodeId),
}

pub trait Strategy: Send {
	fn on_peer_connected(&mut self, peer: PublicKey, init: &Init, graph: &Graph, now: u64, out: &mut Vec<MessageSendEvent>);
	fn on_peer_disconnected(&mut self, _peer: PublicKey) {}
	fn on_reply_channel_range(
		&mut self, _peer: PublicKey, _msg: ReplyChannelRange, _graph: &Graph, _now: u64,
		_out: &mut Vec<MessageSendEvent>,
	) {
	}
	fn on_reply_short_channel_ids_end(
		&mut self, _peer: PublicKey, _msg: ReplyShortChannelIdsEnd, _graph: &Graph, _now: u64,
		_out: &mut Vec<MessageSendEvent>,
	) {
	}
	/// Called once per simulated second.
	fn on_timer(&mut self, _graph: &Graph, _now: u64, _out: &mut Vec<MessageSendEvent>) {}
	fn on_applied(&mut self, _from: Option<PublicKey>, _what: Applied, _now: u64) {}
	/// Called for every gossip message received from a peer, applied or rejected.
	fn on_gossip_from(&mut self, _from: PublicKey, _now: u64) {}
	/// No queries outstanding or queued (used for convergence and early stop).
	fn is_idle(&self) -> bool {
		true
	}
}

/// A strategy and its parameters, as written in an experiment file: either a bare name or a
/// table with `name` plus parameters.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum StrategySpec {
	Name(String),
	Table(StrategyParams),
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyParams {
	pub name: String,
	/// Label used in outputs; defaults to the name plus non-default parameters.
	pub label: Option<String>,
	/// baseline / filter_since_last_seen: peers that get the wide filter.
	pub full_peers: Option<usize>,
	/// filter_since_last_seen: how far before the newest known update the filter starts.
	pub margin_secs: Option<u32>,
	/// range_then_scids: peers asked with `query_channel_range`.
	pub query_peers: Option<usize>,
	/// range_then_scids: ask for checksums too.
	pub use_checksums: Option<bool>,
	/// range_then_scids: SCIDs per `query_short_channel_ids`.
	pub batch: Option<usize>,
	/// range_then_scids: give up on a peer's query after this long without progress.
	pub query_timeout_secs: Option<u64>,
	/// range_then_scids: first_timestamp of the filter is now minus this.
	pub filter_lookback_secs: Option<u32>,
	/// range_then_scids: assign scids after all range replies, balancing load across peers.
	pub balance: Option<bool>,
}

impl StrategySpec {
	pub fn params(&self) -> StrategyParams {
		match self {
			StrategySpec::Name(n) => StrategyParams { name: n.clone(), ..Default::default() },
			StrategySpec::Table(p) => p.clone(),
		}
	}

	pub fn label(&self) -> String {
		let p = self.params();
		if let Some(l) = p.label {
			return l;
		}
		let mut s = p.name.clone();
		macro_rules! add {
			($($f:ident),*) => { $( if let Some(v) = &p.$f { s.push_str(&format!("-{}{}", stringify!($f), v)); } )* };
		}
		add!(full_peers, margin_secs, query_peers, use_checksums, batch, query_timeout_secs, filter_lookback_secs, balance);
		s
	}

	pub fn build(&self) -> crate::Result<Built> {
		let p = self.params();
		if p.name == "ldk_query_sync" {
			let d = lightning::routing::query_sync::QuerySyncConfig::default();
			return Ok(Built::Native(lightning::routing::query_sync::QuerySyncConfig {
				query_peers: p.query_peers.unwrap_or(d.query_peers),
				use_checksums: p.use_checksums.unwrap_or(d.use_checksums),
				batch: p.batch.unwrap_or(d.batch),
				query_timeout_secs: p.query_timeout_secs.unwrap_or(d.query_timeout_secs),
				filter_lookback_secs: p.filter_lookback_secs.unwrap_or(d.filter_lookback_secs),
			}));
		}
		Ok(Built::Wrapper(match p.name.as_str() {
			"baseline" => Box::new(Baseline::new(p.full_peers.unwrap_or(5))),
			"filter_since_last_seen" => {
				Box::new(FilterSinceLastSeen::new(p.full_peers.unwrap_or(5), p.margin_secs.unwrap_or(3600)))
			},
			"range_then_scids" => Box::new(RangeThenScids::new(range_scids::Params {
				query_peers: p.query_peers.unwrap_or(3),
				use_checksums: p.use_checksums.unwrap_or(true),
				batch: p.batch.unwrap_or(2000),
				query_timeout_secs: p.query_timeout_secs.unwrap_or(30),
				filter_lookback_secs: p.filter_lookback_secs.unwrap_or(0),
				balance: p.balance.unwrap_or(true),
			})),
			other => return Err(format!("unknown strategy `{other}`").into()),
		}))
	}
}

/// A strategy implemented in the sim (wrapper) or inside the LDK fork's `P2PGossipSync`.
pub enum Built {
	Wrapper(Box<dyn Strategy>),
	/// `P2PGossipSync::with_query_sync`: the ported range_then_scids running in LDK itself.
	Native(lightning::routing::query_sync::QuerySyncConfig),
}

pub struct StrategyHandler {
	inner: Gossip,
	/// None: native mode, everything is delegated to `inner`.
	strat: Option<Mutex<Box<dyn Strategy>>>,
	pending: Mutex<Vec<MessageSendEvent>>,
	pub applied: AtomicU64,
	pub rejected: AtomicU64,
}

fn now() -> u64 {
	lightning::util::sim_clock::unix_now()
}

impl StrategyHandler {
	/// Builds the handler; a native strategy enables the query sync on the inner `P2PGossipSync`.
	pub fn new(inner: Gossip, built: Built) -> StrategyHandler {
		let (inner, strat) = match built {
			Built::Wrapper(s) => (inner, Some(Mutex::new(s))),
			Built::Native(cfg) => (inner.with_query_sync(cfg), None),
		};
		StrategyHandler {
			inner,
			strat,
			pending: Mutex::new(Vec::new()),
			applied: AtomicU64::new(0),
			rejected: AtomicU64::new(0),
		}
	}

	pub fn graph(&self) -> &Graph {
		self.inner.network_graph()
	}

	pub fn on_timer(&self) {
		let Some(strat) = &self.strat else { return };
		let mut out = Vec::new();
		strat.lock().unwrap().on_timer(self.graph(), now(), &mut out);
		self.pending.lock().unwrap().extend(out);
	}

	pub fn is_idle(&self) -> bool {
		match &self.strat {
			Some(s) => s.lock().unwrap().is_idle(),
			None => self.inner.query_sync_idle(),
		}
	}

	fn record<T>(&self, from: Option<PublicKey>, r: &Result<T, LightningError>, what: Applied) {
		if r.is_ok() {
			self.applied.fetch_add(1, Ordering::Relaxed);
		} else {
			self.rejected.fetch_add(1, Ordering::Relaxed);
		}
		let Some(strat) = &self.strat else { return };
		let mut strat = strat.lock().unwrap();
		if let Some(f) = from {
			strat.on_gossip_from(f, now());
		}
		if r.is_ok() {
			strat.on_applied(from, what, now());
		}
	}
}

impl BaseMessageHandler for StrategyHandler {
	fn get_and_clear_pending_msg_events(&self) -> Vec<MessageSendEvent> {
		let mut v = self.inner.get_and_clear_pending_msg_events();
		v.append(&mut self.pending.lock().unwrap());
		v
	}

	fn peer_disconnected(&self, their_node_id: PublicKey) {
		self.inner.peer_disconnected(their_node_id);
		if let Some(s) = &self.strat {
			s.lock().unwrap().on_peer_disconnected(their_node_id);
		}
	}

	fn provided_node_features(&self) -> NodeFeatures {
		self.inner.provided_node_features()
	}

	fn provided_init_features(&self, their_node_id: PublicKey) -> InitFeatures {
		self.inner.provided_init_features(their_node_id)
	}

	fn peer_connected(&self, their_node_id: PublicKey, msg: &Init, inbound: bool) -> Result<(), ()> {
		let Some(strat) = &self.strat else { return self.inner.peer_connected(their_node_id, msg, inbound) };
		let mut out = Vec::new();
		strat.lock().unwrap().on_peer_connected(their_node_id, msg, self.graph(), now(), &mut out);
		self.pending.lock().unwrap().extend(out);
		Ok(())
	}
}

impl RoutingMessageHandler for StrategyHandler {
	fn handle_node_announcement(
		&self, their_node_id: Option<PublicKey>, msg: &NodeAnnouncement,
	) -> Result<bool, LightningError> {
		let r = self.inner.handle_node_announcement(their_node_id, msg);
		self.record(their_node_id, &r, Applied::Node(msg.contents.node_id));
		r
	}

	fn handle_channel_announcement(
		&self, their_node_id: Option<PublicKey>, msg: &ChannelAnnouncement,
	) -> Result<bool, LightningError> {
		let r = self.inner.handle_channel_announcement(their_node_id, msg);
		self.record(their_node_id, &r, Applied::Chan(msg.contents.short_channel_id));
		r
	}

	fn handle_channel_update(
		&self, their_node_id: Option<PublicKey>, msg: &ChannelUpdate,
	) -> Result<Option<(NodeId, NodeId)>, LightningError> {
		let r = self.inner.handle_channel_update(their_node_id, msg);
		let what = Applied::Upd {
			scid: msg.contents.short_channel_id,
			dir: msg.contents.channel_flags & 1,
			timestamp: msg.contents.timestamp,
		};
		self.record(their_node_id, &r, what);
		r
	}

	fn get_next_channel_announcement(
		&self, starting_point: u64,
	) -> Option<(ChannelAnnouncement, Option<ChannelUpdate>, Option<ChannelUpdate>)> {
		self.inner.get_next_channel_announcement(starting_point)
	}

	fn get_next_node_announcement(&self, starting_point: Option<&NodeId>) -> Option<NodeAnnouncement> {
		self.inner.get_next_node_announcement(starting_point)
	}

	fn handle_reply_channel_range(
		&self, their_node_id: PublicKey, msg: ReplyChannelRange,
	) -> Result<(), LightningError> {
		let Some(strat) = &self.strat else { return self.inner.handle_reply_channel_range(their_node_id, msg) };
		let mut out = Vec::new();
		strat.lock().unwrap().on_reply_channel_range(their_node_id, msg, self.graph(), now(), &mut out);
		self.pending.lock().unwrap().extend(out);
		Ok(())
	}

	fn handle_reply_short_channel_ids_end(
		&self, their_node_id: PublicKey, msg: ReplyShortChannelIdsEnd,
	) -> Result<(), LightningError> {
		let Some(strat) = &self.strat else { return self.inner.handle_reply_short_channel_ids_end(their_node_id, msg) };
		let mut out = Vec::new();
		strat.lock().unwrap().on_reply_short_channel_ids_end(their_node_id, msg, self.graph(), now(), &mut out);
		self.pending.lock().unwrap().extend(out);
		Ok(())
	}

	fn handle_query_channel_range(
		&self, their_node_id: PublicKey, msg: QueryChannelRange,
	) -> Result<(), LightningError> {
		self.inner.handle_query_channel_range(their_node_id, msg)
	}

	fn handle_query_short_channel_ids(
		&self, their_node_id: PublicKey, msg: QueryShortChannelIds,
	) -> Result<(), LightningError> {
		self.inner.handle_query_short_channel_ids(their_node_id, msg)
	}

	fn processing_queue_high(&self) -> bool {
		self.inner.processing_queue_high()
	}
}

/// Newest channel_update timestamp in the graph, if any.
pub fn newest_update(graph: &Graph) -> Option<u32> {
	let ro = graph.read_only();
	ro.channels()
		.unordered_iter()
		.flat_map(|(_, c)| [c.one_to_two.as_ref(), c.two_to_one.as_ref()])
		.flatten()
		.map(|u| u.last_update)
		.max()
}

pub fn filter_event(node_id: PublicKey, graph: &Graph, first: u32) -> MessageSendEvent {
	MessageSendEvent::SendGossipTimestampFilter {
		node_id,
		msg: lightning::ln::msgs::GossipTimestampFilter {
			chain_hash: graph.get_chain_hash(),
			first_timestamp: first,
			timestamp_range: u32::MAX,
		},
	}
}
