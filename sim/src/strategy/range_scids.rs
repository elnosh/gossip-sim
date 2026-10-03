//! Range query with timestamps (and checksums) to a few peers, then `query_short_channel_ids`
//! for only what is missing or newer, with per-scid query flags. The timestamp filter only asks
//! for live gossip (now minus `filter_lookback_secs`).
//!
//! An update is requested from a peer if its timestamp is newer than both ours and any version
//! already requested from another peer, not stale, and, when checksums are available, either its
//! contents differ or ours is close to going stale (Eclair's rule).

use corpus::checksum::channel_update_checksum;
use lightning::util::ser::Writeable;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use super::*;
use crate::responders::{QF_ANN, QF_NODE1, QF_NODE2, QF_UPD1, QF_UPD2};

const STALE_SECS: u64 = 14 * 24 * 3600;
const ALMOST_STALE_SECS: u64 = 10 * 24 * 3600;

#[derive(Debug, Clone)]
pub struct Params {
	pub query_peers: usize,
	pub use_checksums: bool,
	pub batch: usize,
	pub query_timeout_secs: u64,
	pub filter_lookback_secs: u32,
}

#[derive(Default)]
struct PeerState {
	ranging: Option<u64>,
	queue: VecDeque<(u64, u64)>,
	outstanding: Option<Vec<(u64, u64)>>,
	last_progress: u64,
	failed: bool,
}

pub struct RangeThenScids {
	p: Params,
	peers: BTreeMap<PublicKey, PeerState>,
	range_peers: usize,
	ann_requested: BTreeSet<u64>,
	upd_requested: BTreeMap<(u64, usize), u32>,
	/// Which peers offered which scid with which flags, to re-route work from failed peers.
	offers: BTreeMap<u64, Vec<(PublicKey, u64)>>,
}

impl RangeThenScids {
	pub fn new(p: Params) -> Self {
		RangeThenScids {
			p,
			peers: BTreeMap::new(),
			range_peers: 0,
			ann_requested: BTreeSet::new(),
			upd_requested: BTreeMap::new(),
			offers: BTreeMap::new(),
		}
	}

	fn try_send(&mut self, peer: PublicKey, graph: &Graph, now: u64, out: &mut Vec<MessageSendEvent>) {
		let batch = self.p.batch;
		let Some(st) = self.peers.get_mut(&peer) else { return };
		if st.failed || st.outstanding.is_some() || st.queue.is_empty() {
			return;
		}
		let mut items: BTreeMap<u64, u64> = BTreeMap::new();
		while items.len() < batch {
			let Some((scid, flags)) = st.queue.pop_front() else { break };
			*items.entry(scid).or_insert(0) |= flags;
		}
		let items: Vec<(u64, u64)> = items.into_iter().collect();
		out.push(MessageSendEvent::SendShortIdsQuery {
			node_id: peer,
			msg: QueryShortChannelIds {
				chain_hash: graph.get_chain_hash(),
				short_channel_ids: items.iter().map(|(s, _)| *s).collect(),
				query_flags: Some(items.iter().map(|(_, f)| *f).collect()),
			},
		});
		st.outstanding = Some(items);
		st.last_progress = now;
	}

	fn fail_peer(&mut self, peer: PublicKey) {
		let Some(st) = self.peers.get_mut(&peer) else { return };
		st.failed = true;
		st.ranging = None;
		let mut work: Vec<(u64, u64)> = st.outstanding.take().unwrap_or_default();
		work.extend(st.queue.drain(..));
		for (scid, flags) in work {
			let alt = self.offers.get(&scid).and_then(|v| {
				v.iter().find(|(p, _)| *p != peer && self.peers.get(p).map_or(false, |s| !s.failed)).copied()
			});
			if let Some((alt, alt_flags)) = alt {
				self.peers.get_mut(&alt).unwrap().queue.push_back((scid, flags & alt_flags | flags & QF_ANN));
			}
		}
	}
}

impl Strategy for RangeThenScids {
	fn on_peer_connected(&mut self, peer: PublicKey, init: &Init, graph: &Graph, now: u64, out: &mut Vec<MessageSendEvent>) {
		if !init.features.supports_gossip_queries() {
			return;
		}
		out.push(filter_event(peer, graph, (now as u32).saturating_sub(self.p.filter_lookback_secs)));
		let mut st = PeerState { last_progress: now, ..Default::default() };
		if self.range_peers < self.p.query_peers {
			self.range_peers += 1;
			let flags = 1 | if self.p.use_checksums { 2 } else { 0 };
			out.push(MessageSendEvent::SendChannelRangeQuery {
				node_id: peer,
				msg: QueryChannelRange {
					chain_hash: graph.get_chain_hash(),
					first_blocknum: 0,
					number_of_blocks: u32::MAX,
					query_option_flags: Some(flags),
				},
			});
			st.ranging = Some(now);
		}
		self.peers.insert(peer, st);
	}

	fn on_peer_disconnected(&mut self, peer: PublicKey) {
		self.fail_peer(peer);
	}

	fn on_reply_channel_range(
		&mut self, peer: PublicKey, msg: ReplyChannelRange, graph: &Graph, now: u64,
		out: &mut Vec<MessageSendEvent>,
	) {
		let ro = graph.read_only();
		let mut wanted: Vec<(u64, u64)> = Vec::new();
		for (i, scid) in msg.short_channel_ids.iter().enumerate() {
			let ts = msg.timestamps.as_ref().and_then(|t| t.get(i)).map(|(a, b)| [*a, *b]);
			let cs = msg.checksums.as_ref().and_then(|c| c.get(i)).map(|(a, b)| [*a, *b]);
			let mut flags = 0u64;
			let mut offered = QF_ANN | QF_NODE1 | QF_NODE2;
			match ro.channel(*scid) {
				None => {
					if !self.ann_requested.contains(scid) {
						flags |= QF_ANN | QF_NODE1 | QF_NODE2;
					}
					for (d, bit) in [(0usize, QF_UPD1), (1, QF_UPD2)] {
						let Some(ts) = ts else {
							// No timestamps from this peer: take whatever it has for a new channel.
							offered |= bit;
							flags |= bit;
							continue;
						};
						let theirs = ts[d];
						if theirs == 0 || now.saturating_sub(theirs as u64) > STALE_SECS {
							continue;
						}
						offered |= bit;
						if theirs > self.upd_requested.get(&(*scid, d)).copied().unwrap_or(0) {
							flags |= bit;
							self.upd_requested.insert((*scid, d), theirs);
						}
					}
				},
				Some(info) => {
					let Some(ts) = ts else { continue };
					for (d, bit) in [(0usize, QF_UPD1), (1, QF_UPD2)] {
						let theirs = ts[d];
						if theirs == 0 || now.saturating_sub(theirs as u64) > STALE_SECS {
							continue;
						}
						offered |= bit;
						let ours = if d == 0 { info.one_to_two.as_ref() } else { info.two_to_one.as_ref() };
						let ours_ts = ours.map_or(0, |u| u.last_update);
						let requested = self.upd_requested.get(&(*scid, d)).copied().unwrap_or(0);
						if theirs <= ours_ts.max(requested) {
							continue;
						}
						if let (Some(cs), Some(u)) = (cs, ours) {
							let same = u
								.last_update_message
								.as_ref()
								.map_or(false, |m| channel_update_checksum(&m.encode()) == cs[d]);
							if same && now.saturating_sub(ours_ts as u64) < ALMOST_STALE_SECS {
								continue;
							}
						}
						flags |= bit;
						self.upd_requested.insert((*scid, d), theirs);
					}
				},
			}
			self.offers.entry(*scid).or_default().push((peer, offered));
			if flags != 0 {
				if flags & QF_ANN != 0 {
					self.ann_requested.insert(*scid);
				}
				wanted.push((*scid, flags));
			}
		}
		if let Some(st) = self.peers.get_mut(&peer) {
			st.queue.extend(wanted);
			st.last_progress = now;
			if msg.sync_complete {
				st.ranging = None;
			}
		}
		self.try_send(peer, graph, now, out);
	}

	fn on_reply_short_channel_ids_end(
		&mut self, peer: PublicKey, _msg: ReplyShortChannelIdsEnd, graph: &Graph, now: u64,
		out: &mut Vec<MessageSendEvent>,
	) {
		if let Some(st) = self.peers.get_mut(&peer) {
			st.outstanding = None;
			st.last_progress = now;
		}
		self.try_send(peer, graph, now, out);
	}

	fn on_applied(&mut self, from: Option<PublicKey>, _what: Applied, now: u64) {
		if let Some(st) = from.and_then(|p| self.peers.get_mut(&p)) {
			st.last_progress = now;
		}
	}

	fn on_timer(&mut self, graph: &Graph, now: u64, out: &mut Vec<MessageSendEvent>) {
		let timeout = self.p.query_timeout_secs;
		let stuck: Vec<PublicKey> = self
			.peers
			.iter()
			.filter(|(_, s)| {
				!s.failed
					&& (s.outstanding.is_some() || s.ranging.is_some())
					&& now.saturating_sub(s.last_progress) > timeout
			})
			.map(|(p, _)| *p)
			.collect();
		for p in stuck {
			self.fail_peer(p);
		}
		let peers: Vec<PublicKey> = self.peers.keys().copied().collect();
		for p in peers {
			self.try_send(p, graph, now, out);
		}
	}

	fn is_idle(&self) -> bool {
		self.peers.values().all(|s| s.failed || (s.ranging.is_none() && s.outstanding.is_none() && s.queue.is_empty()))
	}
}
