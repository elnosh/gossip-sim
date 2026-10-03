//! Range query with timestamps (and checksums) to a few peers, then `query_short_channel_ids`
//! for only what is missing or newer, with per-scid query flags.
//!
//! - An update is wanted if a peer offers a timestamp newer than ours, not stale, and, when
//!   checksums are available, either its contents differ or ours is close to going stale
//!   (Eclair's rule).
//! - The `gossip_timestamp_filter` to a range-queried peer is sent once its first reply arrives:
//!   now minus `filter_lookback_secs` if the reply carries timestamps, otherwise two weeks ago.
//!   A reply without timestamps comes from an LDK peer, which also ignores scid queries but
//!   replays its whole graph for an old filter. Other peers get the short filter on connect.
//! - `balance = true` waits for all range replies, then assigns each wanted scid to the least
//!   loaded peer holding its freshest version. `balance = false` asks the first peer that offers
//!   something newer, as replies arrive.
//! - A peer that makes no progress for `query_timeout_secs` is dropped and its work moves to
//!   another peer that offered the same scids.

use corpus::checksum::channel_update_checksum;
use lightning::util::ser::Writeable;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use super::*;
use crate::responders::{QF_ANN, QF_NODE1, QF_NODE2, QF_UPD1, QF_UPD2};

const STALE_SECS: u64 = 14 * 24 * 3600;
const ALMOST_STALE_SECS: u64 = 10 * 24 * 3600;
const TWO_WEEKS: u32 = 14 * 24 * 3600;
const UPD_BITS: [u64; 2] = [QF_UPD1, QF_UPD2];
/// Balanced mode waits this long after the first range query for more range peers to connect.
const RANGE_GRACE_SECS: u64 = 5;

#[derive(Debug, Clone)]
pub struct Params {
	pub query_peers: usize,
	pub use_checksums: bool,
	pub batch: usize,
	pub query_timeout_secs: u64,
	pub filter_lookback_secs: u32,
	pub balance: bool,
}

#[derive(Default)]
struct PeerState {
	/// Range query outstanding since this time.
	ranging: Option<u64>,
	filter_sent: bool,
	/// The peer's range replies carry no timestamps (an LDK peer): never send it scid queries.
	no_timestamps: bool,
	queue: VecDeque<(u64, u64)>,
	outstanding: Option<Vec<(u64, u64)>>,
	last_progress: u64,
	failed: bool,
	disconnected: bool,
}

/// What one peer said about one scid in its range reply.
#[derive(Clone, Copy)]
struct Offer {
	peer: PublicKey,
	ts: Option<[u32; 2]>,
	cs: Option<[u32; 2]>,
}

pub struct RangeThenScids {
	p: Params,
	peers: BTreeMap<PublicKey, PeerState>,
	range_peers: usize,
	ann_requested: BTreeSet<u64>,
	upd_requested: BTreeMap<(u64, usize), u32>,
	offers: BTreeMap<u64, Vec<Offer>>,
	/// Scids offered since the last balanced assignment.
	unassigned: BTreeSet<u64>,
	first_range_query: Option<u64>,
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
			unassigned: BTreeSet::new(),
			first_range_query: None,
		}
	}

	fn send_filter(&mut self, peer: PublicKey, wide: bool, graph: &Graph, now: u64, out: &mut Vec<MessageSendEvent>) {
		let Some(st) = self.peers.get_mut(&peer) else { return };
		if st.filter_sent {
			return;
		}
		st.filter_sent = true;
		let first = if wide { (now as u32).saturating_sub(TWO_WEEKS) } else { (now as u32).saturating_sub(self.p.filter_lookback_secs) };
		out.push(filter_event(peer, graph, first));
	}

	/// Drops the parts of a queued request that arrived from elsewhere in the meantime.
	fn still_wanted(&self, scid: u64, flags: u64, graph: &Graph) -> u64 {
		let ro = graph.read_only();
		let Some(info) = ro.channel(scid) else { return flags };
		let mut f = flags & !(QF_ANN | QF_NODE1 | QF_NODE2);
		for d in 0..2 {
			let ours = if d == 0 { info.one_to_two.as_ref() } else { info.two_to_one.as_ref() };
			let requested = self.upd_requested.get(&(scid, d)).copied().unwrap_or(u32::MAX);
			if ours.map_or(false, |u| u.last_update >= requested) {
				f &= !UPD_BITS[d];
			}
		}
		f
	}

	fn try_send(&mut self, peer: PublicKey, graph: &Graph, now: u64, out: &mut Vec<MessageSendEvent>) {
		let batch = self.p.batch;
		let Some(st) = self.peers.get(&peer) else { return };
		if st.failed || st.outstanding.is_some() || st.queue.is_empty() {
			return;
		}
		let mut items: BTreeMap<u64, u64> = BTreeMap::new();
		while items.len() < batch {
			let Some((scid, flags)) = self.peers.get_mut(&peer).unwrap().queue.pop_front() else { break };
			let flags = self.still_wanted(scid, flags, graph);
			if flags != 0 {
				*items.entry(scid).or_insert(0) |= flags;
			}
		}
		if items.is_empty() {
			return;
		}
		let st = self.peers.get_mut(&peer).unwrap();
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

	fn usable(&self, o: &Offer) -> bool {
		self.peers.get(&o.peer).map_or(false, |s| !s.failed && !s.no_timestamps)
	}

	/// Flags worth asking `o.peer` for, given our graph and the versions already requested.
	fn wanted_from(&self, scid: u64, o: &Offer, graph: &Graph, now: u64) -> u64 {
		let ro = graph.read_only();
		let info = ro.channel(scid);
		let mut flags = 0;
		if info.is_none() && !self.ann_requested.contains(&scid) {
			flags |= QF_ANN | QF_NODE1 | QF_NODE2;
		}
		for d in 0..2 {
			let Some(ts) = o.ts else {
				// No timestamps: for a new channel take whatever the peer has.
				if info.is_none() {
					flags |= UPD_BITS[d];
				}
				continue;
			};
			let theirs = ts[d];
			if theirs == 0 || now.saturating_sub(theirs as u64) > STALE_SECS {
				continue;
			}
			let ours = info.and_then(|i| if d == 0 { i.one_to_two.as_ref() } else { i.two_to_one.as_ref() });
			let ours_ts = ours.map_or(0, |u| u.last_update);
			let requested = self.upd_requested.get(&(scid, d)).copied().unwrap_or(0);
			if theirs <= ours_ts.max(requested) {
				continue;
			}
			if let (Some(cs), Some(u)) = (o.cs, ours) {
				let same =
					u.last_update_message.as_ref().map_or(false, |m| channel_update_checksum(&m.encode()) == cs[d]);
				if same && now.saturating_sub(ours_ts as u64) < ALMOST_STALE_SECS {
					continue;
				}
			}
			flags |= UPD_BITS[d];
		}
		flags
	}

	fn enqueue(&mut self, peer: PublicKey, scid: u64, flags: u64, ts: Option<[u32; 2]>) {
		if flags & QF_ANN != 0 {
			self.ann_requested.insert(scid);
		}
		if let Some(ts) = ts {
			for d in 0..2 {
				if flags & UPD_BITS[d] != 0 {
					self.upd_requested.insert((scid, d), ts[d]);
				}
			}
		}
		self.peers.get_mut(&peer).unwrap().queue.push_back((scid, flags));
	}

	/// Balanced mode: whenever no range query is outstanding, give each newly offered scid to the
	/// least loaded usable peer whose offer covers the most wanted parts.
	fn assign_all(&mut self, graph: &Graph, now: u64, out: &mut Vec<MessageSendEvent>) {
		if self.unassigned.is_empty() || self.peers.values().any(|s| s.ranging.is_some()) {
			return;
		}
		let all_slots_used = self.range_peers >= self.p.query_peers;
		let grace_over = self.first_range_query.map_or(true, |t| now >= t + RANGE_GRACE_SECS);
		if !all_slots_used && !grace_over {
			return;
		}
		let mut load: BTreeMap<PublicKey, usize> =
			self.peers.iter().map(|(p, s)| (*p, s.queue.len())).collect();
		let offers = std::mem::take(&mut self.offers);
		for scid in std::mem::take(&mut self.unassigned) {
			let offs = &offers[&scid];
			let scid = &scid;
			let mut best: Option<(u32, usize, Offer, u64)> = None;
			for o in offs.iter().filter(|o| self.usable(o)) {
				let flags = self.wanted_from(*scid, o, graph, now);
				if flags == 0 {
					continue;
				}
				let l = load.get(&o.peer).copied().unwrap_or(0);
				let key = (flags.count_ones(), usize::MAX - l);
				if best.map_or(true, |(c, bl, _, _)| key > (c, bl)) {
					best = Some((key.0, key.1, *o, flags));
				}
			}
			if let Some((_, _, o, flags)) = best {
				*load.entry(o.peer).or_insert(0) += 1;
				self.enqueue(o.peer, *scid, flags, o.ts);
			}
		}
		self.offers = offers;
		let peers: Vec<PublicKey> = self.peers.keys().copied().collect();
		for p in peers {
			self.try_send(p, graph, now, out);
		}
	}

	fn fail_peer(&mut self, peer: PublicKey, graph: &Graph, now: u64, out: &mut Vec<MessageSendEvent>) {
		self.send_filter(peer, false, graph, now, out);
		let Some(st) = self.peers.get_mut(&peer) else { return };
		st.failed = true;
		st.ranging = None;
		let mut work: Vec<(u64, u64)> = st.outstanding.take().unwrap_or_default();
		work.extend(st.queue.drain(..));
		for (scid, flags) in work {
			let alt = self.offers.get(&scid).and_then(|v| v.iter().find(|o| o.peer != peer && self.usable(o)).copied());
			if let Some(o) = alt {
				self.peers.get_mut(&o.peer).unwrap().queue.push_back((scid, flags));
			}
		}
		if self.p.balance {
			self.assign_all(graph, now, out);
		}
	}
}

impl Strategy for RangeThenScids {
	fn on_peer_connected(&mut self, peer: PublicKey, init: &Init, graph: &Graph, now: u64, out: &mut Vec<MessageSendEvent>) {
		if !init.features.supports_gossip_queries() {
			return;
		}
		self.peers.insert(peer, PeerState { last_progress: now, ..Default::default() });
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
			self.peers.get_mut(&peer).unwrap().ranging = Some(now);
			self.first_range_query.get_or_insert(now);
		} else {
			self.send_filter(peer, false, graph, now, out);
		}
	}

	fn on_peer_disconnected(&mut self, peer: PublicKey) {
		// Re-routing its work needs the graph and an event sink: the next timer tick does it.
		if let Some(st) = self.peers.get_mut(&peer) {
			st.disconnected = true;
			st.filter_sent = true;
		}
	}

	fn on_reply_channel_range(
		&mut self, peer: PublicKey, msg: ReplyChannelRange, graph: &Graph, now: u64,
		out: &mut Vec<MessageSendEvent>,
	) {
		if !self.peers.contains_key(&peer) {
			return;
		}
		let no_ts = msg.timestamps.is_none();
		if no_ts {
			self.peers.get_mut(&peer).unwrap().no_timestamps = true;
		}
		self.send_filter(peer, no_ts, graph, now, out);
		for (i, scid) in msg.short_channel_ids.iter().enumerate() {
			let o = Offer {
				peer,
				ts: msg.timestamps.as_ref().and_then(|t| t.get(i)).map(|(a, b)| [*a, *b]),
				cs: msg.checksums.as_ref().and_then(|c| c.get(i)).map(|(a, b)| [*a, *b]),
			};
			self.offers.entry(*scid).or_default().push(o);
			if self.p.balance {
				self.unassigned.insert(*scid);
			} else if !no_ts {
				let flags = self.wanted_from(*scid, &o, graph, now);
				if flags != 0 {
					self.enqueue(peer, *scid, flags, o.ts);
				}
			}
		}
		let st = self.peers.get_mut(&peer).unwrap();
		st.last_progress = now;
		if msg.sync_complete {
			st.ranging = None;
		}
		if self.p.balance {
			self.assign_all(graph, now, out);
		} else {
			self.try_send(peer, graph, now, out);
		}
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

	fn on_gossip_from(&mut self, from: PublicKey, now: u64) {
		if let Some(st) = self.peers.get_mut(&from) {
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
					&& (s.disconnected
						|| ((s.outstanding.is_some() || s.ranging.is_some())
							&& now.saturating_sub(s.last_progress) > timeout))
			})
			.map(|(p, _)| *p)
			.collect();
		for p in stuck {
			self.fail_peer(p, graph, now, out);
		}
		if self.p.balance {
			self.assign_all(graph, now, out);
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
