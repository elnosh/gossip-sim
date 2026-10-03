//! How each implementation answers gossip queries from a connecting peer.
//!
//! Policies only produce plaintext messages tagged with an output stream; the [`crate::peer`]
//! applies pacing, encryption and accounting.

mod cln;
mod eclair;
mod ldk;
mod lnd;

use bitcoin::constants::ChainHash;
use corpus::{msg_type, PeerView, ViewChan};
use lightning::ln::msgs::{
	GossipTimestampFilter, QueryChannelRange, QueryShortChannelIds, ReplyChannelRange,
	ReplyShortChannelIdsEnd, WarningMessage,
};
use lightning::ln::types::ChannelId;
use lightning::routing::gossip::NodeId;
use rand_chacha::ChaCha8Rng;
use std::collections::BTreeSet;

use crate::link::SimTime;
use crate::profile::{Kind, Profile};
use crate::wire::{payload, Bytes};

/// Output streams of a simulated peer. Paced streams share the peer's limiter round-robin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stream {
	/// Query responses, paced.
	Query = 0,
	/// Sent as fast as the link allows (CLN's `reply_channel_range`).
	Unpaced = 1,
	/// Timestamp-filter backlog, paced.
	Backlog = 2,
}

pub struct OutMsg {
	pub ty: u16,
	pub bytes: Bytes,
}

#[derive(Default)]
pub struct Out {
	pub items: Vec<(Stream, OutMsg)>,
}

impl Out {
	pub fn push(&mut self, stream: Stream, ty: u16, bytes: Bytes) {
		self.items.push((stream, OutMsg { ty, bytes }));
	}
}

pub struct Ctx<'a> {
	pub now: SimTime,
	pub now_unix: u32,
	pub view: &'a PeerView,
	pub chain_hash: ChainHash,
	pub rng: &'a mut ChaCha8Rng,
}

pub trait ResponderPolicy: Send {
	/// The `gossip_timestamp_filter` this peer sends after `init`, as (first_timestamp, range).
	fn own_filter(&self, now_unix: u32) -> Option<(u32, u32)>;
	fn on_query_channel_range(&mut self, ctx: &mut Ctx, q: &QueryChannelRange, out: &mut Out);
	fn on_query_short_channel_ids(&mut self, ctx: &mut Ctx, q: &QueryShortChannelIds, out: &mut Out);
	fn on_gossip_timestamp_filter(&mut self, ctx: &mut Ctx, f: &GossipTimestampFilter, out: &mut Out);
	/// Called when a message produced by this policy is handed to the link.
	fn on_sent(&mut self, _ty: u16) {}
}

pub fn make(profile: &Profile) -> Box<dyn ResponderPolicy> {
	match profile.kind {
		Kind::Lnd => Box::new(lnd::Lnd::new(profile.clone())),
		Kind::Cln => Box::new(cln::Cln::new(profile.clone())),
		Kind::Eclair => Box::new(eclair::Eclair::new(profile.clone())),
		Kind::Ldk => Box::new(ldk::Ldk::new(profile.clone())),
	}
}

// ---- shared helpers --------------------------------------------------------------------------

pub(crate) const WANT_TIMESTAMPS: u64 = 1;
pub(crate) const WANT_CHECKSUMS: u64 = 2;

pub(crate) const QF_ANN: u64 = 1;
pub(crate) const QF_UPD1: u64 = 2;
pub(crate) const QF_UPD2: u64 = 4;
pub(crate) const QF_NODE1: u64 = 8;
pub(crate) const QF_NODE2: u64 = 16;
pub(crate) const QF_ALL: u64 = 0x1f;

pub(crate) fn height(scid: u64) -> u32 {
	(scid >> 40) as u32
}

/// End block (exclusive) of a range query, capped at u32::MAX.
pub(crate) fn query_end(q: &QueryChannelRange) -> u32 {
	q.first_blocknum.checked_add(q.number_of_blocks).unwrap_or(u32::MAX)
}

pub(crate) fn wanted_options(q: &QueryChannelRange, p: &Profile) -> (bool, bool) {
	let f = q.query_option_flags.unwrap_or(0);
	(f & WANT_TIMESTAMPS != 0 && p.supports_timestamps, f & WANT_CHECKSUMS != 0 && p.supports_checksums)
}

pub(crate) fn reply_range(
	ctx: &Ctx, first: u32, num: u32, complete: bool, chans: &[(u64, &ViewChan)], ts: bool, cs: bool,
) -> Bytes {
	payload(&ReplyChannelRange {
		chain_hash: ctx.chain_hash,
		first_blocknum: first,
		number_of_blocks: num,
		sync_complete: complete,
		short_channel_ids: chans.iter().map(|(s, _)| *s).collect(),
		timestamps: ts.then(|| chans.iter().map(|(_, c)| (c.timestamp(0), c.timestamp(1))).collect()),
		checksums: cs.then(|| chans.iter().map(|(_, c)| (c.checksum(0), c.checksum(1))).collect()),
	})
}

pub(crate) fn reply_end(ctx: &Ctx, full: bool) -> Bytes {
	payload(&ReplyShortChannelIdsEnd { chain_hash: ctx.chain_hash, full_information: full })
}

pub(crate) fn warning(text: &str) -> Bytes {
	payload(&WarningMessage { channel_id: ChannelId::new_zero(), data: text.to_string() })
}

/// Splits range-query results into replies of at most `limit` entries, keeping each block in one
/// reply unless a single block exceeds `limit` (CLN and Eclair behaviour). Each reply covers
/// [first, next reply's first block); the last one extends to the query end.
pub(crate) fn block_aligned_replies(
	ctx: &Ctx, q: &QueryChannelRange, chans: &[(u64, &ViewChan)], limit: usize, ts: bool, cs: bool,
	stream: Stream, out: &mut Out,
) {
	let end = query_end(q);
	let limit = limit.max(1);
	let mut first = q.first_blocknum;
	let mut idx = 0;
	loop {
		let mut stop = (idx + limit).min(chans.len());
		if stop < chans.len() && height(chans[stop].0) == height(chans[stop - 1].0) {
			let h = height(chans[stop].0);
			let mut k = stop;
			while k > idx && height(chans[k - 1].0) == h {
				k -= 1;
			}
			if k > idx {
				stop = k;
			}
		}
		let last = stop == chans.len();
		let num = if last { end - first } else { height(chans[stop].0) - first };
		out.push(
			stream,
			msg_type::REPLY_CHANNEL_RANGE,
			reply_range(ctx, first, num, last, &chans[idx..stop], ts, cs),
		);
		if last {
			break;
		}
		first = height(chans[stop].0);
		idx = stop;
	}
}

pub(crate) fn range_chans<'a>(view: &'a PeerView, q: &QueryChannelRange) -> Vec<(u64, &'a ViewChan)> {
	view.chans_in_blocks(q.first_blocknum, q.number_of_blocks).map(|(s, c)| (*s, c)).collect()
}

/// Responds to `query_short_channel_ids` with per-scid flags (CLN and Eclair order). CLN batches
/// node announcements after all channels; Eclair interleaves them.
pub(crate) fn flagged_scid_response(
	ctx: &Ctx, q: &QueryShortChannelIds, nodes_last: bool, out: &mut Out,
) {
	let mut nodes_sent: BTreeSet<NodeId> = BTreeSet::new();
	let mut nodes_later: Vec<NodeId> = Vec::new();
	let mut push_node = |id: NodeId, out: &mut Out, nodes_later: &mut Vec<NodeId>| {
		if nodes_last {
			nodes_later.push(id);
		} else if nodes_sent.insert(id) {
			if let Some(n) = ctx.view.nodes.get(&id) {
				out.push(Stream::Query, msg_type::NODE_ANNOUNCEMENT, n.bytes.clone());
			}
		}
	};
	let mut seen = BTreeSet::new();
	for (i, scid) in q.short_channel_ids.iter().enumerate() {
		if !seen.insert(*scid) {
			continue;
		}
		let flags = q.query_flags.as_ref().and_then(|f| f.get(i).copied()).unwrap_or(QF_ALL);
		let Some(c) = ctx.view.chans.get(scid) else { continue };
		if flags & QF_ANN != 0 {
			out.push(Stream::Query, msg_type::CHANNEL_ANNOUNCEMENT, c.entry.ann.clone());
		}
		for (d, bit) in [(0, QF_UPD1), (1, QF_UPD2)] {
			if flags & bit != 0 {
				if let Some(u) = c.upd(d) {
					out.push(Stream::Query, msg_type::CHANNEL_UPDATE, u.bytes.clone());
				}
			}
		}
		if flags & QF_NODE1 != 0 {
			push_node(c.entry.node1, out, &mut nodes_later);
		}
		if flags & QF_NODE2 != 0 {
			push_node(c.entry.node2, out, &mut nodes_later);
		}
	}
	if nodes_last {
		nodes_later.sort();
		nodes_later.dedup();
		for id in nodes_later {
			if let Some(n) = ctx.view.nodes.get(&id) {
				out.push(Stream::Query, msg_type::NODE_ANNOUNCEMENT, n.bytes.clone());
			}
		}
	}
	out.push(Stream::Query, msg_type::REPLY_SHORT_CHANNEL_IDS_END, reply_end(ctx, true));
}

/// Inclusive timestamp window of a filter, as CLN computes it.
pub(crate) fn filter_window_inclusive(f: &GossipTimestampFilter) -> (u32, u32) {
	let min = f.first_timestamp;
	let max = (f.first_timestamp as u64 + f.timestamp_range as u64).saturating_sub(1);
	(min, if max < min as u64 { u32::MAX } else { max.min(u32::MAX as u64) as u32 })
}

#[cfg(test)]
pub(crate) mod tests;
