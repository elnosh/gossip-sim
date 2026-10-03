//! LDK (routing/gossip.rs `handle_query_channel_range`, ln/peer_handler.rs backfill).

use corpus::msg_type;
use lightning::ln::msgs::{GossipTimestampFilter, QueryChannelRange, QueryShortChannelIds};

use super::*;

pub struct Ldk {
	p: Profile,
	filter_seen: bool,
}

impl Ldk {
	pub fn new(p: Profile) -> Self {
		Ldk { p, filter_seen: false }
	}
}

impl ResponderPolicy for Ldk {
	fn own_filter(&self, now_unix: u32) -> Option<(u32, u32)> {
		self.p.own_filter.resolve(now_unix)
	}

	/// Batches of 8000 SCIDs regardless of block boundaries; each reply starts where the previous
	/// one ended (CLN < 0.10 compatibility) and spans up to the block of its last SCID. No TLVs.
	fn on_query_channel_range(&mut self, ctx: &mut Ctx, q: &QueryChannelRange, out: &mut Out) {
		let end = query_end(q);
		if q.number_of_blocks == 0 {
			out.push(Stream::Query, msg_type::REPLY_CHANNEL_RANGE, reply_range(ctx, q.first_blocknum, 0, true, &[], false, false));
			return;
		}
		let chans = range_chans(ctx.view, q);
		let batches: Vec<&[(u64, &ViewChan)]> =
			if chans.is_empty() { vec![&[]] } else { chans.chunks(self.p.range_chunk_scids.max(1)).collect() };
		let mut prev_end = q.first_blocknum;
		let n = batches.len();
		for (i, batch) in batches.into_iter().enumerate() {
			let first = prev_end;
			let (complete, num) =
				if i == n - 1 { (true, end - first) } else { (false, height(batch.last().unwrap().0) - first) };
			prev_end = first + num;
			out.push(Stream::Query, msg_type::REPLY_CHANNEL_RANGE, reply_range(ctx, first, num, complete, batch, false, false));
		}
	}

	/// Not implemented in LDK: no messages and no `reply_short_channel_ids_end`.
	fn on_query_short_channel_ids(&mut self, _ctx: &mut Ctx, _q: &QueryShortChannelIds, _out: &mut Out) {}

	/// Only the first filter counts. If it is older than now - 6h, the whole graph is replayed in
	/// SCID order (announcement + updates), then all node announcements; timestamps are otherwise
	/// ignored. Pacing (32 messages per pong) is the profile's limiter.
	fn on_gossip_timestamp_filter(&mut self, ctx: &mut Ctx, f: &GossipTimestampFilter, out: &mut Out) {
		if self.filter_seen {
			return;
		}
		self.filter_seen = true;
		let threshold = ctx.now_unix.saturating_sub(self.p.full_sync_threshold_secs);
		if f.first_timestamp > threshold {
			return;
		}
		for c in ctx.view.chans.values() {
			out.push(Stream::Backlog, msg_type::CHANNEL_ANNOUNCEMENT, c.entry.ann.clone());
			for d in 0..2 {
				if let Some(u) = c.upd(d) {
					out.push(Stream::Backlog, msg_type::CHANNEL_UPDATE, u.bytes.clone());
				}
			}
		}
		for n in ctx.view.nodes.values() {
			out.push(Stream::Backlog, msg_type::NODE_ANNOUNCEMENT, n.bytes.clone());
		}
	}
}
