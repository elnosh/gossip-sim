//! Eclair (router/Sync.scala, io/PeerConnection.scala, router/StaleChannels.scala).

use corpus::msg_type;
use lightning::ln::msgs::{GossipTimestampFilter, QueryChannelRange, QueryShortChannelIds};
use std::collections::VecDeque;

use super::*;

pub struct Eclair {
	p: Profile,
	/// Times of the last accepted queries (`RateLimiter`'s ring).
	accepted: VecDeque<SimTime>,
	/// A `query_short_channel_ids` was answered but its `reply_short_channel_ids_end` not sent yet.
	scid_query_pending: bool,
}

impl Eclair {
	pub fn new(p: Profile) -> Self {
		Eclair { p, accepted: VecDeque::new(), scid_query_pending: false }
	}

	/// `gossipQueriesRateLimiter.tryAcquire`: at most `queries_per_sec` queries of either kind
	/// in any rolling second; excess ones are dropped silently.
	fn try_acquire(&mut self, now: SimTime) -> bool {
		let n = self.p.queries_per_sec as usize;
		if n == 0 {
			return true;
		}
		if self.accepted.len() == n {
			if now - self.accepted[0] <= 1_000_000 {
				return false;
			}
			self.accepted.pop_front();
		}
		self.accepted.push_back(now);
		true
	}
}

impl ResponderPolicy for Eclair {
	fn own_filter(&self, now_unix: u32) -> Option<(u32, u32)> {
		self.p.own_filter.resolve(now_unix)
	}

	/// `handleQueryChannelRange`: chunks of `channel-range-chunk-size` SCIDs grouped by block,
	/// timestamps and checksums when asked. Queries over the rate limit are dropped. Assumes
	/// uncompressed encoding.
	fn on_query_channel_range(&mut self, ctx: &mut Ctx, q: &QueryChannelRange, out: &mut Out) {
		if !self.try_acquire(ctx.now) {
			return;
		}
		let (ts, cs) = wanted_options(q, &self.p);
		let chans = range_chans(ctx.view, q);
		block_aligned_replies(ctx, q, &chans, self.p.range_chunk_scids, ts, cs, Stream::Query, out);
	}

	/// `handleQueryShortChannelIds`: per-scid flags (none means everything), node announcements
	/// interleaved and deduplicated via `nodesSent`. Dropped silently (`PeerConnection`): over
	/// the rate limit, a flag count different from the scid count, or a query sent before the
	/// previous one's `reply_short_channel_ids_end`.
	fn on_query_short_channel_ids(&mut self, ctx: &mut Ctx, q: &QueryShortChannelIds, out: &mut Out) {
		if !self.try_acquire(ctx.now) {
			return;
		}
		if q.query_flags.as_ref().is_some_and(|f| f.len() != q.short_channel_ids.len()) {
			return;
		}
		if self.scid_query_pending {
			return;
		}
		self.scid_query_pending = true;
		flagged_scid_response(ctx, q, false, out);
	}

	/// Eclair stores the filter and applies it to future rebroadcasts only: no backlog.
	fn on_gossip_timestamp_filter(&mut self, _ctx: &mut Ctx, _f: &GossipTimestampFilter, _out: &mut Out) {}

	fn on_sent(&mut self, ty: u16) {
		if ty == msg_type::REPLY_SHORT_CHANNEL_IDS_END {
			self.scid_query_pending = false;
		}
	}
}
