//! Eclair (router/Sync.scala, io/PeerConnection.scala, router/StaleChannels.scala).

use lightning::ln::msgs::{GossipTimestampFilter, QueryChannelRange, QueryShortChannelIds};

use super::*;

pub struct Eclair {
	p: Profile,
	/// Start of the current one-second rate-limit window and queries accepted in it.
	window: (SimTime, f64),
}

impl Eclair {
	pub fn new(p: Profile) -> Self {
		Eclair { p, window: (0, 0.0) }
	}
}

impl ResponderPolicy for Eclair {
	fn own_filter(&self, now_unix: u32) -> Option<(u32, u32)> {
		self.p.own_filter.resolve(now_unix)
	}

	/// `handleQueryChannelRange`: chunks of `channel-range-chunk-size` SCIDs grouped by block,
	/// timestamps and checksums when asked. Range queries beyond the per-second limit are
	/// dropped (`gossipQueriesRateLimiter`). Assumes uncompressed encoding.
	fn on_query_channel_range(&mut self, ctx: &mut Ctx, q: &QueryChannelRange, out: &mut Out) {
		if self.p.range_queries_per_sec > 0.0 {
			if ctx.now >= self.window.0 + 1_000_000 {
				self.window = (ctx.now, 0.0);
			}
			if self.window.1 >= self.p.range_queries_per_sec {
				return;
			}
			self.window.1 += 1.0;
		}
		let (ts, cs) = wanted_options(q, &self.p);
		let chans = range_chans(ctx.view, q);
		block_aligned_replies(ctx, q, &chans, self.p.range_chunk_scids, ts, cs, Stream::Query, out);
	}

	/// `handleQueryShortChannelIds`: per-scid flags (none means everything), node announcements
	/// interleaved and deduplicated via `nodesSent`.
	fn on_query_short_channel_ids(&mut self, ctx: &mut Ctx, q: &QueryShortChannelIds, out: &mut Out) {
		flagged_scid_response(ctx, q, false, out);
	}

	/// Eclair stores the filter and applies it to future rebroadcasts only: no backlog.
	fn on_gossip_timestamp_filter(&mut self, _ctx: &mut Ctx, _f: &GossipTimestampFilter, _out: &mut Out) {}
}
