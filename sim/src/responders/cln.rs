//! Core Lightning (connectd/queries.c, connectd/multiplex.c, common/gossmap.c).

use corpus::msg_type;
use lightning::ln::msgs::{GossipTimestampFilter, QueryChannelRange, QueryShortChannelIds};

use super::*;

pub struct Cln {
	p: Profile,
	scid_query_busy: bool,
}

impl Cln {
	pub fn new(p: Profile) -> Self {
		Cln { p, scid_query_busy: false }
	}

	/// `max_entries`: how many SCIDs fit in one reply given the requested TLVs.
	fn max_entries(&self, ts: bool, cs: bool) -> usize {
		let per = 8 + if ts { 8 } else { 0 } + if cs { 8 } else { 0 };
		let budget = self.p.range_max_bytes;
		let rough = budget / per;
		let bigsize_len = |v: usize| if v < 0xfd { 1 } else if v <= 0xffff { 3 } else { 5 };
		let mut overhead = 0;
		if ts {
			overhead += 1 + bigsize_len(rough * 8) + 1;
		}
		if cs {
			overhead += 1 + bigsize_len(rough * 8);
		}
		((budget - overhead) / per).max(1)
	}
}

/// A gossip_store record approximated from the view: store order is append order, which for a
/// snapshot is approximated by timestamp; announcements carry the timestamp of their first update.
struct Rec<'a> {
	ts: u32,
	order: (u32, u8, u64),
	ty: u16,
	bytes: &'a Bytes,
}

impl ResponderPolicy for Cln {
	fn own_filter(&self, now_unix: u32) -> Option<(u32, u32)> {
		self.p.own_filter.resolve(now_unix)
	}

	/// `queue_channel_ranges`: byte-capped replies cut at block boundaries, pushed unpaced.
	/// Channels without any update are excluded by the view filter (`gather_range`).
	fn on_query_channel_range(&mut self, ctx: &mut Ctx, q: &QueryChannelRange, out: &mut Out) {
		let (ts, cs) = wanted_options(q, &self.p);
		let chans: Vec<_> = range_chans(ctx.view, q).into_iter().filter(|(_, c)| c.has_any_update()).collect();
		let limit = self.max_entries(ts, cs);
		block_aligned_replies(ctx, q, &chans, limit, ts, cs, Stream::Unpaced, out);
	}

	/// `handle_query_short_channel_ids`: one query at a time (a concurrent one gets a warning and
	/// is dropped), per-scid flags honoured, node announcements sorted and deduplicated at the end.
	fn on_query_short_channel_ids(&mut self, ctx: &mut Ctx, q: &QueryShortChannelIds, out: &mut Out) {
		if self.scid_query_busy {
			out.push(Stream::Query, msg_type::WARNING, warning("Bad concurrent query_short_channel_ids"));
			return;
		}
		if let Some(f) = &q.query_flags {
			if f.len() != q.short_channel_ids.len() {
				out.push(Stream::Query, msg_type::WARNING, warning("Bad query_short_channel_ids query_flags"));
				return;
			}
		}
		self.scid_query_busy = true;
		flagged_scid_response(ctx, q, true, out);
	}

	/// `handle_gossip_timestamp_filter_in`: 0 replays the whole store, 0xFFFFFFFF nothing, and any
	/// other value starts from the "recent" iterator (~now - 2h) regardless of how old the filter
	/// is. Records are then filtered by the window; announcements with no update (store ts 0)
	/// always pass.
	fn on_gossip_timestamp_filter(&mut self, ctx: &mut Ctx, f: &GossipTimestampFilter, out: &mut Out) {
		if f.first_timestamp == u32::MAX {
			return;
		}
		let start_ts = if f.first_timestamp == 0 { 0 } else { ctx.now_unix.saturating_sub(self.p.recent_window_secs) };
		let (min, max) = filter_window_inclusive(f);
		let mut recs: Vec<Rec> = Vec::new();
		for (scid, c) in &ctx.view.chans {
			let ann_ts = (0..2).filter_map(|d| c.upd(d).map(|u| u.timestamp)).min().unwrap_or(0);
			recs.push(Rec { ts: ann_ts, order: (ann_ts, 0, *scid), ty: msg_type::CHANNEL_ANNOUNCEMENT, bytes: &c.entry.ann });
			for d in 0..2 {
				if let Some(u) = c.upd(d) {
					recs.push(Rec { ts: u.timestamp, order: (u.timestamp, 1, *scid), ty: msg_type::CHANNEL_UPDATE, bytes: &u.bytes });
				}
			}
		}
		for (i, n) in ctx.view.nodes.values().enumerate() {
			recs.push(Rec { ts: n.timestamp, order: (n.timestamp, 2, i as u64), ty: msg_type::NODE_ANNOUNCEMENT, bytes: &n.bytes });
		}
		recs.sort_by_key(|r| r.order);
		for r in recs {
			if r.order.0 < start_ts {
				continue;
			}
			if r.ts != 0 && (r.ts < min || r.ts > max) {
				continue;
			}
			out.push(Stream::Backlog, r.ty, r.bytes.clone());
		}
	}

	fn on_sent(&mut self, ty: u16) {
		if ty == msg_type::REPLY_SHORT_CHANNEL_IDS_END {
			self.scid_query_busy = false;
		}
	}
}
