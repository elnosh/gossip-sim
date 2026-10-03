//! LND (discovery/syncer.go, discovery/chan_series.go, graph/db).

use corpus::msg_type;
use lightning::ln::msgs::{GossipTimestampFilter, QueryChannelRange, QueryShortChannelIds};
use lightning::routing::gossip::NodeId;
use rand::seq::SliceRandom;
use std::collections::BTreeSet;

use super::*;

pub struct Lnd {
	p: Profile,
	backlog_sent: bool,
}

impl Lnd {
	pub fn new(p: Profile) -> Self {
		Lnd { p, backlog_sent: false }
	}
}

impl ResponderPolicy for Lnd {
	fn own_filter(&self, now_unix: u32) -> Option<(u32, u32)> {
		self.p.own_filter.resolve(now_unix)
	}

	/// `replyChanRangeQuery`: blocks are kept whole; a block with more channels than the chunk
	/// size is shuffled and truncated (the excess is silently dropped). An empty first reply
	/// covers the gap before an overflowing first block. Checksums are not implemented.
	fn on_query_channel_range(&mut self, ctx: &mut Ctx, q: &QueryChannelRange, out: &mut Out) {
		let (ts, _) = wanted_options(q, &self.p);
		let chunk = if ts { self.p.range_chunk_scids / 2 } else { self.p.range_chunk_scids };
		let chans = range_chans(ctx.view, q);
		let last_block = query_end(q).saturating_sub(1);
		let mut first = q.first_blocknum;
		let mut cur: Vec<(u64, &ViewChan)> = Vec::new();
		let mut i = 0;
		while i < chans.len() {
			let h = height(chans[i].0);
			let mut j = i;
			while j < chans.len() && height(chans[j].0) == h {
				j += 1;
			}
			let mut block: Vec<_> = chans[i..j].to_vec();
			i = j;
			if cur.len() + block.len() <= chunk {
				cur.extend(block);
				continue;
			}
			out.push(
				Stream::Query,
				msg_type::REPLY_CHANNEL_RANGE,
				reply_range(ctx, first, h - first, false, &cur, ts, false),
			);
			first = h;
			if block.len() > chunk {
				block.shuffle(ctx.rng);
				block.truncate(chunk);
				block.sort_by_key(|(s, _)| *s);
			}
			cur = block;
		}
		out.push(
			Stream::Query,
			msg_type::REPLY_CHANNEL_RANGE,
			reply_range(ctx, first, last_block - first + 1, true, &cur, ts, false),
		);
	}

	/// `replyShortChanIDs` + `FetchChanAnns`: announcement, then each existing update followed by
	/// the *opposite* node's announcement, deduplicated within the query. Query flags are ignored.
	/// An empty query gets no reply at all.
	fn on_query_short_channel_ids(&mut self, ctx: &mut Ctx, q: &QueryShortChannelIds, out: &mut Out) {
		if q.short_channel_ids.is_empty() {
			return;
		}
		let mut nodes_sent: BTreeSet<NodeId> = BTreeSet::new();
		for scid in &q.short_channel_ids {
			let Some(c) = ctx.view.chans.get(scid) else { continue };
			out.push(Stream::Query, msg_type::CHANNEL_ANNOUNCEMENT, c.entry.ann.clone());
			for (d, other) in [(0, c.entry.node2), (1, c.entry.node1)] {
				if let Some(u) = c.upd(d) {
					out.push(Stream::Query, msg_type::CHANNEL_UPDATE, u.bytes.clone());
					if nodes_sent.insert(other) {
						if let Some(n) = ctx.view.nodes.get(&other) {
							out.push(Stream::Query, msg_type::NODE_ANNOUNCEMENT, n.bytes.clone());
						}
					}
				}
			}
		}
		out.push(Stream::Query, msg_type::REPLY_SHORT_CHANNEL_IDS_END, reply_end(ctx, true));
	}

	/// `ApplyGossipFilter` + `UpdatesInHorizon`: the full backlog of the window
	/// [first, first + range), uncapped. Channels with any update in the window are sent with
	/// their announcement and both latest updates, in update-index (timestamp) order, then node
	/// announcements in the window. Only one backlog per peer.
	fn on_gossip_timestamp_filter(&mut self, ctx: &mut Ctx, f: &GossipTimestampFilter, out: &mut Out) {
		if self.backlog_sent {
			return;
		}
		self.backlog_sent = true;
		let start = f.first_timestamp as u64;
		let end = start + f.timestamp_range as u64;
		let in_win = |ts: u32| (ts as u64) >= start && (ts as u64) < end;
		let mut chans: Vec<(u32, u64, &ViewChan)> = ctx
			.view
			.chans
			.iter()
			.filter_map(|(scid, c)| {
				(0..2).filter_map(|d| c.upd(d).map(|u| u.timestamp)).filter(|t| in_win(*t)).min().map(|t| (t, *scid, c))
			})
			.collect();
		chans.sort_by_key(|(t, s, _)| (*t, *s));
		for (_, _, c) in chans {
			out.push(Stream::Backlog, msg_type::CHANNEL_ANNOUNCEMENT, c.entry.ann.clone());
			for d in 0..2 {
				if let Some(u) = c.upd(d) {
					out.push(Stream::Backlog, msg_type::CHANNEL_UPDATE, u.bytes.clone());
				}
			}
		}
		let mut nodes: Vec<_> = ctx.view.nodes.values().filter(|n| in_win(n.timestamp)).collect();
		nodes.sort_by_key(|n| (n.timestamp, n.node_id));
		for n in nodes {
			out.push(Stream::Backlog, msg_type::NODE_ANNOUNCEMENT, n.bytes.clone());
		}
	}
}
