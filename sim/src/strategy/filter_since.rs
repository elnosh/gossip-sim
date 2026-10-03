//! Like the baseline, but the wide filter starts at the newest update already in the graph
//! (minus a margin) instead of two weeks ago. On an empty graph it is the baseline.

use super::*;

pub struct FilterSinceLastSeen {
	full_peers: usize,
	margin_secs: u32,
	full_syncs_requested: usize,
	/// Fixed at the first connection so later peers do not move the start forward.
	start: Option<u32>,
}

impl FilterSinceLastSeen {
	pub fn new(full_peers: usize, margin_secs: u32) -> Self {
		FilterSinceLastSeen { full_peers, margin_secs, full_syncs_requested: 0, start: None }
	}
}

impl Strategy for FilterSinceLastSeen {
	fn on_peer_connected(&mut self, peer: PublicKey, init: &Init, graph: &Graph, now: u64, out: &mut Vec<MessageSendEvent>) {
		if !init.features.supports_gossip_queries() {
			return;
		}
		let two_weeks_ago = (now - 14 * 24 * 3600) as u32;
		let start = *self.start.get_or_insert_with(|| match newest_update(graph) {
			Some(t) => t.saturating_sub(self.margin_secs).max(two_weeks_ago),
			None => two_weeks_ago,
		});
		let full = self.full_syncs_requested < self.full_peers;
		if full {
			self.full_syncs_requested += 1;
		}
		let first = if full { start } else { (now - 3600) as u32 };
		out.push(filter_event(peer, graph, first));
	}
}
