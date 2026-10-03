//! Current LDK behaviour (`P2PGossipSync::peer_connected`): a `gossip_timestamp_filter` with
//! first_timestamp two weeks ago for the first 5 gossip_queries peers (never reset), one hour ago
//! for later ones. No queries.

use super::*;

pub struct Baseline {
	full_peers: usize,
	full_syncs_requested: usize,
}

impl Baseline {
	pub fn new(full_peers: usize) -> Self {
		Baseline { full_peers, full_syncs_requested: 0 }
	}
}

impl Strategy for Baseline {
	fn on_peer_connected(&mut self, peer: PublicKey, init: &Init, graph: &Graph, now: u64, out: &mut Vec<MessageSendEvent>) {
		if !init.features.supports_gossip_queries() {
			return;
		}
		let full = self.full_syncs_requested < self.full_peers;
		if full {
			self.full_syncs_requested += 1;
		}
		let first = if full { now - 60 * 60 * 24 * 7 * 2 } else { now - 60 * 60 };
		out.push(filter_event(peer, graph, first as u32));
	}
}
