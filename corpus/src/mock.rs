//! A small mock graph for tests and smoke runs. Real experiments use a
//! real dump; this only mimics the field shapes and timestamp spread.

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::collections::{BTreeMap, HashSet};

use crate::graph::{Address, Edge, Feature, Graph, Node, Policy};

fn hex(bytes: &[u8]) -> String {
	bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Generate a graph with `n_nodes` nodes and `n_chans` channels. Update timestamps are spread over
/// the 13.5 days before `dump_time`; about 20% of channels have one or no policy; about a third of
/// the nodes have no node announcement.
pub fn graph(seed: u64, n_nodes: usize, n_chans: usize, dump_time: u32, max_height: u32) -> Graph {
	let mut rng = ChaCha8Rng::seed_from_u64(seed);
	let two_weeks_ish = 13 * 24 * 3600 + 12 * 3600;
	let mut pubkeys = Vec::with_capacity(n_nodes);
	let mut nodes = Vec::with_capacity(n_nodes);
	for i in 0..n_nodes {
		let mut k = [0u8; 33];
		rng.fill(&mut k[1..]);
		k[0] = if rng.gen_bool(0.5) { 2 } else { 3 };
		let pk = hex(&k);
		pubkeys.push(pk.clone());
		let announced = rng.gen_bool(0.66);
		let mut features = BTreeMap::new();
		if announced {
			for (bit, name, req) in [
				(0usize, "data-loss-protect", true),
				(5, "upfront-shutdown-script", false),
				(7, "gossip-queries", false),
				(8, "tlv-onion", true),
				(12, "static-remote-key", true),
				(14, "payment-addr", true),
				(17, "multi-path-payments", false),
			] {
				features.insert(
					bit.to_string(),
					Feature { name: name.into(), is_required: req, is_known: true },
				);
			}
		}
		let mut addresses = Vec::new();
		if announced && rng.gen_bool(0.7) {
			addresses.push(Address {
				network: "tcp".into(),
				addr: format!("{}.{}.{}.{}:9735", rng.gen::<u8>(), rng.gen::<u8>(), rng.gen::<u8>(), rng.gen::<u8>()),
			});
		}
		nodes.push(Node {
			last_update: if announced { dump_time - rng.gen_range(0..two_weeks_ish) } else { 0 },
			pub_key: pk,
			alias: if announced { format!("mock-node-{i}") } else { String::new() },
			addresses,
			color: if announced { "#3399ff".into() } else { "#000000".into() },
			features,
		});
	}

	let mut used = HashSet::new();
	let mut edges = Vec::with_capacity(n_chans);
	while edges.len() < n_chans {
		let a = rng.gen_range(0..n_nodes);
		let b = rng.gen_range(0..n_nodes);
		if a == b {
			continue;
		}
		let height = max_height - rng.gen_range(0..150_000u32);
		let scid = ((height as u64) << 40) | ((rng.gen_range(1..2000u64)) << 16) | rng.gen_range(0..2u64);
		if !used.insert(scid) {
			continue;
		}
		let (n1, n2) = if pubkeys[a] < pubkeys[b] { (a, b) } else { (b, a) };
		let capacity = rng.gen_range(20_000..50_000_000u64);
		let policy = |rng: &mut ChaCha8Rng| {
			let fresh = rng.gen_bool(0.7);
			let age = if fresh { rng.gen_range(0..86_400 * 2) } else { rng.gen_range(0..two_weeks_ish) };
			Some(Policy {
				time_lock_delta: [40, 80, 144][rng.gen_range(0..3)],
				min_htlc: 1000,
				fee_base_msat: [0, 1000, 1000, 500][rng.gen_range(0..4)],
				fee_rate_milli_msat: rng.gen_range(0..2000),
				disabled: rng.gen_bool(0.1),
				max_htlc_msat: capacity * 1000 * 99 / 100,
				last_update: dump_time - age,
			})
		};
		let shape = rng.gen_range(0..10);
		let (p1, p2) = match shape {
			0 => (None, None),
			1 => (policy(&mut rng), None),
			2 => (None, policy(&mut rng)),
			_ => (policy(&mut rng), policy(&mut rng)),
		};
		let last_update = p1.as_ref().map_or(0, |p| p.last_update).max(p2.as_ref().map_or(0, |p| p.last_update));
		edges.push(Edge {
			channel_id: scid,
			chan_point: format!("{}:{}", hex(&rng.gen::<[u8; 32]>()), scid & 0xffff),
			last_update,
			node1_pub: pubkeys[n1].clone(),
			node2_pub: pubkeys[n2].clone(),
			capacity,
			node1_policy: p1,
			node2_policy: p2,
		});
	}
	Graph { nodes, edges }
}

pub fn json(seed: u64, n_nodes: usize, n_chans: usize, dump_time: u32, max_height: u32) -> String {
	serde_json::to_string_pretty(&graph(seed, n_nodes, n_chans, dump_time, max_height)).unwrap()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn json_round_trips_through_parser() {
		let j = json(1, 10, 20, 1_700_000_000, 800_000);
		let g: Graph = serde_json::from_str(&j).unwrap();
		assert_eq!(g.nodes.len(), 10);
		assert_eq!(g.edges.len(), 20);
	}
}
