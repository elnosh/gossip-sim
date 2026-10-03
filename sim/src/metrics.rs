//! Completeness against ground truth, byte accounting, and the per-run output rows.

use corpus::GroundTruth;
use serde::Serialize;
use std::collections::BTreeMap;

use crate::ldk::Graph;
use crate::peer::TypeCounters;
use crate::wire;

#[derive(Debug, Clone, Default, Serialize)]
pub struct Completeness {
	/// Ground-truth channels the node has.
	pub chans_have: u32,
	pub chans_gt: u32,
	/// Channels the node has that no peer has (e.g. closed or pruned elsewhere).
	pub chans_extra: u32,
	/// Ground-truth channels with at least one update, held by the node with at least one update.
	pub routable_have: u32,
	pub routable_gt: u32,
	/// Directions where the node's update is at least as new as the freshest one any peer has.
	pub upds_fresh: u32,
	/// Directions where the node has an older update than some peer.
	pub upds_stale: u32,
	pub upds_gt: u32,
	pub nodes_fresh: u32,
	pub nodes_gt: u32,
}

impl Completeness {
	pub fn measure(graph: &Graph, gt: &GroundTruth) -> Completeness {
		let ro = graph.read_only();
		let mut c = Completeness {
			chans_gt: gt.chans.len() as u32,
			upds_gt: gt.update_count() as u32,
			nodes_gt: gt.nodes.len() as u32,
			..Default::default()
		};
		for (scid, g) in &gt.chans {
			let routable = g.iter().any(|u| u.is_some());
			if routable {
				c.routable_gt += 1;
			}
			let Some(info) = ro.channel(*scid) else { continue };
			c.chans_have += 1;
			let ours = [info.one_to_two.as_ref().map(|u| u.last_update), info.two_to_one.as_ref().map(|u| u.last_update)];
			if routable && ours.iter().any(|u| u.is_some()) {
				c.routable_have += 1;
			}
			for d in 0..2 {
				if let (Some(gt_ts), Some(ours)) = (g[d], ours[d]) {
					if ours >= gt_ts {
						c.upds_fresh += 1;
					} else {
						c.upds_stale += 1;
					}
				}
			}
		}
		for (scid, _) in ro.channels().unordered_iter() {
			if !gt.chans.contains_key(scid) {
				c.chans_extra += 1;
			}
		}
		for (id, ts) in &gt.nodes {
			if let Some(n) = ro.node(id) {
				if n.announcement_info.as_ref().map_or(false, |a| a.last_update() >= *ts) {
					c.nodes_fresh += 1;
				}
			}
		}
		c
	}

	fn frac(a: u32, b: u32) -> f64 {
		if b == 0 {
			1.0
		} else {
			a as f64 / b as f64
		}
	}
	pub fn frac_chans(&self) -> f64 {
		Self::frac(self.chans_have, self.chans_gt)
	}
	pub fn frac_routable(&self) -> f64 {
		Self::frac(self.routable_have, self.routable_gt)
	}
	pub fn frac_upds(&self) -> f64 {
		Self::frac(self.upds_fresh, self.upds_gt)
	}
	pub fn frac_nodes(&self) -> f64 {
		Self::frac(self.nodes_fresh, self.nodes_gt)
	}
}

/// Byte totals by message family.
pub fn by_family(c: &TypeCounters) -> BTreeMap<&'static str, u64> {
	let mut m = BTreeMap::new();
	for (ty, (_, bytes)) in c {
		*m.entry(wire::family(*ty)).or_insert(0) += bytes;
	}
	m
}

pub fn total(c: &TypeCounters) -> u64 {
	c.values().map(|(_, b)| b).sum()
}

pub fn merge(into: &mut TypeCounters, from: &TypeCounters) {
	for (ty, (m, b)) in from {
		let e = into.entry(*ty).or_insert((0, 0));
		e.0 += m;
		e.1 += b;
	}
}

#[derive(Debug, Clone, Serialize)]
pub struct PeerSample {
	pub label: String,
	/// Wire bytes the node received from this peer.
	pub rx: u64,
	/// Wire bytes the node sent to this peer.
	pub tx: u64,
	pub queued_msgs: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct Sample {
	pub t_s: f64,
	#[serde(flatten)]
	pub c: Completeness,
	pub applied: u64,
	pub rejected: u64,
	pub rx: BTreeMap<&'static str, u64>,
	pub tx: BTreeMap<&'static str, u64>,
	pub peers: Vec<PeerSample>,
}

/// One CSV row: ordered (column, value) pairs.
#[derive(Debug, Clone, Default)]
pub struct Row(pub Vec<(String, String)>);

impl Row {
	pub fn put(&mut self, k: &str, v: impl ToString) {
		self.0.push((k.to_string(), v.to_string()));
	}
	pub fn get(&self, k: &str) -> Option<&str> {
		self.0.iter().find(|(c, _)| c == k).map(|(_, v)| v.as_str())
	}
}

fn csv_escape(s: &str) -> String {
	if s.contains([',', '"', '\n']) {
		format!("\"{}\"", s.replace('"', "\"\""))
	} else {
		s.to_string()
	}
}

/// Writes rows with the union of their columns, in first-seen order.
pub fn write_csv(path: &std::path::Path, rows: &[Row]) -> std::io::Result<()> {
	let mut cols: Vec<String> = Vec::new();
	for r in rows {
		for (k, _) in &r.0 {
			if !cols.contains(k) {
				cols.push(k.clone());
			}
		}
	}
	let mut s = cols.join(",");
	s.push('\n');
	for r in rows {
		let line: Vec<String> = cols.iter().map(|c| csv_escape(r.get(c).unwrap_or(""))).collect();
		s.push_str(&line.join(","));
		s.push('\n');
	}
	std::fs::write(path, s)
}
