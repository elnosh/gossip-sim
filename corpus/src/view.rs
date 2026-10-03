//! Per-peer views of the corpus at a point in time, and the ground truth (their union).

use lightning::routing::gossip::NodeId;
use rand::Rng;
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;

use crate::corpus::{ChanEntry, Corpus, NodeEntry, UpdEntry};

/// How a peer prunes channels whose updates are older than `stale_secs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StaleRule {
	/// Never prune (the simulated LDK node itself prunes through the real `NetworkGraph`).
	Never,
	/// Prune when both directions are stale (LND default).
	BothSides,
	/// Prune when either direction is stale (CLN, Eclair, LND strict).
	EitherSide,
}

/// Structured ways a peer's view can differ from the full corpus, standing in for the
/// propagation gaps between peer clusters until the recorder measures them.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StructuredDrop {
	/// Drop a fraction of channels announced (by block time) within `newer_than_secs` of the view time.
	RecentAnnouncements { newer_than_secs: u32, fraction: f64 },
	/// Drop a fraction of channels whose freshest update is within `within_secs` of going stale.
	NearStaleEdge { within_secs: u32, fraction: f64 },
	/// Drop a uniformly random fraction of channels.
	RandomFraction { fraction: f64 },
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default)]
pub struct ViewFilter {
	/// Whether channels with no (known) channel_update are part of the view (LND yes, CLN no).
	pub include_no_update_chans: bool,
	pub stale_rule: StaleRule,
	/// Age after which a direction counts as stale.
	pub stale_secs: u32,
	/// Whether a missing direction counts as stale for `stale_rule` (LND and Eclair yes, CLN no).
	pub missing_is_stale: bool,
	/// Whether channels with no update at all can be pruned (LND never finds them in its update
	/// index, so it keeps them; Eclair and LDK prune them).
	pub prune_no_update_chans: bool,
	/// Channels younger than this (by block time) are never pruned (Eclair: 2016 blocks).
	pub prune_min_age_secs: u32,
	pub drops: Vec<StructuredDrop>,
}

impl Default for ViewFilter {
	fn default() -> Self {
		ViewFilter {
			include_no_update_chans: true,
			stale_rule: StaleRule::Never,
			stale_secs: 14 * 24 * 3600,
			missing_is_stale: true,
			prune_no_update_chans: true,
			prune_min_age_secs: 0,
			drops: Vec::new(),
		}
	}
}

/// A channel as one peer sees it: the shared corpus entry plus which directions are known.
#[derive(Debug, Clone)]
pub struct ViewChan {
	pub entry: Arc<ChanEntry>,
	pub has_upd: [bool; 2],
}

impl ViewChan {
	pub fn upd(&self, dir: usize) -> Option<&UpdEntry> {
		if self.has_upd[dir] {
			self.entry.upd[dir].as_ref()
		} else {
			None
		}
	}
	/// Update timestamp for a direction, 0 if unknown (what peers put in `timestamps` TLVs).
	pub fn timestamp(&self, dir: usize) -> u32 {
		self.upd(dir).map(|u| u.timestamp).unwrap_or(0)
	}
	/// Update checksum for a direction, 0 if unknown.
	pub fn checksum(&self, dir: usize) -> u32 {
		self.upd(dir).map(|u| u.checksum).unwrap_or(0)
	}
	pub fn has_any_update(&self) -> bool {
		self.has_upd[0] || self.has_upd[1]
	}
	pub fn newest_update(&self) -> Option<u32> {
		(0..2).filter_map(|d| self.upd(d).map(|u| u.timestamp)).max()
	}
}

#[derive(Debug, Clone, Default)]
pub struct PeerView {
	/// Unix time the view represents.
	pub at: u32,
	pub chans: BTreeMap<u64, ViewChan>,
	pub nodes: BTreeMap<NodeId, Arc<NodeEntry>>,
}

impl PeerView {
	/// The corpus as a peer with policy `filter` would hold it at unix time `t`.
	pub fn derive(corpus: &Corpus, t: u32, filter: &ViewFilter, rng: &mut impl Rng) -> PeerView {
		let max_height = corpus.height_at(t);
		let mut chans = BTreeMap::new();
		for (scid, entry) in &corpus.chans {
			if entry.block_height() > max_height {
				continue;
			}
			let mut has_upd = [false; 2];
			let mut stale = [false; 2];
			for d in 0..2 {
				match &entry.upd[d] {
					Some(u) if u.timestamp <= t => {
						has_upd[d] = true;
						stale[d] = t - u.timestamp > filter.stale_secs;
					},
					_ => stale[d] = filter.missing_is_stale,
				}
			}
			let age = t.saturating_sub(corpus.time_of_height(entry.block_height()));
			let prunable = age > filter.prune_min_age_secs
				&& (filter.prune_no_update_chans || has_upd[0] || has_upd[1]);
			let pruned = prunable
				&& match filter.stale_rule {
					StaleRule::Never => false,
					StaleRule::BothSides => stale[0] && stale[1],
					StaleRule::EitherSide => stale[0] || stale[1],
				};
			if pruned {
				continue;
			}
			let vc = ViewChan { entry: entry.clone(), has_upd };
			if !filter.include_no_update_chans && !vc.has_any_update() {
				continue;
			}
			if filter.drops.iter().any(|d| drop_applies(corpus, t, filter, &vc, d, rng)) {
				continue;
			}
			chans.insert(*scid, vc);
		}
		let mut with_chans: HashSet<NodeId> = HashSet::new();
		for c in chans.values() {
			with_chans.insert(c.entry.node1);
			with_chans.insert(c.entry.node2);
		}
		// Announcements dated after the dump itself are bogus future timestamps that every peer
		// nonetheless holds, so they are present in every view.
		let future = corpus.meta.dump_time.saturating_add(86_400);
		let nodes = corpus
			.nodes
			.iter()
			.filter(|(id, n)| (n.timestamp <= t || n.timestamp > future) && with_chans.contains(*id))
			.map(|(id, n)| (*id, n.clone()))
			.collect();
		PeerView { at: t, chans, nodes }
	}

	/// Channels whose SCID block height is in `[first, first + n)`, in SCID order.
	pub fn chans_in_blocks(&self, first: u32, n: u32) -> impl Iterator<Item = (&u64, &ViewChan)> {
		let lo = Corpus::scid_from_height(first);
		let end_block = first.saturating_add(n);
		let hi = if end_block == u32::MAX {
			Bound::Unbounded
		} else {
			Bound::Excluded(Corpus::scid_from_height(end_block))
		};
		self.chans.range((Bound::Included(lo), hi))
	}

	pub fn update_count(&self) -> usize {
		self.chans.values().map(|c| c.has_upd.iter().filter(|b| **b).count()).sum()
	}
}

fn drop_applies(
	corpus: &Corpus, t: u32, filter: &ViewFilter, vc: &ViewChan, d: &StructuredDrop,
	rng: &mut impl Rng,
) -> bool {
	match d {
		StructuredDrop::RecentAnnouncements { newer_than_secs, fraction } => {
			let announced_at = corpus.time_of_height(vc.entry.block_height());
			announced_at.saturating_add(*newer_than_secs) > t && rng.gen::<f64>() < *fraction
		},
		StructuredDrop::NearStaleEdge { within_secs, fraction } => match vc.newest_update() {
			Some(ts) => {
				let age = t.saturating_sub(ts);
				age.saturating_add(*within_secs) > filter.stale_secs && rng.gen::<f64>() < *fraction
			},
			None => false,
		},
		StructuredDrop::RandomFraction { fraction } => rng.gen::<f64>() < *fraction,
	}
}

/// What a fully synced node could know at a point in time: the union of the peers' views,
/// counting only updates young enough to be accepted (LDK rejects updates older than 14 days).
#[derive(Debug, Clone, Default)]
pub struct GroundTruth {
	/// scid -> freshest known update timestamp per direction (None if no peer knows one).
	pub chans: BTreeMap<u64, [Option<u32>; 2]>,
	/// node -> freshest node_announcement timestamp.
	pub nodes: BTreeMap<NodeId, u32>,
}

impl GroundTruth {
	/// Updates older than this at the view time are not counted.
	pub const MAX_UPDATE_AGE: u32 = 14 * 24 * 3600;

	pub fn union<'a>(views: impl IntoIterator<Item = &'a PeerView>) -> GroundTruth {
		let mut gt = GroundTruth::default();
		for v in views {
			for (scid, c) in &v.chans {
				let e = gt.chans.entry(*scid).or_insert([None, None]);
				for d in 0..2 {
					if let Some(u) = c.upd(d).filter(|u| v.at.saturating_sub(u.timestamp) <= Self::MAX_UPDATE_AGE) {
						e[d] = Some(e[d].map_or(u.timestamp, |x| x.max(u.timestamp)));
					}
				}
			}
			for (id, n) in &v.nodes {
				let e = gt.nodes.entry(*id).or_insert(0);
				*e = (*e).max(n.timestamp);
			}
		}
		gt
	}

	pub fn update_count(&self) -> usize {
		self.chans.values().map(|u| u.iter().flatten().count()).sum()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::{import, mock};
	use rand::SeedableRng;

	fn corpus() -> Corpus {
		let g = mock::graph(11, 80, 400, 1_787_747_191, 964_125);
		import::build(&g).unwrap().0
	}

	#[test]
	fn full_view_at_dump_time_matches_corpus() {
		let c = corpus();
		let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(1);
		let v = PeerView::derive(&c, c.meta.dump_time, &ViewFilter::default(), &mut rng);
		assert_eq!(v.chans.len(), c.chans.len());
		assert_eq!(v.update_count(), c.update_count());
		assert_eq!(v.nodes.len(), c.nodes.len());
	}

	#[test]
	fn older_view_hides_newer_channels_and_updates() {
		let c = corpus();
		let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(1);
		let t = c.meta.dump_time - 3 * 24 * 3600;
		let v = PeerView::derive(&c, t, &ViewFilter::default(), &mut rng);
		assert!(v.chans.len() < c.chans.len());
		assert!(v.chans.values().all(|vc| vc.entry.block_height() <= c.height_at(t)));
		for vc in v.chans.values() {
			for d in 0..2 {
				if let Some(u) = vc.upd(d) {
					assert!(u.timestamp <= t);
				}
			}
		}
		assert!(v.nodes.values().all(|n| n.timestamp <= t));
	}

	#[test]
	fn cln_like_filter_excludes_no_update_channels() {
		let c = corpus();
		let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(1);
		let f = ViewFilter {
			include_no_update_chans: false,
			stale_rule: StaleRule::EitherSide,
			missing_is_stale: false,
			..Default::default()
		};
		let v = PeerView::derive(&c, c.meta.dump_time, &f, &mut rng);
		assert!(v.chans.values().all(|vc| vc.has_any_update()));
		assert!(v.chans.len() < c.chans.len());
	}

	#[test]
	fn drops_are_seeded_and_bounded() {
		let c = corpus();
		let f = ViewFilter {
			drops: vec![StructuredDrop::RandomFraction { fraction: 0.25 }],
			..Default::default()
		};
		let mut r1 = rand_chacha::ChaCha8Rng::seed_from_u64(5);
		let mut r2 = rand_chacha::ChaCha8Rng::seed_from_u64(5);
		let a = PeerView::derive(&c, c.meta.dump_time, &f, &mut r1);
		let b = PeerView::derive(&c, c.meta.dump_time, &f, &mut r2);
		assert_eq!(a.chans.keys().collect::<Vec<_>>(), b.chans.keys().collect::<Vec<_>>());
		let frac = a.chans.len() as f64 / c.chans.len() as f64;
		assert!(frac > 0.6 && frac < 0.9, "kept {frac}");
	}

	#[test]
	fn range_query_and_ground_truth() {
		let c = corpus();
		let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(1);
		let v = PeerView::derive(&c, c.meta.dump_time, &ViewFilter::default(), &mut rng);
		let all: Vec<_> = v.chans_in_blocks(0, u32::MAX).map(|(s, _)| *s).collect();
		assert_eq!(all.len(), v.chans.len());
		let h = c.meta.max_height - 50_000;
		let part: Vec<_> = v.chans_in_blocks(h, 10_000).map(|(s, _)| *s).collect();
		assert!(part.iter().all(|s| { let bh = (*s >> 40) as u32; bh >= h && bh < h + 10_000 }));
		let f = ViewFilter { drops: vec![StructuredDrop::RandomFraction { fraction: 0.5 }], ..Default::default() };
		let v2 = PeerView::derive(&c, c.meta.dump_time, &f, &mut rng);
		let gt = GroundTruth::union([&v, &v2]);
		assert_eq!(gt.chans.len(), v.chans.len());
		assert_eq!(gt.update_count(), v.update_count());
	}
}
