//! Starting state of the node under test.

use bitcoin::Network;
use corpus::{Corpus, PeerView, ViewFilter};
use lightning::ln::msgs::{ChannelAnnouncement, ChannelUpdate, NodeAnnouncement};
use lightning::util::ser::{ReadableArgs, Writeable};
use lightning::util::sim_clock::set_unix_now;
use rand::SeedableRng;
use std::sync::Arc;

use crate::ldk::{Graph, SimLogger};
use crate::wire::decode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scenario {
	/// Empty graph.
	Bootstrap,
	/// The node synced the graph up to `offline_secs` before the dump, persisted it, and restarts
	/// at dump time.
	Restart { offline_secs: u32 },
}

impl Scenario {
	pub fn label(&self) -> String {
		match self {
			Scenario::Bootstrap => "bootstrap".into(),
			Scenario::Restart { offline_secs } => format!("restart-{}", fmt_duration(*offline_secs)),
		}
	}
	pub fn offline_secs(&self) -> u32 {
		match self {
			Scenario::Bootstrap => 0,
			Scenario::Restart { offline_secs } => *offline_secs,
		}
	}
}

pub fn fmt_duration(s: u32) -> String {
	if s % 86_400 == 0 {
		format!("{}d", s / 86_400)
	} else if s % 3600 == 0 {
		format!("{}h", s / 3600)
	} else if s % 60 == 0 {
		format!("{}m", s / 60)
	} else {
		format!("{s}s")
	}
}

pub fn parse_duration(s: &str) -> crate::Result<u32> {
	let s = s.trim();
	let (num, mult) = match s.chars().last() {
		Some('d') => (&s[..s.len() - 1], 86_400),
		Some('h') => (&s[..s.len() - 1], 3600),
		Some('m') => (&s[..s.len() - 1], 60),
		Some('s') => (&s[..s.len() - 1], 1),
		_ => (s, 1),
	};
	Ok(num.parse::<u32>().map_err(|e| format!("bad duration `{s}`: {e}"))? * mult)
}

/// The node's graph when the run starts. Leaves the sim clock at `corpus.meta.dump_time`.
pub fn initial_graph(corpus: &Corpus, scenario: Scenario, logger: Arc<SimLogger>) -> Graph {
	let now = corpus.meta.dump_time;
	let g = match scenario {
		Scenario::Bootstrap => Graph::new(Network::Bitcoin, logger),
		Scenario::Restart { offline_secs } => persisted_graph(corpus, now - offline_secs, logger),
	};
	set_unix_now(now as u64);
	g
}

/// What a node that tracked the full corpus would have persisted at `t_shutdown`.
///
/// Single-dump approximation: channels whose block is at most the height at `t_shutdown`, and only
/// updates and node announcements timestamped at or before `t_shutdown`. Each channel is inserted
/// with the clock at its approximate announcement time (so `announcement_received_time` is
/// realistic), updates with the clock at `t_shutdown`, and the graph is pruned at `t_shutdown`
/// as a live node's hourly prune would have done. The result goes through a write/read round trip.
pub fn persisted_graph(corpus: &Corpus, t_shutdown: u32, logger: Arc<SimLogger>) -> Graph {
	let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(0);
	let view = PeerView::derive(corpus, t_shutdown, &ViewFilter::default(), &mut rng);
	let g = Graph::new(Network::Bitcoin, logger.clone());
	for c in view.chans.values() {
		let announced = corpus.time_of_height(c.entry.block_height()).saturating_add(3600).min(t_shutdown);
		set_unix_now(announced as u64);
		let ann: ChannelAnnouncement = decode(&c.entry.ann).expect("corpus announcement");
		if g.update_channel_from_announcement_no_lookup(&ann).is_err() {
			continue;
		}
		set_unix_now(t_shutdown as u64);
		for d in 0..2 {
			if let Some(u) = c.upd(d) {
				let upd: ChannelUpdate = decode(&u.bytes).expect("corpus update");
				let _ = g.update_channel(&upd);
			}
		}
	}
	set_unix_now(t_shutdown as u64);
	for n in view.nodes.values() {
		let na: NodeAnnouncement = decode(&n.bytes).expect("corpus node announcement");
		let _ = g.update_node_from_announcement(&na);
	}
	g.remove_stale_channels_and_tracking_with_time(t_shutdown as u64);
	let bytes = g.encode();
	Graph::read(&mut &bytes[..], logger).expect("network graph round trip")
}

#[cfg(test)]
mod tests {
	use super::*;
	use corpus::{import, mock};

	#[test]
	fn durations() {
		assert_eq!(parse_duration("3d").unwrap(), 3 * 86_400);
		assert_eq!(parse_duration("90m").unwrap(), 5400);
		assert_eq!(parse_duration("15").unwrap(), 15);
		assert_eq!(fmt_duration(86_400 * 7), "7d");
		assert_eq!(fmt_duration(3600 * 6), "6h");
	}

	#[test]
	fn persisted_graph_shrinks_with_offline_time() {
		lightning::util::sim_clock::set_skip_sig_verify(true);
		let corpus = import::build(&mock::graph(5, 60, 300, 1_787_747_191, 964_125)).unwrap().0;
		let logger = Arc::new(SimLogger::from_env("test"));
		let n = |off: u32| {
			let g = initial_graph(&corpus, Scenario::Restart { offline_secs: off }, logger.clone());
			let n = g.read_only().channels().len();
			n
		};
		let (a, b, c) = (n(3600), n(3 * 86_400), n(20 * 86_400));
		assert!(a >= b && b >= c, "{a} {b} {c}");
		assert!(a > 0);
		assert_eq!(lightning::util::sim_clock::unix_now(), corpus.meta.dump_time as u64);
	}
}
