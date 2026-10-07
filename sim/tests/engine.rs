//! End-to-end runs on a small mock graph.

use corpus::{import, mock, msg_type, Corpus};
use sim::engine::{self, LinkCfg, RunCfg, RunSpec};
use sim::profile::{Kind, Profile};
use sim::scenario::Scenario;
use sim::strategy::{StrategyParams, StrategySpec};
use std::path::PathBuf;
use std::sync::OnceLock;

const DUMP: u32 = 1_787_747_191;

fn corpus() -> &'static Corpus {
	static C: OnceLock<Corpus> = OnceLock::new();
	C.get_or_init(|| import::build(&mock::graph(9, 300, 1200, DUMP, 964_125)).unwrap().0)
}

fn spec(id: &str, strategy: StrategySpec, peers: &[(Kind, usize)], scenario: Scenario) -> RunSpec {
	let mut list = Vec::new();
	for (kind, n) in peers {
		for i in 0..*n {
			let p = Profile::builtin(*kind);
			list.push((format!("{}{}", p.name, i + 1), p));
		}
	}
	RunSpec {
		id: id.to_string(),
		strategy,
		peer_set: "test".into(),
		peers: list,
		scenario,
		link: LinkCfg::default(),
		run: RunCfg { duration_s: 3600, quiesce_s: 120, min_duration_s: 60, ..Default::default() },
		seed: 3,
	}
}

fn out_dir(name: &str) -> PathBuf {
	let d = std::env::temp_dir().join(format!("gossip-sim-test-{}-{name}", std::process::id()));
	let _ = std::fs::remove_dir_all(&d);
	d
}

fn f(row: &sim::metrics::Row, k: &str) -> f64 {
	row.get(k).unwrap().parse().unwrap()
}

#[test]
fn runs_are_deterministic() {
	let s = spec("det", StrategySpec::Name("range_then_scids".into()), &[(Kind::Lnd, 1), (Kind::Cln, 1), (Kind::Eclair, 1)], Scenario::Bootstrap);
	let a = engine::run(&s, corpus(), &out_dir("det-a")).unwrap();
	let b = engine::run(&s, corpus(), &out_dir("det-b")).unwrap();
	let (sa, sb) = (std::fs::read(&a.samples_path).unwrap(), std::fs::read(&b.samples_path).unwrap());
	assert!(!sa.is_empty());
	assert!(sa == sb, "samples differ between identical runs");
}

#[test]
fn baseline_filters_two_weeks_for_first_five_peers_then_one_hour() {
	let s = spec("filters", StrategySpec::Name("baseline".into()), &[(Kind::Ldk, 7)], Scenario::Bootstrap);
	let o = engine::run(&s, corpus(), &out_dir("filters")).unwrap();
	let firsts: Vec<u32> = o.peers.iter().map(|(_, st)| {
		assert_eq!(st.filters_from_ldk.len(), 1);
		assert_eq!(st.filters_from_ldk[0].1, u32::MAX);
		st.filters_from_ldk[0].0
	}).collect();
	// Peers connect 100 ms apart, all within the first simulated second.
	let now = corpus().meta.dump_time;
	assert!(firsts[..5].iter().all(|t| *t == now - 14 * 86_400), "{firsts:?}");
	assert!(firsts[5..].iter().all(|t| *t == now - 3600), "{firsts:?}");
	assert_eq!(f(&o.row, "frac_routable"), 1.0);
	assert_eq!(f(&o.row, "frac_upds"), 1.0);
	assert_eq!(o.row.get("closed").unwrap(), "0");
}

#[test]
fn baseline_gets_nothing_from_eclair_and_only_recent_gossip_from_cln() {
	let s = spec("ecl", StrategySpec::Name("baseline".into()), &[(Kind::Eclair, 2)], Scenario::Bootstrap);
	let o = engine::run(&s, corpus(), &out_dir("ecl")).unwrap();
	assert_eq!(f(&o.row, "frac_chans"), 0.0);
	let s = spec("cln", StrategySpec::Name("baseline".into()), &[(Kind::Cln, 2)], Scenario::Bootstrap);
	let o = engine::run(&s, corpus(), &out_dir("cln")).unwrap();
	assert!(f(&o.row, "frac_routable") < 0.5, "CLN replays only its last ~2h for a non-zero filter");
}

#[test]
fn range_then_scids_syncs_from_cln_without_concurrent_queries() {
	let s = spec("rts", StrategySpec::Name("range_then_scids".into()), &[(Kind::Cln, 3)], Scenario::Bootstrap);
	let o = engine::run(&s, corpus(), &out_dir("rts")).unwrap();
	assert_eq!(f(&o.row, "frac_routable"), 1.0);
	assert_eq!(f(&o.row, "frac_upds"), 1.0);
	assert!(!o.row.get("t_converged_s").unwrap().is_empty());
	assert_eq!(f(&o.row, "warnings_to_ldk"), 0.0, "a second in-flight scid query would get a warning");
	let queried: u64 = o.peers.iter().filter_map(|(_, st)| st.from_ldk.get(&msg_type::QUERY_SHORT_CHANNEL_IDS)).map(|(n, _)| n).sum();
	assert!(queried > 0);
}

#[test]
fn range_then_scids_works_around_ldk_peers_ignoring_scid_queries() {
	let s = spec("rts-ldk", StrategySpec::Name("range_then_scids".into()), &[(Kind::Ldk, 1), (Kind::Lnd, 1)], Scenario::Bootstrap);
	let o = engine::run(&s, corpus(), &out_dir("rts-ldk")).unwrap();
	assert_eq!(f(&o.row, "frac_routable"), 1.0, "work assigned to the LDK peer must move to the LND peer");
}

#[test]
fn restart_needs_less_than_bootstrap_with_range_queries() {
	let st = StrategySpec::Table(StrategyParams { name: "range_then_scids".into(), ..Default::default() });
	let boot = engine::run(&spec("b", st.clone(), &[(Kind::Lnd, 2)], Scenario::Bootstrap), corpus(), &out_dir("b")).unwrap();
	let rst = engine::run(&spec("r", st, &[(Kind::Lnd, 2)], Scenario::Restart { offline_secs: 3600, persisted: Default::default() }), corpus(), &out_dir("r")).unwrap();
	assert_eq!(f(&rst.row, "frac_upds"), 1.0);
	assert!(f(&rst.row, "rx_bytes") < f(&boot.row, "rx_bytes") / 3.0);
}

#[test]
fn balanced_assignment_spreads_scid_queries_across_peers() {
	let mk = |balance| StrategySpec::Table(StrategyParams { name: "range_then_scids".into(), balance: Some(balance), ..Default::default() });
	let bal = engine::run(&spec("bal", mk(true), &[(Kind::Lnd, 3)], Scenario::Bootstrap), corpus(), &out_dir("bal")).unwrap();
	let first = engine::run(&spec("first", mk(false), &[(Kind::Lnd, 3)], Scenario::Bootstrap), corpus(), &out_dir("first")).unwrap();
	let queried = |o: &engine::RunOutput| -> Vec<u64> {
		o.peers.iter().map(|(_, st)| st.from_ldk.get(&msg_type::QUERY_SHORT_CHANNEL_IDS).map_or(0, |x| x.0)).collect()
	};
	assert!(queried(&bal).iter().all(|n| *n > 0), "{:?}", queried(&bal));
	let t = |o: &engine::RunOutput| f(&o.row, "t_converged_s");
	assert!(t(&bal) * 2.0 < t(&first), "balanced {} vs first-come {}", t(&bal), t(&first));
}

#[test]
fn reconnect_requests_full_syncs_until_five_are_used() {
	let first_filters = |n: usize| {
		let mut s = spec(&format!("rc{n}"), StrategySpec::Name("baseline".into()), &[(Kind::Lnd, n)], Scenario::Bootstrap);
		s.run.reconnect = Some(engine::ReconnectCfg { down_s: 60 });
		let o = engine::run(&s, corpus(), &out_dir(&format!("rc{n}"))).unwrap();
		assert!(f(&o.row, "rx_bytes_after_reconnect") > 0.0);
		let now = corpus().meta.dump_time;
		// The first n entries are the original connections, the rest their reconnected copies.
		// Two-week filters start ~14 days back, one-hour filters within the run.
		o.peers.iter().map(|(_, st)| (st.filters_from_ldk[0].0 < now - 86_400) as usize).collect::<Vec<_>>()
	};
	assert_eq!(first_filters(3), [1, 1, 1, 1, 1, 0]);
	assert_eq!(first_filters(5), [1, 1, 1, 1, 1, 0, 0, 0, 0, 0]);
}

#[test]
fn queries_then_filter_never_asks_for_a_replay_and_requeries_on_reconnect() {
	let run = |st: &str, peers: &[(Kind, usize)]| {
		let mut s = spec(&format!("qtf-{st}"), StrategySpec::Name(st.into()), peers, Scenario::Bootstrap);
		s.run.reconnect = Some(engine::ReconnectCfg { down_s: 60 });
		engine::run(&s, corpus(), &out_dir(&format!("qtf-{st}-{}", peers.len()))).unwrap()
	};
	let o = run("queries_then_filter", &[(Kind::Ldk, 1), (Kind::Lnd, 3)]);
	assert_eq!(f(&o.row, "frac_routable"), 1.0);
	let now = corpus().meta.dump_time;
	for (label, st) in &o.peers {
		assert!(st.filters_from_ldk.iter().all(|(first, _)| *first >= now - 3600), "{label}: {:?}", st.filters_from_ldk);
	}
	// The first 3 peers held the range slots; their reconnected copies (entries 4..7) get them back.
	let ranged: Vec<bool> =
		o.peers.iter().map(|(_, st)| st.from_ldk.contains_key(&msg_type::QUERY_CHANNEL_RANGE)).collect();
	assert_eq!(ranged, [true, true, true, false, true, true, true, false]);

	let qtf = run("queries_then_filter", &[(Kind::Lnd, 3)]);
	let base = run("baseline", &[(Kind::Lnd, 3)]);
	let after = |o: &engine::RunOutput| f(&o.row, "rx_bytes_after_reconnect");
	assert!(after(&qtf) < after(&base) / 2.0, "qtf {} baseline {}", after(&qtf), after(&base));
}
