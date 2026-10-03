//! `gossip-sim` command line.

use corpus::GroundTruth;
use sim::engine;
use sim::runner::{self, Experiment};
use sim::scenario::{initial_graph, Scenario};
use std::path::PathBuf;
use std::process::exit;
use std::sync::Arc;

const USAGE: &str = "usage:
  gossip-sim import <graph.json>
      Build the signed corpus and cache it as <graph.json>.corpus.
  gossip-sim mock <out.json> [nodes] [channels]
      Write a mock graph (for smoke tests).
  gossip-sim views <experiment.toml>
      Print what each peer set's peers hold and the restart graph sizes, without running.
  gossip-sim run <experiment.toml> [-j N] [--only SUBSTR] [--check-determinism]
      Run the experiment matrix, write <out_dir>/runs/*/samples.jsonl and <out_dir>/summary.csv.";

fn die(msg: impl std::fmt::Display) -> ! {
	eprintln!("{msg}");
	exit(1)
}

fn usage() -> ! {
	eprintln!("{USAGE}");
	exit(2)
}

fn load_corpus(exp: &Experiment) -> Arc<corpus::Corpus> {
	let started = std::time::Instant::now();
	let c = corpus::import::load_or_import(&exp.corpus)
		.unwrap_or_else(|e| die(format!("loading {}: {e}", exp.corpus.display())));
	eprintln!(
		"corpus: {} channels, {} updates, {} node announcements, dump_time {} ({:.1}s)",
		c.chans.len(),
		c.update_count(),
		c.nodes.len(),
		c.meta.dump_time,
		started.elapsed().as_secs_f64()
	);
	Arc::new(c)
}

fn main() {
	let args: Vec<String> = std::env::args().skip(1).collect();
	match args.first().map(String::as_str) {
		Some("import") => {
			let Some(path) = args.get(1) else { usage() };
			let path = PathBuf::from(path);
			let started = std::time::Instant::now();
			let (c, stats) = corpus::import::import_graph(&path).unwrap_or_else(|e| die(e));
			let cache = corpus::import::cache_path(&path);
			c.save(&cache).unwrap_or_else(|e| die(e));
			println!("{stats}");
			println!(
				"dump_time={} max_height={} channels={} updates={} node_anns={}",
				c.meta.dump_time,
				c.meta.max_height,
				c.chans.len(),
				c.update_count(),
				c.nodes.len()
			);
			println!("wrote {} in {:.1}s", cache.display(), started.elapsed().as_secs_f64());
		},
		Some("mock") => {
			let Some(path) = args.get(1) else { usage() };
			let nodes = args.get(2).map(|s| s.parse().unwrap_or_else(|_| usage())).unwrap_or(500);
			let chans = args.get(3).map(|s| s.parse().unwrap_or_else(|_| usage())).unwrap_or(1500);
			let json = corpus::mock::json(42, nodes, chans, 1_787_747_191, 964_125);
			std::fs::write(path, json).unwrap_or_else(|e| die(e));
			println!("wrote {path} ({nodes} nodes, {chans} channels)");
		},
		Some("views") => {
			let Some(path) = args.get(1) else { usage() };
			let exp = Experiment::load(&PathBuf::from(path)).unwrap_or_else(|e| die(e));
			let corpus = load_corpus(&exp);
			for (name, peers) in exp.peer_sets().unwrap_or_else(|e| die(e)) {
				let views = engine::derive_views(&corpus, &peers, exp.seed);
				let gt = GroundTruth::union(views.iter().map(|v| v.as_ref()));
				println!("peer set {name}: ground truth {} channels, {} updates, {} nodes", gt.chans.len(), gt.update_count(), gt.nodes.len());
				for ((label, _), v) in peers.iter().zip(&views) {
					let routable = v.chans.values().filter(|c| c.has_any_update()).count();
					println!(
						"  {label:10} channels {:6} routable {:6} updates {:6} nodes {:6}",
						v.chans.len(),
						routable,
						v.update_count(),
						v.nodes.len()
					);
				}
			}
			lightning::util::sim_clock::set_skip_sig_verify(true);
			let logger = Arc::new(sim::ldk::SimLogger::from_env("views"));
			for s in exp.scenarios().unwrap_or_else(|e| die(e)) {
				if let Scenario::Restart { .. } = s {
					let g = initial_graph(&corpus, s, logger.clone());
					let ro = g.read_only();
					let upds: usize = ro
						.channels()
						.unordered_iter()
						.map(|(_, c)| c.one_to_two.is_some() as usize + c.two_to_one.is_some() as usize)
						.sum();
					println!("{}: persisted graph {} channels, {} updates", s.label(), ro.channels().len(), upds);
				}
			}
		},
		Some("run") => {
			let Some(path) = args.get(1) else { usage() };
			let mut jobs = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
			let mut only: Option<String> = None;
			let mut check = false;
			let mut i = 2;
			while i < args.len() {
				match args[i].as_str() {
					"-j" => {
						jobs = args.get(i + 1).and_then(|s| s.parse().ok()).unwrap_or_else(|| usage());
						i += 1;
					},
					"--only" => {
						only = Some(args.get(i + 1).cloned().unwrap_or_else(|| usage()));
						i += 1;
					},
					"--check-determinism" => check = true,
					_ => usage(),
				}
				i += 1;
			}
			let exp = Experiment::load(&PathBuf::from(path)).unwrap_or_else(|e| die(e));
			let mut specs = exp.expand().unwrap_or_else(|e| die(e));
			if let Some(o) = &only {
				specs.retain(|s| s.id.contains(o.as_str()));
			}
			if specs.is_empty() {
				die("no runs selected");
			}
			let corpus = load_corpus(&exp);
			if check {
				let spec = &specs[0];
				let a = exp.out_dir.join("determinism/a");
				let b = exp.out_dir.join("determinism/b");
				let ra = engine::run(spec, &corpus, &a).unwrap_or_else(|e| die(e));
				let rb = engine::run(spec, &corpus, &b).unwrap_or_else(|e| die(e));
				let (sa, sb) = (std::fs::read(&ra.samples_path).unwrap(), std::fs::read(&rb.samples_path).unwrap());
				if sa == sb {
					println!("deterministic: {} produced identical samples ({} bytes) twice", spec.id, sa.len());
				} else {
					die(format!("NOT deterministic: {} and {} differ", ra.samples_path.display(), rb.samples_path.display()));
				}
				return;
			}
			eprintln!("{} runs on {} threads", specs.len(), jobs);
			let rows = runner::run_all(specs, &corpus, &exp.out_dir, jobs);
			let path = runner::write_summary(&exp.out_dir, &rows).unwrap_or_else(|e| die(e));
			println!("wrote {}", path.display());
		},
		_ => usage(),
	}
}
