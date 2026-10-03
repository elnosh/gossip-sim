//! Experiment files: strategies x peer sets x scenarios, run in parallel.

use corpus::{Corpus, StructuredDrop};
use serde::Deserialize;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::engine::{self, LinkCfg, RunCfg, RunSpec};
use crate::metrics::{self, Row};
use crate::profile::Profile;
use crate::scenario::{parse_duration, PersistedUpdates, Scenario};
use crate::strategy::StrategySpec;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Experiment {
	/// Graph file (paths are relative to the working directory).
	pub corpus: PathBuf,
	#[serde(default = "default_profiles_dir")]
	pub profiles_dir: PathBuf,
	pub out_dir: PathBuf,
	#[serde(default = "default_seed")]
	pub seed: u64,
	pub strategies: Vec<StrategySpec>,
	#[serde(default)]
	pub run: RunCfg,
	#[serde(default)]
	pub link: LinkCfg,
	pub peer_sets: Vec<PeerSetCfg>,
	pub scenarios: Vec<ScenarioCfg>,
}

fn default_profiles_dir() -> PathBuf {
	"profiles".into()
}
fn default_seed() -> u64 {
	1
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerSetCfg {
	pub name: String,
	pub peers: Vec<PeerCfg>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerCfg {
	pub profile: String,
	/// Number of identical peers of this profile (each gets its own view sample).
	#[serde(default = "one")]
	pub count: usize,
	/// Extra structured differences from the corpus, added to the profile's view filter.
	#[serde(default)]
	pub drops: Vec<StructuredDrop>,
}

fn one() -> usize {
	1
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScenarioCfg {
	Bootstrap,
	Restart {
		offline: Vec<String>,
		#[serde(default)]
		persisted_updates: PersistedUpdates,
	},
}

impl Experiment {
	pub fn load(path: &Path) -> crate::Result<Experiment> {
		Ok(toml::from_str(&std::fs::read_to_string(path)?)?)
	}

	pub fn peer_sets(&self) -> crate::Result<Vec<(String, Vec<(String, Profile)>)>> {
		let mut sets = Vec::new();
		for ps in &self.peer_sets {
			let mut peers = Vec::new();
			for pc in &ps.peers {
				let mut profile = Profile::load(&self.profiles_dir, &pc.profile)?;
				profile.view.drops.extend(pc.drops.iter().cloned());
				for _ in 0..pc.count {
					let n = peers.iter().filter(|(_, p): &&(String, Profile)| p.name == profile.name).count();
					peers.push((format!("{}{}", profile.name, n + 1), profile.clone()));
				}
			}
			sets.push((ps.name.clone(), peers));
		}
		Ok(sets)
	}

	pub fn scenarios(&self) -> crate::Result<Vec<Scenario>> {
		let mut v = Vec::new();
		for s in &self.scenarios {
			match s {
				ScenarioCfg::Bootstrap => v.push(Scenario::Bootstrap),
				ScenarioCfg::Restart { offline, persisted_updates } => {
					for o in offline {
						v.push(Scenario::Restart { offline_secs: parse_duration(o)?, persisted: *persisted_updates });
					}
				},
			}
		}
		Ok(v)
	}

	pub fn expand(&self) -> crate::Result<Vec<RunSpec>> {
		let mut runs = Vec::new();
		for scenario in self.scenarios()? {
			for (set_name, peers) in self.peer_sets()? {
				for strategy in &self.strategies {
					strategy.build()?; // validate early
					runs.push(RunSpec {
						id: format!("{}__{}__{}", scenario.label(), set_name, strategy.label()),
						strategy: strategy.clone(),
						peer_set: set_name.clone(),
						peers: peers.clone(),
						scenario,
						link: self.link.clone(),
						run: self.run.clone(),
						seed: self.seed,
					});
				}
			}
		}
		Ok(runs)
	}
}

/// Run all specs on `jobs` threads; returns summary rows in spec order.
pub fn run_all(specs: Vec<RunSpec>, corpus: &Corpus, out_dir: &Path, jobs: usize) -> Vec<Row> {
	let total = specs.len();
	let queue: Mutex<VecDeque<(usize, RunSpec)>> = Mutex::new(specs.into_iter().enumerate().collect());
	let results: Mutex<Vec<(usize, Row)>> = Mutex::new(Vec::new());
	std::thread::scope(|s| {
		for _ in 0..jobs.max(1) {
			s.spawn(|| loop {
				let Some((idx, spec)) = queue.lock().unwrap().pop_front() else { break };
				let started = std::time::Instant::now();
				let row = match engine::run(&spec, corpus, out_dir) {
					Ok(o) => o.row,
					Err(e) => {
						let mut r = Row::default();
						r.put("run_id", &spec.id);
						r.put("error", e.to_string());
						r
					},
				};
				let done = results.lock().unwrap().len() + 1;
				eprintln!(
					"[{done}/{total}] {} end={} converged={}s rx={}B tx={}B routable={} upds={} ({:.1}s wall)",
					spec.id,
					row.get("end_reason").unwrap_or("error"),
					row.get("t_converged_s").filter(|s| !s.is_empty()).unwrap_or("-"),
					row.get("rx_bytes").unwrap_or("-"),
					row.get("tx_bytes").unwrap_or("-"),
					row.get("frac_routable").unwrap_or("-"),
					row.get("frac_upds").unwrap_or("-"),
					started.elapsed().as_secs_f64(),
				);
				results.lock().unwrap().push((idx, row));
			});
		}
	});
	let mut rows = results.into_inner().unwrap();
	rows.sort_by_key(|(i, _)| *i);
	rows.into_iter().map(|(_, r)| r).collect()
}

pub fn write_summary(out_dir: &Path, rows: &[Row]) -> std::io::Result<PathBuf> {
	std::fs::create_dir_all(out_dir)?;
	let path = out_dir.join("summary.csv");
	metrics::write_csv(&path, rows)?;
	Ok(path)
}
