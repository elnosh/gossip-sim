//! Discrete-event engine: one LDK node (real `PeerManager` + `P2PGossipSync` + `NetworkGraph`)
//! connected to simulated peers over modelled links, under a simulated clock.

use corpus::{Corpus, GroundTruth, PeerView};
use lightning::ln::peer_handler::{ErroringMessageHandler, IgnoringMessageHandler, MessageHandler};
use lightning::routing::gossip::P2PGossipSync;
use lightning::util::sim_clock::{set_skip_sig_verify, set_unix_now};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde::Deserialize;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::ldk::{Graph, Keys, NoUtxo, Pm, SimLogger};
use crate::link::{Link, SimTime};
use crate::metrics::{self, Completeness, PeerSample, Row, Sample};
use crate::peer::{Next, SimPeer, TypeCounters};
use crate::profile::Profile;
use crate::scenario::{self, Scenario};
use crate::socket::SimDescriptor;
use crate::strategy::{StrategyHandler, StrategySpec};

const SEC: SimTime = 1_000_000;

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RunCfg {
	/// Hard stop.
	pub duration_s: u64,
	/// Stop early once no gossip moved for this long, the strategy is idle and peers have nothing
	/// queued.
	pub quiesce_s: u64,
	/// Never stop early before this.
	pub min_duration_s: u64,
	/// (from_s, every_s) steps for completeness samples.
	pub sample_every_s: Vec<(u64, u64)>,
	pub converge_routable: f64,
	pub converge_upds: f64,
	pub verify_sigs: bool,
	/// Delay between successive outbound connections at startup.
	pub connect_spacing_ms: u64,
}

impl Default for RunCfg {
	fn default() -> Self {
		RunCfg {
			duration_s: 4 * 3600,
			quiesce_s: 300,
			min_duration_s: 120,
			sample_every_s: vec![(0, 1), (300, 5), (1800, 30)],
			converge_routable: 0.99,
			converge_upds: 0.95,
			verify_sigs: false,
			connect_spacing_ms: 100,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LinkCfg {
	/// One-way latency.
	pub latency_ms: u64,
	pub bandwidth_mbps: f64,
	/// Kernel send buffer on each side.
	pub sock_buf_kb: usize,
}

impl Default for LinkCfg {
	fn default() -> Self {
		LinkCfg { latency_ms: 40, bandwidth_mbps: 20.0, sock_buf_kb: 256 }
	}
}

#[derive(Debug, Clone)]
pub struct RunSpec {
	pub id: String,
	pub strategy: StrategySpec,
	pub peer_set: String,
	pub peers: Vec<(String, Profile)>,
	pub scenario: Scenario,
	pub link: LinkCfg,
	pub run: RunCfg,
	pub seed: u64,
}

enum Ev {
	Connect(usize),
	/// Bytes arrive; `to_ldk` selects the direction.
	Deliver { conn: usize, to_ldk: bool, bytes: Vec<u8> },
	/// Bytes the node sent left its socket buffer.
	LinkDrain { conn: usize, len: usize },
	PeerPump(usize),
	LdkTick,
	LdkPrune,
	StrategyTick,
	Sample,
	End,
}

struct Scheduled {
	at: SimTime,
	seq: u64,
	ev: Ev,
}
impl PartialEq for Scheduled {
	fn eq(&self, o: &Self) -> bool {
		(self.at, self.seq) == (o.at, o.seq)
	}
}
impl Eq for Scheduled {}
impl PartialOrd for Scheduled {
	fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(o))
	}
}
impl Ord for Scheduled {
	fn cmp(&self, o: &Self) -> std::cmp::Ordering {
		(self.at, self.seq).cmp(&(o.at, o.seq))
	}
}

struct Conn {
	peer: SimPeer,
	desc: SimDescriptor,
	to_peer: Link,
	to_ldk: Link,
	ldk_rx: VecDeque<Vec<u8>>,
	alive: bool,
	closed_reason: Option<String>,
	pump_at: Option<SimTime>,
}

pub struct RunOutput {
	pub row: Row,
	pub samples_path: PathBuf,
	/// Per peer: label and traffic statistics.
	pub peers: Vec<(String, crate::peer::PeerStats)>,
}

struct Sim<'a> {
	spec: &'a RunSpec,
	now: SimTime,
	epoch: u64,
	seq: u64,
	queue: BinaryHeap<Reverse<Scheduled>>,
	pm: Arc<Pm>,
	handler: Arc<StrategyHandler>,
	conns: Vec<Conn>,
	gt: GroundTruth,
	max_backlog_us: u64,
	samples: std::io::BufWriter<std::fs::File>,
	next_sample: SimTime,
	last_applied: u64,
	t_last_change: SimTime,
	converged: Option<(SimTime, TypeCounters, TypeCounters)>,
	last_c: Completeness,
	end_reason: Option<&'static str>,
}

impl<'a> Sim<'a> {
	fn schedule(&mut self, at: SimTime, ev: Ev) {
		self.seq += 1;
		self.queue.push(Reverse(Scheduled { at, seq: self.seq, ev }));
	}

	fn unix(&self) -> u64 {
		self.epoch + self.now / SEC
	}

	fn schedule_pump(&mut self, i: usize, at: SimTime) {
		if self.conns[i].pump_at.map_or(true, |x| at < x) {
			self.conns[i].pump_at = Some(at);
			self.schedule(at, Ev::PeerPump(i));
		}
	}

	fn close(&mut self, i: usize, reason: String, notify_ldk: bool) {
		let c = &mut self.conns[i];
		if !c.alive {
			return;
		}
		c.alive = false;
		c.closed_reason = Some(reason);
		if notify_ldk {
			self.pm.socket_disconnected(&c.desc);
		}
	}

	/// Feed buffered bytes to LDK while it is not read-paused, run `process_events`, and move
	/// everything LDK wrote onto the links. Repeats until nothing changes.
	fn pump_ldk(&mut self) {
		loop {
			let mut read_any = false;
			for i in 0..self.conns.len() {
				loop {
					let c = &mut self.conns[i];
					if !c.alive || c.ldk_rx.is_empty() || c.desc.st.lock().unwrap().read_paused {
						break;
					}
					let chunk = c.ldk_rx.pop_front().unwrap();
					let mut d = c.desc.clone();
					read_any = true;
					if let Err(e) = self.pm.read_event(&mut d, &chunk) {
						self.close(i, format!("ldk read_event error: {e:?}"), false);
						break;
					}
				}
			}
			if read_any {
				self.pm.process_events();
			}
			self.flush_outboxes();
			if !read_any {
				break;
			}
		}
	}

	fn flush_outboxes(&mut self) {
		for i in 0..self.conns.len() {
			let (bytes, disc) = {
				let mut s = self.conns[i].desc.st.lock().unwrap();
				(std::mem::take(&mut s.outbox), s.disconnect_requested)
			};
			if !bytes.is_empty() && self.conns[i].alive {
				let len = bytes.len();
				let (finish, deliver) = self.conns[i].to_peer.send(self.now, len);
				self.schedule(deliver, Ev::Deliver { conn: i, to_ldk: false, bytes });
				self.schedule(finish, Ev::LinkDrain { conn: i, len });
			}
			if disc {
				self.close(i, "ldk disconnected the peer".into(), false);
			}
		}
	}

	fn pump_peer(&mut self, i: usize) {
		if self.conns[i].pump_at == Some(self.now) {
			self.conns[i].pump_at = None;
		}
		loop {
			let c = &mut self.conns[i];
			if !c.alive {
				return;
			}
			let backlog = c.to_ldk.backlog_us(self.now);
			if backlog > self.max_backlog_us {
				let at = self.now + backlog - self.max_backlog_us;
				self.schedule_pump(i, at);
				return;
			}
			match c.peer.next(self.now) {
				Next::Frame(frame, _ty) => {
					let (_, deliver) = c.to_ldk.send(self.now, frame.len());
					self.schedule(deliver, Ev::Deliver { conn: i, to_ldk: true, bytes: frame });
				},
				Next::WaitUntil(t) => {
					self.schedule_pump(i, t);
					return;
				},
				Next::Idle => return,
			}
		}
	}

	fn dispatch(&mut self, ev: Ev) {
		match ev {
			Ev::Connect(i) => {
				let c = &mut self.conns[i];
				match self.pm.new_outbound_connection(c.peer.node_id, c.desc.clone(), None) {
					Ok(act1) => {
						let len = act1.len();
						let (_, deliver) = c.to_peer.send(self.now, len);
						self.schedule(deliver, Ev::Deliver { conn: i, to_ldk: false, bytes: act1 });
					},
					Err(e) => self.close(i, format!("connect failed: {e:?}"), false),
				}
			},
			Ev::Deliver { conn, to_ldk: true, bytes } => {
				if self.conns[conn].alive {
					self.conns[conn].ldk_rx.push_back(bytes);
					self.pump_ldk();
				}
			},
			Ev::Deliver { conn, to_ldk: false, bytes } => {
				let now_unix = self.unix() as u32;
				let c = &mut self.conns[conn];
				if !c.alive {
					return;
				}
				c.peer.on_bytes(self.now, now_unix, &bytes);
				if let Some(why) = c.peer.failed.clone() {
					self.close(conn, format!("peer failed: {why}"), true);
					self.pump_ldk();
				} else if c.peer.has_pending() {
					self.schedule_pump(conn, self.now);
				}
			},
			Ev::LinkDrain { conn, len } => {
				let pending = {
					let mut s = self.conns[conn].desc.st.lock().unwrap();
					s.sock_free += len;
					std::mem::take(&mut s.pending_space_avail)
				};
				if pending && self.conns[conn].alive {
					let mut d = self.conns[conn].desc.clone();
					if let Err(e) = self.pm.write_buffer_space_avail(&mut d) {
						self.close(conn, format!("ldk write error: {e:?}"), false);
					}
				}
				self.pump_ldk();
			},
			Ev::PeerPump(i) => self.pump_peer(i),
			Ev::LdkTick => {
				self.pm.timer_tick_occurred();
				self.pm.process_events();
				self.pump_ldk();
				self.schedule(self.now + 10 * SEC, Ev::LdkTick);
			},
			Ev::LdkPrune => {
				self.handler.graph().remove_stale_channels_and_tracking_with_time(self.unix());
				self.schedule(self.now + 3600 * SEC, Ev::LdkPrune);
			},
			Ev::StrategyTick => {
				self.handler.on_timer();
				self.pm.process_events();
				self.pump_ldk();
				self.schedule(self.now + SEC, Ev::StrategyTick);
			},
			Ev::Sample => {
				self.sample();
				if self.end_reason.is_none() {
					let t = self.now + self.sample_period();
					self.next_sample = t;
					self.schedule(t, Ev::Sample);
				}
			},
			Ev::End => {
				if self.end_reason.is_none() {
					self.end_reason = Some("duration");
				}
			},
		}
	}

	fn sample_period(&self) -> SimTime {
		let t_s = self.now / SEC;
		let mut every = 1;
		for (from, e) in &self.spec.run.sample_every_s {
			if t_s >= *from {
				every = *e;
			}
		}
		every.max(1) * SEC
	}

	fn totals(&self) -> (TypeCounters, TypeCounters) {
		let mut rx = TypeCounters::new();
		let mut tx = TypeCounters::new();
		for c in &self.conns {
			metrics::merge(&mut rx, &c.peer.stats.to_ldk);
			metrics::merge(&mut tx, &c.peer.stats.from_ldk);
		}
		(rx, tx)
	}

	fn sample(&mut self) {
		let c = Completeness::measure(self.handler.graph(), &self.gt);
		let applied = self.handler.applied.load(Ordering::Relaxed);
		if applied != self.last_applied {
			self.last_applied = applied;
			self.t_last_change = self.now;
		}
		let (rx, tx) = self.totals();
		let idle = self.handler.is_idle();
		if self.converged.is_none()
			&& c.frac_routable() >= self.spec.run.converge_routable
			&& c.frac_upds() >= self.spec.run.converge_upds
			&& idle
		{
			self.converged = Some((self.now, rx.clone(), tx.clone()));
		}
		let s = Sample {
			t_s: self.now as f64 / SEC as f64,
			c: c.clone(),
			applied,
			rejected: self.handler.rejected.load(Ordering::Relaxed),
			rx: metrics::by_family(&rx),
			tx: metrics::by_family(&tx),
			peers: self
				.conns
				.iter()
				.map(|c| PeerSample {
					label: c.peer.label.clone(),
					rx: metrics::total(&c.peer.stats.to_ldk),
					tx: metrics::total(&c.peer.stats.from_ldk),
					queued_msgs: c.peer.queued_msgs(),
				})
				.collect(),
		};
		serde_json::to_writer(&mut self.samples, &s).expect("write sample");
		self.samples.write_all(b"\n").expect("write sample");
		self.last_c = c;

		let run = &self.spec.run;
		let last_gossip = self.conns.iter().map(|c| c.peer.last_gossip).max().unwrap_or(0);
		let peers_busy = self.conns.iter().any(|c| c.alive && c.peer.has_pending());
		if self.now >= run.min_duration_s * SEC
			&& self.now.saturating_sub(last_gossip) >= run.quiesce_s * SEC
			&& idle
			&& !peers_busy
		{
			self.end_reason = Some("quiesced");
		}
	}

	fn summary(&self) -> Row {
		let spec = self.spec;
		let mut r = Row::default();
		r.put("run_id", &spec.id);
		r.put("strategy", spec.strategy.label());
		r.put("peer_set", &spec.peer_set);
		r.put("scenario", if spec.scenario == Scenario::Bootstrap { "bootstrap" } else { "restart" });
		r.put("offline_s", spec.scenario.offline_secs());
		r.put("seed", spec.seed);
		r.put("end_reason", self.end_reason.unwrap_or(""));
		r.put("t_end_s", self.now / SEC);
		r.put("t_converged_s", self.converged.as_ref().map(|c| (c.0 / SEC).to_string()).unwrap_or_default());
		r.put("t_last_change_s", self.t_last_change / SEC);
		let c = &self.last_c;
		r.put("frac_chans", format!("{:.4}", c.frac_chans()));
		r.put("frac_routable", format!("{:.4}", c.frac_routable()));
		r.put("frac_upds", format!("{:.4}", c.frac_upds()));
		r.put("frac_nodes", format!("{:.4}", c.frac_nodes()));
		r.put("chans_gt", c.chans_gt);
		r.put("routable_gt", c.routable_gt);
		r.put("upds_gt", c.upds_gt);
		r.put("nodes_gt", c.nodes_gt);
		r.put("chans_extra", c.chans_extra);
		r.put("upds_stale", c.upds_stale);
		let (rx, tx) = self.totals();
		r.put("rx_bytes", metrics::total(&rx));
		r.put("tx_bytes", metrics::total(&tx));
		let (rx0, tx0) = match &self.converged {
			Some((_, a, b)) => (metrics::total(a), metrics::total(b)),
			None => (metrics::total(&rx), metrics::total(&tx)),
		};
		r.put("rx_bytes_to_converge", rx0);
		r.put("tx_bytes_to_converge", tx0);
		for fam in ["chan_ann", "chan_upd", "node_ann", "query", "reply", "ping", "handshake", "other"] {
			r.put(&format!("rx_{fam}"), metrics::by_family(&rx).get(fam).copied().unwrap_or(0));
		}
		for fam in ["chan_ann", "chan_upd", "node_ann", "query", "reply", "ping", "handshake", "other"] {
			r.put(&format!("tx_{fam}"), metrics::by_family(&tx).get(fam).copied().unwrap_or(0));
		}
		r.put("applied", self.handler.applied.load(Ordering::Relaxed));
		r.put("rejected", self.handler.rejected.load(Ordering::Relaxed));
		let warnings: u64 = self
			.conns
			.iter()
			.map(|c| c.peer.stats.to_ldk.get(&corpus::msg_type::WARNING).map_or(0, |x| x.0))
			.sum();
		r.put("warnings_to_ldk", warnings);
		let closed: Vec<String> = self
			.conns
			.iter()
			.filter_map(|c| c.closed_reason.as_ref().map(|w| format!("{}: {}", c.peer.label, w)))
			.collect();
		r.put("closed", closed.len());
		r.put("closed_reasons", closed.join("; "));
		r
	}
}

/// Seed for peer `i`'s view and keys, independent of the strategy so all strategies see identical
/// peers.
fn peer_seed(seed: u64, i: usize) -> u64 {
	seed ^ (i as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

pub fn derive_views(corpus: &Corpus, peers: &[(String, Profile)], seed: u64) -> Vec<Arc<PeerView>> {
	peers
		.iter()
		.enumerate()
		.map(|(i, (_, p))| {
			let mut rng = ChaCha8Rng::seed_from_u64(peer_seed(seed, i));
			Arc::new(PeerView::derive(corpus, corpus.meta.dump_time, &p.view, &mut rng))
		})
		.collect()
}

pub fn run(spec: &RunSpec, corpus: &Corpus, out_dir: &Path) -> crate::Result<RunOutput> {
	set_skip_sig_verify(!spec.run.verify_sigs);
	let epoch = corpus.meta.dump_time as u64;
	set_unix_now(epoch);
	let logger = Arc::new(SimLogger::from_env("ldk"));

	let views = derive_views(corpus, &spec.peers, spec.seed);
	let gt = GroundTruth::union(views.iter().map(|v| v.as_ref()));

	let graph: Arc<Graph> = Arc::new(scenario::initial_graph(corpus, spec.scenario, logger.clone()));
	let chain_hash = graph.get_chain_hash();
	let gossip = P2PGossipSync::new(graph.clone(), None::<Arc<NoUtxo>>, logger.clone());
	let handler = Arc::new(StrategyHandler::new(gossip, spec.strategy.build()?));

	let mut rng = ChaCha8Rng::seed_from_u64(spec.seed);
	let keys = Arc::new(Keys::new(&rng.gen::<[u8; 32]>(), epoch, 0, true, logger.clone()));
	let pm = Arc::new(Pm::new(
		MessageHandler {
			chan_handler: Arc::new(ErroringMessageHandler::new()),
			route_handler: handler.clone(),
			onion_message_handler: Arc::new(IgnoringMessageHandler {}),
			custom_message_handler: Arc::new(IgnoringMessageHandler {}),
			send_only_message_handler: Arc::new(IgnoringMessageHandler {}),
		},
		epoch as u32,
		&rng.gen::<[u8; 32]>(),
		logger.clone(),
		keys,
	));

	let link = &spec.link;
	let bps = link.bandwidth_mbps * 1e6 / 8.0;
	let latency_us = link.latency_ms * 1000;
	let sock_buf = link.sock_buf_kb * 1024;
	let conns = spec
		.peers
		.iter()
		.zip(views)
		.enumerate()
		.map(|(i, ((label, profile), view))| {
			let prng = ChaCha8Rng::seed_from_u64(peer_seed(spec.seed, i).rotate_left(17));
			Conn {
				peer: SimPeer::new(label.clone(), profile.clone(), view, 2 * latency_us, epoch, prng, chain_hash),
				desc: SimDescriptor::new(i, sock_buf),
				to_peer: Link::new(latency_us, bps),
				to_ldk: Link::new(latency_us, bps),
				ldk_rx: VecDeque::new(),
				alive: true,
				closed_reason: None,
				pump_at: None,
			}
		})
		.collect::<Vec<_>>();

	let run_dir = out_dir.join("runs").join(&spec.id);
	std::fs::create_dir_all(&run_dir)?;
	let samples_path = run_dir.join("samples.jsonl");
	let samples = std::io::BufWriter::new(std::fs::File::create(&samples_path)?);

	let mut sim = Sim {
		spec,
		now: 0,
		epoch,
		seq: 0,
		queue: BinaryHeap::new(),
		pm,
		handler,
		conns,
		gt,
		max_backlog_us: (sock_buf as f64 / bps * 1e6) as u64,
		samples,
		next_sample: 0,
		last_applied: 0,
		t_last_change: 0,
		converged: None,
		last_c: Completeness::default(),
		end_reason: None,
	};
	for i in 0..sim.conns.len() {
		sim.schedule(i as u64 * spec.run.connect_spacing_ms * 1000, Ev::Connect(i));
	}
	sim.schedule(10 * SEC, Ev::LdkTick);
	sim.schedule(60 * SEC, Ev::LdkPrune);
	sim.schedule(SEC, Ev::StrategyTick);
	sim.schedule(0, Ev::Sample);
	sim.schedule(spec.run.duration_s * SEC, Ev::End);

	while let Some(Reverse(s)) = sim.queue.pop() {
		sim.now = s.at;
		set_unix_now(sim.unix());
		sim.dispatch(s.ev);
		if sim.end_reason.is_some() {
			break;
		}
	}
	if sim.next_sample != sim.now {
		sim.sample();
	}
	sim.samples.flush()?;
	let peers = sim.conns.iter().map(|c| (c.peer.label.clone(), c.peer.stats.clone())).collect();
	Ok(RunOutput { row: sim.summary(), samples_path, peers })
}
