//! Type aliases and small adapters for the LDK node under test.

use bitcoin::constants::ChainHash;
use lightning::ln::peer_handler::{ErroringMessageHandler, IgnoringMessageHandler, PeerManager};
use lightning::routing::gossip::{NetworkGraph, P2PGossipSync};
use lightning::routing::utxo::{UtxoLookup, UtxoLookupError, UtxoResult};
use lightning::sign::KeysManager;
use lightning::util::logger::{Level, Logger, Record};
use lightning::util::wakers::Notifier;
use std::sync::Arc;

use crate::socket::SimDescriptor;
use crate::strategy::StrategyHandler;

fn level_rank(l: Level) -> u8 {
	match l {
		Level::Gossip => 0,
		Level::Trace => 1,
		Level::Debug => 2,
		Level::Info => 3,
		Level::Warn => 4,
		Level::Error => 5,
	}
}

/// Logger printing to stderr at or above `min` (set from `GOSSIP_SIM_LDK_LOG`), silent otherwise.
pub struct SimLogger {
	min: Option<u8>,
	label: String,
}

impl SimLogger {
	pub fn from_env(label: &str) -> SimLogger {
		let min = std::env::var("GOSSIP_SIM_LDK_LOG").ok().and_then(|v| match v.as_str() {
			"gossip" => Some(0),
			"trace" => Some(1),
			"debug" => Some(2),
			"info" => Some(3),
			"warn" => Some(4),
			"error" => Some(5),
			_ => None,
		});
		SimLogger { min, label: label.to_string() }
	}
}

impl Logger for SimLogger {
	fn log(&self, record: Record) {
		if let Some(min) = self.min {
			if level_rank(record.level) >= min {
				let t = lightning::util::sim_clock::unix_now();
				eprintln!("[{}] {} {:?} {}: {}", self.label, t, record.level, record.module_path, record.args);
			}
		}
	}
}

/// UTXO lookups are disabled in the simulator (the synthetic funding outputs do not exist).
pub struct NoUtxo;
impl UtxoLookup for NoUtxo {
	fn get_utxo(&self, _: &ChainHash, _: u64, _: Arc<Notifier>) -> UtxoResult {
		UtxoResult::Sync(Err(UtxoLookupError::UnknownTx))
	}
}

pub type Graph = NetworkGraph<Arc<SimLogger>>;
pub type Gossip = P2PGossipSync<Arc<Graph>, Arc<NoUtxo>, Arc<SimLogger>>;
pub type Keys = KeysManager<Arc<SimLogger>>;
pub type Pm = PeerManager<
	SimDescriptor,
	Arc<ErroringMessageHandler>,
	Arc<StrategyHandler>,
	Arc<IgnoringMessageHandler>,
	Arc<SimLogger>,
	Arc<IgnoringMessageHandler>,
	Arc<Keys>,
	Arc<IgnoringMessageHandler>,
>;
