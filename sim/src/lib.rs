//! Simulates LDK graph sync strategies: a real LDK node (`PeerManager`, `P2PGossipSync`,
//! `NetworkGraph`) against simulated LND / CLN / Eclair / LDK peers under a discrete-event clock.
//! Requires the `gossip-sim` LDK fork built with `--cfg sim_clock` (see `.cargo/config.toml`).

pub mod engine;
pub mod ldk;
pub mod link;
pub mod metrics;
pub mod pace;
pub mod peer;
pub mod profile;
pub mod responders;
pub mod runner;
pub mod scenario;
pub mod socket;
pub mod strategy;
pub mod wire;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T> = std::result::Result<T, Error>;

/// Chain hash of the simulated network (mainnet; the corpus is signed for it).
pub fn ldk_chain_hash() -> bitcoin::constants::ChainHash {
	corpus::import::mainnet_chain_hash()
}
