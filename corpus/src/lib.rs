//! Signed gossip message corpus built from a graph file, plus per-peer views of it.
//!
//! The graph has no signatures, so every message is re-signed with deterministic synthetic keys
//! derived from the real public keys (see [`keys`]). Topology, timestamps, policies, aliases,
//! addresses, features and therefore message sizes are the real ones.

pub mod checksum;
pub mod corpus;
pub mod graph;
pub mod import;
pub mod keys;
pub mod mock;
pub mod view;

pub use corpus::{ChanEntry, Corpus, Meta, NodeEntry, UpdEntry};
pub use view::{GroundTruth, PeerView, StaleRule, StructuredDrop, ViewChan, ViewFilter};

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T> = std::result::Result<T, Error>;

/// Seconds per block used to map between SCID block heights and unix time.
pub const SECS_PER_BLOCK: u32 = 600;

/// BOLT 1 message type numbers used throughout the simulator.
pub mod msg_type {
	pub const INIT: u16 = 16;
	pub const ERROR: u16 = 17;
	pub const WARNING: u16 = 1;
	pub const PING: u16 = 18;
	pub const PONG: u16 = 19;
	pub const CHANNEL_ANNOUNCEMENT: u16 = 256;
	pub const NODE_ANNOUNCEMENT: u16 = 257;
	pub const CHANNEL_UPDATE: u16 = 258;
	pub const QUERY_SHORT_CHANNEL_IDS: u16 = 261;
	pub const REPLY_SHORT_CHANNEL_IDS_END: u16 = 262;
	pub const QUERY_CHANNEL_RANGE: u16 = 263;
	pub const REPLY_CHANNEL_RANGE: u16 = 264;
	pub const GOSSIP_TIMESTAMP_FILTER: u16 = 265;

	pub fn name(ty: u16) -> &'static str {
		match ty {
			INIT => "init",
			ERROR => "error",
			WARNING => "warning",
			PING => "ping",
			PONG => "pong",
			CHANNEL_ANNOUNCEMENT => "channel_announcement",
			NODE_ANNOUNCEMENT => "node_announcement",
			CHANNEL_UPDATE => "channel_update",
			QUERY_SHORT_CHANNEL_IDS => "query_short_channel_ids",
			REPLY_SHORT_CHANNEL_IDS_END => "reply_short_channel_ids_end",
			QUERY_CHANNEL_RANGE => "query_channel_range",
			REPLY_CHANNEL_RANGE => "reply_channel_range",
			GOSSIP_TIMESTAMP_FILTER => "gossip_timestamp_filter",
			_ => "other",
		}
	}
}
