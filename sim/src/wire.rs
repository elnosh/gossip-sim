//! Encoding helpers for BOLT 1 messages (LDK's `wire` module is crate-private).

use lightning::ln::msgs::DecodeError;
use lightning::util::ser::{LengthReadable, Writeable};
use std::sync::Arc;

pub type Bytes = Arc<[u8]>;

/// Noise overhead per message: 2-byte length + 16-byte MAC, plus the 16-byte body MAC.
pub const NOISE_OVERHEAD: usize = 2 + 16 + 16;

pub fn payload<T: Writeable>(msg: &T) -> Bytes {
	msg.encode().into()
}

/// `[type u16 BE][payload]`
pub fn plaintext(ty: u16, payload: &[u8]) -> Vec<u8> {
	let mut v = Vec::with_capacity(2 + payload.len());
	v.extend_from_slice(&ty.to_be_bytes());
	v.extend_from_slice(payload);
	v
}

pub fn decode<T: LengthReadable>(payload: &[u8]) -> Result<T, DecodeError> {
	T::read_from_fixed_length_buffer(&mut &payload[..])
}

/// Coarse message families used in summaries.
pub fn family(ty: u16) -> &'static str {
	use corpus::msg_type::*;
	match ty {
		CHANNEL_ANNOUNCEMENT => "chan_ann",
		CHANNEL_UPDATE => "chan_upd",
		NODE_ANNOUNCEMENT => "node_ann",
		QUERY_CHANNEL_RANGE | QUERY_SHORT_CHANNEL_IDS | GOSSIP_TIMESTAMP_FILTER => "query",
		REPLY_CHANNEL_RANGE | REPLY_SHORT_CHANNEL_IDS_END => "reply",
		PING | PONG => "ping",
		HANDSHAKE => "handshake",
		_ => "other",
	}
}

/// Pseudo message type used to account Noise handshake bytes.
pub const HANDSHAKE: u16 = u16::MAX;

pub fn is_gossip(ty: u16) -> bool {
	(256..=265).contains(&ty)
}
