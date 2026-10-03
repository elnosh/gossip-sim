//! Deterministic synthetic keys derived from real public keys.
//!
//! The same real key always maps to the same synthetic key, so dumps taken at different times
//! produce consistent node identities.

use bitcoin::hashes::{sha256, Hash, HashEngine};
use bitcoin::secp256k1::SecretKey;

fn derive(tag: &[u8], parts: &[&[u8]]) -> SecretKey {
	let mut counter = 0u8;
	loop {
		let mut eng = sha256::Hash::engine();
		eng.input(tag);
		for p in parts {
			eng.input(p);
		}
		eng.input(&[counter]);
		let h = sha256::Hash::from_engine(eng);
		if let Ok(sk) = SecretKey::from_slice(h.as_byte_array()) {
			return sk;
		}
		counter += 1;
	}
}

/// Node secret key for a real node public key (hex-decoded, 33 bytes).
pub fn node_secret(real_pubkey: &[u8]) -> SecretKey {
	derive(b"gossip-sim/node", &[real_pubkey])
}

/// Bitcoin (funding) secret key for a (real node public key, scid) pair.
pub fn bitcoin_secret(real_pubkey: &[u8], scid: u64) -> SecretKey {
	derive(b"gossip-sim/btc", &[real_pubkey, &scid.to_be_bytes()])
}

#[cfg(test)]
mod tests {
	use super::*;
	use bitcoin::secp256k1::{PublicKey, Secp256k1};

	#[test]
	fn deterministic_and_distinct() {
		let secp = Secp256k1::new();
		let a = node_secret(&[2u8; 33]);
		let b = node_secret(&[2u8; 33]);
		let c = node_secret(&[3u8; 33]);
		assert_eq!(a, b);
		assert_ne!(a, c);
		assert_ne!(PublicKey::from_secret_key(&secp, &a), PublicKey::from_secret_key(&secp, &c));
		assert_ne!(bitcoin_secret(&[2u8; 33], 1), bitcoin_secret(&[2u8; 33], 2));
	}
}
