//! CRC32C as used by `reply_channel_range` checksums (BOLT 7): computed over the
//! `channel_update` without its signature and timestamp.

const POLY: u32 = 0x82F6_3B78;

fn table() -> &'static [u32; 256] {
	use std::sync::OnceLock;
	static TABLE: OnceLock<[u32; 256]> = OnceLock::new();
	TABLE.get_or_init(|| {
		let mut t = [0u32; 256];
		for i in 0..256u32 {
			let mut crc = i;
			for _ in 0..8 {
				crc = if crc & 1 != 0 { (crc >> 1) ^ POLY } else { crc >> 1 };
			}
			t[i as usize] = crc;
		}
		t
	})
}

pub fn crc32c(data: &[u8]) -> u32 {
	crc32c_append(0, data)
}

pub fn crc32c_append(crc: u32, data: &[u8]) -> u32 {
	let t = table();
	let mut c = !crc;
	for &b in data {
		c = t[((c ^ b as u32) & 0xff) as usize] ^ (c >> 8);
	}
	!c
}

/// Offsets inside a `channel_update` payload (without the 2-byte message type).
const SIG_LEN: usize = 64;
const CHAIN_HASH_LEN: usize = 32;
const SCID_LEN: usize = 8;
const TIMESTAMP_LEN: usize = 4;

/// Checksum of a `channel_update` payload (no type prefix): CRC32C over everything except the
/// signature and the timestamp. Matches CLN's `crc32_of_update` and Eclair's `getChecksum`.
pub fn channel_update_checksum(payload: &[u8]) -> u32 {
	let ts_start = SIG_LEN + CHAIN_HASH_LEN + SCID_LEN;
	let ts_end = ts_start + TIMESTAMP_LEN;
	assert!(payload.len() >= ts_end + 2, "channel_update payload too short");
	let c = crc32c(&payload[SIG_LEN..ts_start]);
	crc32c_append(c, &payload[ts_end..])
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn known_vector() {
		// Standard CRC32C check value.
		assert_eq!(crc32c(b"123456789"), 0xE306_9283);
		assert_eq!(crc32c(b""), 0);
	}

	#[test]
	fn append_matches_single_pass() {
		let data = b"hello world, this is a crc test";
		let whole = crc32c(data);
		let split = crc32c_append(crc32c(&data[..10]), &data[10..]);
		assert_eq!(whole, split);
	}

	#[test]
	fn checksum_ignores_sig_and_timestamp() {
		let mut a = vec![0u8; 64 + 32 + 8 + 4 + 2 + 2 + 8 + 8 + 4 + 4];
		for (i, b) in a.iter_mut().enumerate() {
			*b = i as u8;
		}
		let mut b = a.clone();
		b[..64].iter_mut().for_each(|x| *x ^= 0xff);
		b[104..108].iter_mut().for_each(|x| *x ^= 0xff);
		assert_eq!(channel_update_checksum(&a), channel_update_checksum(&b));
		let mut c = a.clone();
		c[110] ^= 1;
		assert_ne!(channel_update_checksum(&a), channel_update_checksum(&c));
	}
}
