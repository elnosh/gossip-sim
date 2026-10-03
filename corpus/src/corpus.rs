//! The corpus: every signed gossip message derived from a dump, plus a compact binary cache
//! format so a dump is only signed once.

use bitcoin::hashes::sha256d::Hash as Sha256dHash;
use bitcoin::hashes::Hash;
use bitcoin::secp256k1::{Message, Secp256k1, SecretKey};
use lightning::ln::msgs::{ChannelUpdate, NodeAnnouncement};
use lightning::routing::gossip::NodeId;
use lightning::util::ser::{LengthReadable, Writeable};
use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::sync::Arc;

use crate::SECS_PER_BLOCK;

pub type Bytes = Arc<[u8]>;

/// A signed `channel_update` payload (no 2-byte type prefix) with its timestamp and the BOLT 7
/// checksum.
#[derive(Debug, Clone)]
pub struct UpdEntry {
	pub bytes: Bytes,
	pub timestamp: u32,
	pub checksum: u32,
}

/// A channel: its signed announcement and up to two directional updates.
/// `upd[0]` is node1 -> node2 (channel_flags direction bit 0), `upd[1]` is node2 -> node1.
#[derive(Debug, Clone)]
pub struct ChanEntry {
	pub scid: u64,
	pub node1: NodeId,
	pub node2: NodeId,
	pub capacity_sat: u64,
	pub ann: Bytes,
	pub upd: [Option<UpdEntry>; 2],
}

impl ChanEntry {
	pub fn block_height(&self) -> u32 {
		(self.scid >> 40) as u32
	}
	/// Timestamp of the freshest update, if any.
	pub fn newest_update(&self) -> Option<u32> {
		self.upd.iter().flatten().map(|u| u.timestamp).max()
	}
	pub fn nodes(&self) -> [NodeId; 2] {
		[self.node1, self.node2]
	}
}

#[derive(Debug, Clone)]
pub struct NodeEntry {
	pub node_id: NodeId,
	pub bytes: Bytes,
	pub timestamp: u32,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Meta {
	/// Unix time the dump represents: the newest channel_update timestamp. Node announcements
	/// are not used because some nodes publish bogus far-future timestamps.
	pub dump_time: u32,
	/// Highest SCID block height in the dump.
	pub max_height: u32,
}

#[derive(Debug, Default, Clone)]
pub struct Corpus {
	pub meta: Meta,
	pub chans: BTreeMap<u64, Arc<ChanEntry>>,
	pub nodes: BTreeMap<NodeId, Arc<NodeEntry>>,
	/// Synthetic signing keys of channel endpoints, to re-sign messages with other timestamps.
	pub node_keys: BTreeMap<NodeId, SecretKey>,
}

impl Corpus {
	/// Approximate block height at unix time `t`, assuming [`SECS_PER_BLOCK`] back from the dump.
	pub fn height_at(&self, t: u32) -> u32 {
		let back = self.meta.dump_time.saturating_sub(t) / SECS_PER_BLOCK;
		self.meta.max_height.saturating_sub(back)
	}

	/// Approximate unix time at which block `height` was mined.
	pub fn time_of_height(&self, height: u32) -> u32 {
		let back = self.meta.max_height.saturating_sub(height) * SECS_PER_BLOCK;
		self.meta.dump_time.saturating_sub(back)
	}

	pub fn scid_from_height(height: u32) -> u64 {
		(height as u64) << 40
	}

	pub fn update_count(&self) -> usize {
		self.chans.values().map(|c| c.upd.iter().flatten().count()).sum()
	}

	fn sign(&self, node: &NodeId, unsigned: &impl Writeable) -> Option<bitcoin::secp256k1::ecdsa::Signature> {
		let sk = self.node_keys.get(node)?;
		let h = Sha256dHash::hash(&unsigned.encode());
		Some(Secp256k1::signing_only().sign_ecdsa(&Message::from_digest(h.to_byte_array()), sk))
	}

	/// The channel's update in direction `dir` with a different timestamp, re-signed (same
	/// contents, so the same checksum: what a keepalive refresh looks like).
	pub fn retimestamp_update(&self, chan: &ChanEntry, dir: usize, timestamp: u32) -> Option<ChannelUpdate> {
		let u = chan.upd[dir].as_ref()?;
		let mut msg = ChannelUpdate::read_from_fixed_length_buffer(&mut &u.bytes[..]).ok()?;
		msg.contents.timestamp = timestamp;
		let signer = if dir == 0 { chan.node1 } else { chan.node2 };
		msg.signature = self.sign(&signer, &msg.contents)?;
		Some(msg)
	}

	/// A node's announcement with a different timestamp, re-signed.
	pub fn retimestamp_node(&self, node: &NodeEntry, timestamp: u32) -> Option<NodeAnnouncement> {
		let mut msg = NodeAnnouncement::read_from_fixed_length_buffer(&mut &node.bytes[..]).ok()?;
		msg.contents.timestamp = timestamp;
		msg.signature = self.sign(&node.node_id, &msg.contents)?;
		Some(msg)
	}

	// ---- binary cache -----------------------------------------------------------------------

	const MAGIC: &'static [u8; 4] = b"GSC2";

	pub fn write_to(&self, w: &mut impl Write) -> io::Result<()> {
		let mut w = io::BufWriter::new(w);
		w.write_all(Self::MAGIC)?;
		w.write_all(&self.meta.dump_time.to_le_bytes())?;
		w.write_all(&self.meta.max_height.to_le_bytes())?;
		w.write_all(&(self.chans.len() as u32).to_le_bytes())?;
		for c in self.chans.values() {
			w.write_all(&c.scid.to_le_bytes())?;
			w.write_all(c.node1.as_slice())?;
			w.write_all(c.node2.as_slice())?;
			w.write_all(&c.capacity_sat.to_le_bytes())?;
			write_bytes(&mut w, &c.ann)?;
			for u in &c.upd {
				match u {
					None => w.write_all(&[0])?,
					Some(u) => {
						w.write_all(&[1])?;
						w.write_all(&u.timestamp.to_le_bytes())?;
						w.write_all(&u.checksum.to_le_bytes())?;
						write_bytes(&mut w, &u.bytes)?;
					},
				}
			}
		}
		w.write_all(&(self.node_keys.len() as u32).to_le_bytes())?;
		for (id, sk) in &self.node_keys {
			w.write_all(id.as_slice())?;
			w.write_all(&sk.secret_bytes())?;
		}
		w.write_all(&(self.nodes.len() as u32).to_le_bytes())?;
		for n in self.nodes.values() {
			w.write_all(n.node_id.as_slice())?;
			w.write_all(&n.timestamp.to_le_bytes())?;
			write_bytes(&mut w, &n.bytes)?;
		}
		w.flush()
	}

	pub fn read_from(r: &mut impl Read) -> io::Result<Corpus> {
		let mut r = io::BufReader::new(r);
		let mut magic = [0u8; 4];
		r.read_exact(&mut magic)?;
		if &magic != Self::MAGIC {
			return Err(io::Error::new(io::ErrorKind::InvalidData, "bad corpus magic"));
		}
		let meta = Meta { dump_time: read_u32(&mut r)?, max_height: read_u32(&mut r)? };
		let nchans = read_u32(&mut r)? as usize;
		let mut chans = BTreeMap::new();
		for _ in 0..nchans {
			let scid = read_u64(&mut r)?;
			let node1 = read_node_id(&mut r)?;
			let node2 = read_node_id(&mut r)?;
			let capacity_sat = read_u64(&mut r)?;
			let ann = read_bytes(&mut r)?;
			let mut upd = [None, None];
			for slot in upd.iter_mut() {
				let mut flag = [0u8];
				r.read_exact(&mut flag)?;
				if flag[0] == 1 {
					let timestamp = read_u32(&mut r)?;
					let checksum = read_u32(&mut r)?;
					let bytes = read_bytes(&mut r)?;
					*slot = Some(UpdEntry { bytes, timestamp, checksum });
				}
			}
			chans.insert(scid, Arc::new(ChanEntry { scid, node1, node2, capacity_sat, ann, upd }));
		}
		let nkeys = read_u32(&mut r)? as usize;
		let mut node_keys = BTreeMap::new();
		for _ in 0..nkeys {
			let id = read_node_id(&mut r)?;
			let mut b = [0u8; 32];
			r.read_exact(&mut b)?;
			let sk = SecretKey::from_slice(&b).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad key"))?;
			node_keys.insert(id, sk);
		}
		let nnodes = read_u32(&mut r)? as usize;
		let mut nodes = BTreeMap::new();
		for _ in 0..nnodes {
			let node_id = read_node_id(&mut r)?;
			let timestamp = read_u32(&mut r)?;
			let bytes = read_bytes(&mut r)?;
			nodes.insert(node_id, Arc::new(NodeEntry { node_id, bytes, timestamp }));
		}
		Ok(Corpus { meta, chans, nodes, node_keys })
	}

	pub fn save(&self, path: &std::path::Path) -> io::Result<()> {
		let mut f = std::fs::File::create(path)?;
		self.write_to(&mut f)
	}

	pub fn load(path: &std::path::Path) -> io::Result<Corpus> {
		let mut f = std::fs::File::open(path)?;
		Self::read_from(&mut f)
	}
}

fn write_bytes(w: &mut impl Write, b: &[u8]) -> io::Result<()> {
	w.write_all(&(b.len() as u16).to_le_bytes())?;
	w.write_all(b)
}

fn read_bytes(r: &mut impl Read) -> io::Result<Bytes> {
	let mut l = [0u8; 2];
	r.read_exact(&mut l)?;
	let mut v = vec![0u8; u16::from_le_bytes(l) as usize];
	r.read_exact(&mut v)?;
	Ok(v.into())
}

fn read_u32(r: &mut impl Read) -> io::Result<u32> {
	let mut b = [0u8; 4];
	r.read_exact(&mut b)?;
	Ok(u32::from_le_bytes(b))
}

fn read_u64(r: &mut impl Read) -> io::Result<u64> {
	let mut b = [0u8; 8];
	r.read_exact(&mut b)?;
	Ok(u64::from_le_bytes(b))
}

fn read_node_id(r: &mut impl Read) -> io::Result<NodeId> {
	let mut b = [0u8; 33];
	r.read_exact(&mut b)?;
	NodeId::from_slice(&b).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad node id"))
}
