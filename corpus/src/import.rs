//! Build a signed [`Corpus`] from a graph file.

use bitcoin::constants::ChainHash;
use bitcoin::hashes::sha256d::Hash as Sha256dHash;
use bitcoin::hashes::Hash;
use bitcoin::secp256k1::ecdsa::Signature;
use bitcoin::secp256k1::{All, Message, PublicKey, Secp256k1, SecretKey};
use bitcoin::Network;
use lightning::ln::msgs::{self, SocketAddress};
use lightning::routing::gossip::{NodeAlias, NodeId};
use lightning::types::features::{ChannelFeatures, NodeFeatures};
use lightning::util::ser::Writeable;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use crate::checksum::channel_update_checksum;
use crate::corpus::{ChanEntry, Corpus, Meta, NodeEntry, UpdEntry};
use crate::graph::{Graph, Policy};
use crate::{keys, Result};

#[derive(Debug, Default, Clone)]
pub struct ImportStats {
	pub channels: usize,
	pub channels_skipped: usize,
	pub updates: usize,
	pub updates_skipped_zero_timestamp: usize,
	pub nodes: usize,
	pub nodes_without_announcement: usize,
	pub nodes_skipped_no_channels: usize,
	pub addresses_skipped: usize,
	pub feature_bits_skipped: usize,
	pub sign_secs: f64,
}

impl fmt::Display for ImportStats {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		writeln!(f, "channels: {} (skipped {})", self.channels, self.channels_skipped)?;
		writeln!(
			f,
			"updates: {} (skipped {} with zero timestamp)",
			self.updates, self.updates_skipped_zero_timestamp
		)?;
		writeln!(
			f,
			"node announcements: {} (nodes without announcement {}, skipped {} without channels)",
			self.nodes, self.nodes_without_announcement, self.nodes_skipped_no_channels
		)?;
		writeln!(
			f,
			"addresses skipped: {}, feature bits skipped: {}",
			self.addresses_skipped, self.feature_bits_skipped
		)?;
		write!(f, "signing took {:.1}s", self.sign_secs)
	}
}

pub fn mainnet_chain_hash() -> ChainHash {
	ChainHash::using_genesis_block(Network::Bitcoin)
}

/// Parse the JSON at `path` and build the corpus.
pub fn import_graph(path: &Path) -> Result<(Corpus, ImportStats)> {
	let graph = Graph::from_json_file(path)?;
	build(&graph)
}

/// Load the cached corpus next to `json_path` (`<json_path>.corpus`) if present and newer than the
/// JSON, otherwise import and write the cache.
pub fn load_or_import(json_path: &Path) -> Result<Corpus> {
	let cache = cache_path(json_path);
	if let (Ok(cm), Ok(jm)) = (std::fs::metadata(&cache), std::fs::metadata(json_path)) {
		if cm.modified()? >= jm.modified()? {
			return Ok(Corpus::load(&cache)?);
		}
	}
	let (corpus, _stats) = import_graph(json_path)?;
	corpus.save(&cache)?;
	Ok(corpus)
}

pub fn cache_path(json_path: &Path) -> std::path::PathBuf {
	let mut s = json_path.as_os_str().to_owned();
	s.push(".corpus");
	s.into()
}

fn sign(secp: &Secp256k1<All>, unsigned: &impl Writeable, sk: &SecretKey) -> Signature {
	let h = Sha256dHash::hash(&unsigned.encode());
	secp.sign_ecdsa(&Message::from_digest(h.to_byte_array()), sk)
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
	if s.len() % 2 != 0 {
		return None;
	}
	(0..s.len() / 2).map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()).collect()
}

struct SynthNode {
	sk: SecretKey,
	id: NodeId,
}

fn synth_node(secp: &Secp256k1<All>, real_hex: &str) -> Option<SynthNode> {
	let real = hex_decode(real_hex)?;
	if real.len() != 33 {
		return None;
	}
	let sk = keys::node_secret(&real);
	let id = NodeId::from_pubkey(&PublicKey::from_secret_key(secp, &sk));
	Some(SynthNode { sk, id })
}

fn address_rank(a: &SocketAddress) -> u8 {
	match a {
		SocketAddress::TcpIpV4 { .. } => 1,
		SocketAddress::TcpIpV6 { .. } => 2,
		SocketAddress::OnionV2(_) => 3,
		SocketAddress::OnionV3 { .. } => 4,
		SocketAddress::Hostname { .. } => 5,
	}
}

fn build_update(
	secp: &Secp256k1<All>, chain_hash: ChainHash, scid: u64, capacity_sat: u64, direction: u8,
	policy: &Policy, sk: &SecretKey,
) -> UpdEntry {
	let htlc_maximum_msat =
		if policy.max_htlc_msat > 0 { policy.max_htlc_msat } else { capacity_sat * 1000 };
	let contents = msgs::UnsignedChannelUpdate {
		chain_hash,
		short_channel_id: scid,
		timestamp: policy.last_update,
		message_flags: 1, // htlc_maximum_msat present (mandatory)
		channel_flags: direction | if policy.disabled { 2 } else { 0 },
		cltv_expiry_delta: policy.time_lock_delta.min(u16::MAX as u32) as u16,
		htlc_minimum_msat: policy.min_htlc,
		htlc_maximum_msat,
		fee_base_msat: policy.fee_base_msat.min(u32::MAX as u64) as u32,
		fee_proportional_millionths: policy.fee_rate_milli_msat.min(u32::MAX as u64) as u32,
		excess_data: Vec::new(),
	};
	let signature = sign(secp, &contents, sk);
	let bytes = msgs::ChannelUpdate { signature, contents }.encode();
	let checksum = channel_update_checksum(&bytes);
	UpdEntry { bytes: bytes.into(), timestamp: policy.last_update, checksum }
}

/// Build the corpus from a parsed dump.
pub fn build(graph: &Graph) -> Result<(Corpus, ImportStats)> {
	let started = std::time::Instant::now();
	let secp = Secp256k1::new();
	let chain_hash = mainnet_chain_hash();
	let mut stats = ImportStats::default();
	let mut synth: HashMap<&str, SynthNode> = HashMap::new();
	let mut chans: BTreeMap<u64, Arc<ChanEntry>> = BTreeMap::new();
	let mut nodes_with_chans: HashSet<NodeId> = HashSet::new();
	let mut dump_time = 0u32;
	let mut max_height = 0u32;

	for e in &graph.edges {
		for k in [e.node1_pub.as_str(), e.node2_pub.as_str()] {
			if !synth.contains_key(k) {
				match synth_node(&secp, k) {
					Some(n) => {
						synth.insert(k, n);
					},
					None => {},
				}
			}
		}
		let (Some(n1), Some(n2)) = (synth.get(e.node1_pub.as_str()), synth.get(e.node2_pub.as_str()))
		else {
			stats.channels_skipped += 1;
			continue;
		};
		if n1.id == n2.id || chans.contains_key(&e.channel_id) {
			stats.channels_skipped += 1;
			continue;
		}
		// BOLT 7 requires node_id_1 < node_id_2; synthetic keys may reorder the real pair, in
		// which case the policies swap sides too.
		let swapped = n2.id < n1.id;
		let (a, a_real, a_pol, b, b_real, b_pol) = if swapped {
			(n2, &e.node2_pub, &e.node2_policy, n1, &e.node1_pub, &e.node1_policy)
		} else {
			(n1, &e.node1_pub, &e.node1_policy, n2, &e.node2_pub, &e.node2_policy)
		};
		let scid = e.channel_id;
		let a_real_bytes = hex_decode(a_real).unwrap();
		let b_real_bytes = hex_decode(b_real).unwrap();
		let btc1 = keys::bitcoin_secret(&a_real_bytes, scid);
		let btc2 = keys::bitcoin_secret(&b_real_bytes, scid);
		let contents = msgs::UnsignedChannelAnnouncement {
			features: ChannelFeatures::empty(),
			chain_hash,
			short_channel_id: scid,
			node_id_1: a.id,
			node_id_2: b.id,
			bitcoin_key_1: NodeId::from_pubkey(&PublicKey::from_secret_key(&secp, &btc1)),
			bitcoin_key_2: NodeId::from_pubkey(&PublicKey::from_secret_key(&secp, &btc2)),
			excess_data: Vec::new(),
		};
		let ann = msgs::ChannelAnnouncement {
			node_signature_1: sign(&secp, &contents, &a.sk),
			node_signature_2: sign(&secp, &contents, &b.sk),
			bitcoin_signature_1: sign(&secp, &contents, &btc1),
			bitcoin_signature_2: sign(&secp, &contents, &btc2),
			contents,
		};
		let mut upd = [None, None];
		for (dir, pol, sk) in [(0u8, a_pol, &a.sk), (1u8, b_pol, &b.sk)] {
			if let Some(p) = pol {
				if p.last_update == 0 {
					stats.updates_skipped_zero_timestamp += 1;
					continue;
				}
				let u = build_update(&secp, chain_hash, scid, e.capacity, dir, p, sk);
				dump_time = dump_time.max(u.timestamp);
				upd[dir as usize] = Some(u);
				stats.updates += 1;
			}
		}
		max_height = max_height.max((scid >> 40) as u32);
		nodes_with_chans.insert(a.id);
		nodes_with_chans.insert(b.id);
		chans.insert(
			scid,
			Arc::new(ChanEntry {
				scid,
				node1: a.id,
				node2: b.id,
				capacity_sat: e.capacity,
				ann: ann.encode().into(),
				upd,
			}),
		);
		stats.channels += 1;
	}

	let mut nodes: BTreeMap<NodeId, Arc<NodeEntry>> = BTreeMap::new();
	for n in &graph.nodes {
		if n.last_update == 0 {
			stats.nodes_without_announcement += 1;
			continue;
		}
		let Some(sn) = synth.get(n.pub_key.as_str()).or_else(|| None) else {
			// Node is not an endpoint of any imported channel.
			stats.nodes_skipped_no_channels += 1;
			continue;
		};
		if !nodes_with_chans.contains(&sn.id) {
			stats.nodes_skipped_no_channels += 1;
			continue;
		}
		let mut features = NodeFeatures::empty();
		for (bit, _) in &n.features {
			let Ok(bit) = bit.parse::<usize>() else {
				stats.feature_bits_skipped += 1;
				continue;
			};
			let r = match (bit < 256, bit % 2 == 0) {
				(true, true) => features.set_required_feature_bit(bit),
				(true, false) => features.set_optional_feature_bit(bit),
				(false, true) => features.set_required_custom_bit(bit),
				(false, false) => features.set_optional_custom_bit(bit),
			};
			if r.is_err() {
				stats.feature_bits_skipped += 1;
			}
		}
		let mut addresses: Vec<SocketAddress> = Vec::new();
		for a in &n.addresses {
			match SocketAddress::from_str(&a.addr) {
				Ok(sa) => addresses.push(sa),
				Err(_) => stats.addresses_skipped += 1,
			}
		}
		addresses.sort_by_key(address_rank);
		let mut alias = [0u8; 32];
		let ab = n.alias.as_bytes();
		let l = ab.len().min(32);
		alias[..l].copy_from_slice(&ab[..l]);
		let rgb = n
			.color
			.strip_prefix('#')
			.and_then(hex_decode)
			.filter(|v| v.len() == 3)
			.map(|v| [v[0], v[1], v[2]])
			.unwrap_or([0; 3]);
		let contents = msgs::UnsignedNodeAnnouncement {
			features,
			timestamp: n.last_update,
			node_id: sn.id,
			rgb,
			alias: NodeAlias(alias),
			addresses,
			excess_address_data: Vec::new(),
			excess_data: Vec::new(),
		};
		let signature = sign(&secp, &contents, &sn.sk);
		let bytes = msgs::NodeAnnouncement { signature, contents }.encode();
		nodes.insert(
			sn.id,
			Arc::new(NodeEntry { node_id: sn.id, bytes: bytes.into(), timestamp: n.last_update }),
		);
		stats.nodes += 1;
	}

	stats.sign_secs = started.elapsed().as_secs_f64();
	Ok((Corpus { meta: Meta { dump_time, max_height }, chans, nodes }, stats))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::mock;
	use lightning::routing::gossip::NetworkGraph;
	use lightning::util::logger::{Logger, Record};
	use lightning::util::ser::LengthReadable;

	pub struct NullLogger;
	impl Logger for NullLogger {
		fn log(&self, _r: Record) {}
	}

	#[test]
	fn mock_corpus_is_accepted_by_real_network_graph() {
		let g = mock::graph(7, 60, 200, 1_787_747_191, 964_125);
		let (corpus, stats) = build(&g).unwrap();
		assert_eq!(stats.channels, corpus.chans.len());
		assert_eq!(stats.updates, corpus.update_count());
		assert!(stats.nodes > 0);
		assert!(corpus.meta.dump_time <= 1_787_747_191 && corpus.meta.dump_time > 1_787_747_191 - 14 * 86_400);

		lightning::util::sim_clock::set_unix_now(corpus.meta.dump_time as u64);
		let logger = NullLogger;
		let graph = NetworkGraph::new(Network::Bitcoin, &logger);
		for c in corpus.chans.values() {
			let ann = msgs::ChannelAnnouncement::read_from_fixed_length_buffer(&mut &c.ann[..])
				.expect("decode ann");
			assert_eq!(ann.contents.short_channel_id, c.scid);
			assert!(ann.contents.node_id_1 < ann.contents.node_id_2);
			graph.update_channel_from_announcement_no_lookup(&ann).expect("accept ann");
			for (dir, u) in c.upd.iter().enumerate() {
				if let Some(u) = u {
					let upd = msgs::ChannelUpdate::read_from_fixed_length_buffer(&mut &u.bytes[..])
						.expect("decode upd");
					assert_eq!(upd.contents.channel_flags & 1, dir as u8);
					assert_eq!(upd.contents.timestamp, u.timestamp);
					assert_eq!(channel_update_checksum(&u.bytes), u.checksum);
					graph.update_channel(&upd).expect("accept upd");
				}
			}
		}
		for n in corpus.nodes.values() {
			let na = msgs::NodeAnnouncement::read_from_fixed_length_buffer(&mut &n.bytes[..])
				.expect("decode node ann");
			graph.update_node_from_announcement(&na).expect("accept node ann");
		}
		let ro = graph.read_only();
		assert_eq!(ro.channels().len(), corpus.chans.len());
		let announced = ro.nodes().unordered_iter().filter(|(_, n)| n.announcement_info.is_some()).count();
		assert_eq!(announced, corpus.nodes.len());
	}

	#[test]
	fn corpus_cache_round_trips() {
		let g = mock::graph(3, 20, 40, 1_700_000_000, 800_000);
		let (corpus, _) = build(&g).unwrap();
		let mut buf = Vec::new();
		corpus.write_to(&mut buf).unwrap();
		let back = Corpus::read_from(&mut &buf[..]).unwrap();
		assert_eq!(back.chans.len(), corpus.chans.len());
		assert_eq!(back.nodes.len(), corpus.nodes.len());
		assert_eq!(back.meta.dump_time, corpus.meta.dump_time);
		for (scid, c) in &corpus.chans {
			let b = &back.chans[scid];
			assert_eq!(b.ann, c.ann);
			for d in 0..2 {
				assert_eq!(b.upd[d].as_ref().map(|u| (&u.bytes, u.timestamp, u.checksum)),
					c.upd[d].as_ref().map(|u| (&u.bytes, u.timestamp, u.checksum)));
			}
		}
	}
}
