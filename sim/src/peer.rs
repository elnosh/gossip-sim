//! A simulated remote node: BOLT 8 transport (LDK's Noise implementation), BOLT 1 framing, ping
//! handling, and a [`ResponderPolicy`] that decides what it sends.

use bitcoin::constants::ChainHash;
use bitcoin::secp256k1::{PublicKey, Secp256k1, SecretKey};
use corpus::{msg_type, PeerView};
use lightning::ln::msgs::{GossipTimestampFilter, Init, Ping, Pong, QueryChannelRange, QueryShortChannelIds};
use lightning::ln::peer_channel_encryptor::{MessageBuf, PeerChannelEncryptor};
use lightning::sign::{NodeSigner, Recipient};
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use crate::ldk::{Keys, SimLogger};
use crate::link::SimTime;
use crate::pace::Limiter;
use crate::profile::Profile;
use crate::responders::{self, Ctx, Out, OutMsg, ResponderPolicy, Stream};
use crate::wire::{self, decode, payload, plaintext, Bytes, HANDSHAKE};

/// Per message type: (messages, wire bytes).
pub type TypeCounters = BTreeMap<u16, (u64, u64)>;

#[derive(Debug, Default, Clone)]
pub struct PeerStats {
	/// Sent by this peer, received by the LDK node.
	pub to_ldk: TypeCounters,
	/// Sent by the LDK node to this peer.
	pub from_ldk: TypeCounters,
	pub warnings_from_ldk: u64,
	pub errors_from_ldk: u64,
	pub decode_errors: u64,
	/// Every gossip_timestamp_filter the LDK node sent, as (first_timestamp, range).
	pub filters_from_ldk: Vec<(u32, u32)>,
}

fn count(c: &mut TypeCounters, ty: u16, wire_len: usize) {
	let e = c.entry(ty).or_insert((0, 0));
	e.0 += 1;
	e.1 += wire_len as u64;
}

enum Handshake {
	AwaitActOne,
	AwaitActThree,
	Done,
}

enum Control {
	Raw(Vec<u8>),
	Msg(u16, Bytes),
}

pub enum Next {
	/// An encrypted frame to put on the link now, with its message type.
	Frame(Vec<u8>, u16),
	/// Nothing may be sent before this time.
	WaitUntil(SimTime),
	Idle,
}

pub struct SimPeer {
	pub label: String,
	pub profile: Profile,
	pub node_id: PublicKey,
	pub view: Arc<PeerView>,
	pub stats: PeerStats,
	/// Sim time the last gossip message (types 256..=265) moved in either direction.
	pub last_gossip: SimTime,
	signer: Keys,
	secp: Secp256k1<bitcoin::secp256k1::All>,
	enc: PeerChannelEncryptor,
	hs: Handshake,
	ephemeral: SecretKey,
	rx: Vec<u8>,
	rx_off: usize,
	body_len: Option<usize>,
	control: VecDeque<Control>,
	streams: [VecDeque<OutMsg>; 3],
	rr: usize,
	limiter: Limiter,
	policy: Box<dyn ResponderPolicy>,
	rng: ChaCha8Rng,
	chain_hash: ChainHash,
	got_init: bool,
	pub failed: Option<String>,
}

impl SimPeer {
	pub fn new(
		label: String, profile: Profile, view: Arc<PeerView>, rtt_us: u64, epoch: u64,
		mut rng: ChaCha8Rng, chain_hash: ChainHash,
	) -> SimPeer {
		let seed: [u8; 32] = rng.gen();
		let signer = Keys::new(&seed, epoch, 0, true, Arc::new(SimLogger::from_env(&label)));
		let node_id = signer.get_node_id(Recipient::Node).unwrap();
		let enc = PeerChannelEncryptor::new_inbound(&signer);
		let ephemeral = SecretKey::from_slice(&rng.gen::<[u8; 32]>()).unwrap();
		let limiter = Limiter::new(&profile.limiter, rtt_us);
		let policy = responders::make(&profile);
		SimPeer {
			label,
			profile,
			node_id,
			view,
			stats: PeerStats::default(),
			last_gossip: 0,
			signer,
			secp: Secp256k1::new(),
			enc,
			hs: Handshake::AwaitActOne,
			ephemeral,
			rx: Vec::new(),
			rx_off: 0,
			body_len: None,
			control: VecDeque::new(),
			streams: [VecDeque::new(), VecDeque::new(), VecDeque::new()],
			rr: 0,
			limiter,
			policy,
			rng,
			chain_hash,
			got_init: false,
			failed: None,
		}
	}

	pub fn has_pending(&self) -> bool {
		!self.control.is_empty() || self.streams.iter().any(|s| !s.is_empty())
	}

	pub fn queued_msgs(&self) -> usize {
		self.streams.iter().map(|s| s.len()).sum()
	}

	fn rx_avail(&self) -> &[u8] {
		&self.rx[self.rx_off..]
	}

	fn rx_consume(&mut self, n: usize) -> Vec<u8> {
		let v = self.rx[self.rx_off..self.rx_off + n].to_vec();
		self.rx_off += n;
		if self.rx_off > 1 << 16 && self.rx_off * 2 > self.rx.len() {
			self.rx.drain(..self.rx_off);
			self.rx_off = 0;
		}
		v
	}

	/// Bytes from the LDK node arrived.
	pub fn on_bytes(&mut self, now: SimTime, now_unix: u32, bytes: &[u8]) {
		if self.failed.is_some() {
			return;
		}
		self.rx.extend_from_slice(bytes);
		loop {
			match self.hs {
				Handshake::AwaitActOne => {
					if self.rx_avail().len() < 50 {
						return;
					}
					count(&mut self.stats.from_ldk, HANDSHAKE, 50);
					let act1 = self.rx_consume(50);
					match self.enc.process_act_one_with_keys(&act1, &self.signer, self.ephemeral, &self.secp) {
						Ok(act2) => {
							self.control.push_back(Control::Raw(act2.to_vec()));
							self.hs = Handshake::AwaitActThree;
						},
						Err(e) => return self.fail(format!("act one: {}", e.err)),
					}
				},
				Handshake::AwaitActThree => {
					if self.rx_avail().len() < 66 {
						return;
					}
					count(&mut self.stats.from_ldk, HANDSHAKE, 66);
					let act3 = self.rx_consume(66);
					if let Err(e) = self.enc.process_act_three(&act3) {
						return self.fail(format!("act three: {}", e.err));
					}
					self.hs = Handshake::Done;
				},
				Handshake::Done => match self.body_len {
					None => {
						if self.rx_avail().len() < 18 {
							return;
						}
						let hdr = self.rx_consume(18);
						match self.enc.decrypt_length_header(&hdr) {
							Ok(l) => self.body_len = Some(l as usize),
							Err(e) => return self.fail(format!("length header: {}", e.err)),
						}
					},
					Some(len) => {
						if self.rx_avail().len() < len + 16 {
							return;
						}
						let mut body = self.rx_consume(len + 16);
						self.body_len = None;
						if let Err(e) = self.enc.decrypt_message(&mut body) {
							return self.fail(format!("decrypt: {}", e.err));
						}
						body.truncate(len);
						if len < 2 {
							return self.fail("message shorter than its type".into());
						}
						let ty = u16::from_be_bytes([body[0], body[1]]);
						count(&mut self.stats.from_ldk, ty, len + wire::NOISE_OVERHEAD);
						self.on_message(now, now_unix, ty, &body[2..]);
					},
				},
			}
		}
	}

	fn fail(&mut self, why: String) {
		self.failed = Some(why);
	}

	fn on_message(&mut self, now: SimTime, now_unix: u32, ty: u16, body: &[u8]) {
		if wire::is_gossip(ty) {
			self.last_gossip = now;
		}
		let mut out = Out::default();
		let view = self.view.clone();
		let mut ctx = Ctx { now, now_unix, view: &view, chain_hash: self.chain_hash, rng: &mut self.rng };
		match ty {
			msg_type::INIT => {
				let Ok(init) = decode::<Init>(body) else {
					self.stats.decode_errors += 1;
					return;
				};
				if self.got_init {
					return;
				}
				self.got_init = true;
				// Mirror the LDK node's features: only gossip_queries matters here, and echoing
				// guarantees neither side requires bits the other lacks.
				let ours = Init { features: init.features, networks: None, remote_network_address: None };
				self.control.push_back(Control::Msg(msg_type::INIT, payload(&ours)));
				if let Some((first, range)) = self.policy.own_filter(now_unix) {
					let f = GossipTimestampFilter {
						chain_hash: self.chain_hash,
						first_timestamp: first,
						timestamp_range: range,
					};
					self.control.push_back(Control::Msg(msg_type::GOSSIP_TIMESTAMP_FILTER, payload(&f)));
				}
			},
			msg_type::PING => {
				if let Ok(ping) = decode::<Ping>(body) {
					if ping.ponglen < 65532 {
						let pong = Pong { byteslen: ping.ponglen };
						self.control.push_back(Control::Msg(msg_type::PONG, payload(&pong)));
					}
				} else {
					self.stats.decode_errors += 1;
				}
			},
			msg_type::WARNING => self.stats.warnings_from_ldk += 1,
			msg_type::ERROR => self.stats.errors_from_ldk += 1,
			msg_type::QUERY_CHANNEL_RANGE => match decode::<QueryChannelRange>(body) {
				Ok(q) => self.policy.on_query_channel_range(&mut ctx, &q, &mut out),
				Err(_) => self.stats.decode_errors += 1,
			},
			msg_type::QUERY_SHORT_CHANNEL_IDS => match decode::<QueryShortChannelIds>(body) {
				Ok(q) => self.policy.on_query_short_channel_ids(&mut ctx, &q, &mut out),
				Err(_) => self.stats.decode_errors += 1,
			},
			msg_type::GOSSIP_TIMESTAMP_FILTER => match decode::<GossipTimestampFilter>(body) {
				Ok(f) => {
					self.stats.filters_from_ldk.push((f.first_timestamp, f.timestamp_range));
					self.policy.on_gossip_timestamp_filter(&mut ctx, &f, &mut out)
				},
				Err(_) => self.stats.decode_errors += 1,
			},
			// Gossip forwarded by the LDK node, replies, pongs: only accounted.
			_ => {},
		}
		for (stream, m) in out.items {
			self.streams[stream as usize].push_back(m);
		}
	}

	fn frame(&mut self, ty: u16, body: &[u8]) -> Vec<u8> {
		let pt = plaintext(ty, body);
		let frame = self.enc.encrypt_buffer(MessageBuf::from_encoded(&pt).expect("message too long"));
		count(&mut self.stats.to_ldk, ty, frame.len());
		frame
	}

	/// The next frame to send, honouring stream priority and the limiter.
	pub fn next(&mut self, now: SimTime) -> Next {
		if let Some(c) = self.control.pop_front() {
			return match c {
				Control::Raw(b) => {
					count(&mut self.stats.to_ldk, HANDSHAKE, b.len());
					Next::Frame(b, HANDSHAKE)
				},
				Control::Msg(ty, body) => Next::Frame(self.frame(ty, &body), ty),
			};
		}
		if let Some(m) = self.streams[Stream::Unpaced as usize].pop_front() {
			return self.emit(now, m);
		}
		let paced = [Stream::Query as usize, Stream::Backlog as usize];
		for k in 0..2 {
			let s = paced[(self.rr + k) % 2];
			let Some(m) = self.streams[s].front() else { continue };
			let size = m.bytes.len() + 2 + wire::NOISE_OVERHEAD;
			let at = self.limiter.ready_at(now, size);
			if at > now {
				return Next::WaitUntil(at);
			}
			self.limiter.consume(now, size);
			let m = self.streams[s].pop_front().unwrap();
			self.rr = (self.rr + k + 1) % 2;
			return self.emit(now, m);
		}
		Next::Idle
	}

	fn emit(&mut self, now: SimTime, m: OutMsg) -> Next {
		self.policy.on_sent(m.ty);
		if wire::is_gossip(m.ty) {
			self.last_gossip = now;
		}
		let f = self.frame(m.ty, &m.bytes);
		Next::Frame(f, m.ty)
	}
}
