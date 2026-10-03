use super::*;
use crate::ldk::{Graph, NoUtxo, SimLogger};
use crate::profile::{Kind, Profile};
use crate::wire::decode;
use bitcoin::secp256k1::{PublicKey, Secp256k1, SecretKey};
use bitcoin::Network;
use corpus::{import, mock, Corpus, ViewFilter};
use lightning::ln::msgs::{
	BaseMessageHandler, ChannelAnnouncement, ChannelUpdate, MessageSendEvent, NodeAnnouncement,
	RoutingMessageHandler,
};
use lightning::routing::gossip::P2PGossipSync;
use rand::SeedableRng;
use std::sync::{Arc, OnceLock};

const DUMP: u32 = 1_787_747_191;

fn corpus() -> &'static Corpus {
	static C: OnceLock<Corpus> = OnceLock::new();
	C.get_or_init(|| import::build(&mock::graph(21, 400, 9000, DUMP, 964_125)).unwrap().0)
}

fn view(filter: &ViewFilter) -> PeerView {
	let mut rng = ChaCha8Rng::seed_from_u64(1);
	PeerView::derive(corpus(), DUMP, filter, &mut rng)
}

fn ctx<'a>(view: &'a PeerView, rng: &'a mut ChaCha8Rng, now: SimTime) -> Ctx<'a> {
	Ctx { now, now_unix: DUMP, view, chain_hash: crate::ldk_chain_hash(), rng }
}

fn range_query(first: u32, num: u32, flags: Option<u64>) -> QueryChannelRange {
	QueryChannelRange { chain_hash: crate::ldk_chain_hash(), first_blocknum: first, number_of_blocks: num, query_option_flags: flags }
}

fn replies(out: &Out) -> Vec<(Stream, ReplyChannelRange, usize)> {
	out.items
		.iter()
		.filter(|(_, m)| m.ty == msg_type::REPLY_CHANNEL_RANGE)
		.map(|(s, m)| (*s, decode::<ReplyChannelRange>(&m.bytes).unwrap(), m.bytes.len()))
		.collect()
}

/// Replies are contiguous, start at the query start, end at the query end, only the last is
/// complete, and SCIDs are ascending and within each reply's blocks.
fn check_coverage(q: &QueryChannelRange, rs: &[(Stream, ReplyChannelRange, usize)]) {
	assert!(!rs.is_empty());
	let mut expect_first = q.first_blocknum;
	for (i, (_, r, _)) in rs.iter().enumerate() {
		assert_eq!(r.first_blocknum, expect_first, "reply {i} not contiguous");
		let end = r.first_blocknum as u64 + r.number_of_blocks as u64;
		for s in &r.short_channel_ids {
			let h = height(*s) as u64;
			assert!(h >= r.first_blocknum as u64 && h < end, "scid outside reply {i}");
		}
		assert!(r.short_channel_ids.windows(2).all(|w| w[0] < w[1]));
		assert_eq!(r.sync_complete, i == rs.len() - 1);
		expect_first = end as u32;
	}
	assert_eq!(expect_first, query_end(q));
}

fn blocks_whole(rs: &[(Stream, ReplyChannelRange, usize)]) {
	for w in rs.windows(2) {
		if let (Some(a), Some(b)) = (w[0].1.short_channel_ids.last(), w[1].1.short_channel_ids.first()) {
			assert_ne!(height(*a), height(*b), "block split across replies");
		}
	}
}

#[test]
fn lnd_range_chunks_blocks_and_timestamps() {
	let mut p = Profile::builtin(Kind::Lnd);
	p.range_chunk_scids = 200;
	let v = view(&p.view);
	let mut rng = ChaCha8Rng::seed_from_u64(1);
	let mut lnd = lnd::Lnd::new(p);
	let q = range_query(800_000, 200_000, Some(WANT_TIMESTAMPS | WANT_CHECKSUMS));
	let mut out = Out::default();
	lnd.on_query_channel_range(&mut ctx(&v, &mut rng, 0), &q, &mut out);
	let rs = replies(&out);
	check_coverage(&q, &rs);
	blocks_whole(&rs);
	let total: usize = rs.iter().map(|(_, r, _)| r.short_channel_ids.len()).sum();
	assert_eq!(total, v.chans_in_blocks(800_000, 200_000).count());
	for (s, r, _) in &rs {
		assert_eq!(*s, Stream::Query);
		assert!(r.short_channel_ids.len() <= 100, "timestamps halve the chunk");
		assert_eq!(r.timestamps.as_ref().unwrap().len(), r.short_channel_ids.len());
		assert!(r.checksums.is_none(), "LND does not implement checksums");
	}
}

#[test]
fn cln_range_byte_cap_excludes_no_update_and_checksums() {
	let mut p = Profile::builtin(Kind::Cln);
	p.range_max_bytes = 4000;
	let v = view(&ViewFilter { include_no_update_chans: true, ..p.view.clone() });
	let mut rng = ChaCha8Rng::seed_from_u64(1);
	let mut cln = cln::Cln::new(p);
	let q = range_query(0, u32::MAX, Some(WANT_TIMESTAMPS | WANT_CHECKSUMS));
	let mut out = Out::default();
	cln.on_query_channel_range(&mut ctx(&v, &mut rng, 0), &q, &mut out);
	let rs = replies(&out);
	check_coverage(&q, &rs);
	blocks_whole(&rs);
	let mut total = 0;
	for (s, r, _) in &rs {
		assert_eq!(*s, Stream::Unpaced);
		// 8 bytes of scid + 8 timestamps + 8 checksums per entry must fit the cap.
		assert!(r.short_channel_ids.len() * 24 <= 4000);
		assert_eq!(r.checksums.as_ref().unwrap().len(), r.short_channel_ids.len());
		for s in &r.short_channel_ids {
			assert!(v.chans[s].has_any_update(), "CLN leaves out channels without updates");
		}
		total += r.short_channel_ids.len();
	}
	assert_eq!(total, v.chans.values().filter(|c| c.has_any_update()).count());
	assert!(v.chans.values().any(|c| !c.has_any_update()), "test needs no-update channels");
}

#[test]
fn eclair_drops_range_queries_over_rate() {
	let p = Profile::builtin(Kind::Eclair);
	let v = view(&p.view);
	let mut rng = ChaCha8Rng::seed_from_u64(1);
	let mut ecl = eclair::Eclair::new(p);
	let q = range_query(900_000, 10, None);
	let mut answered = 0;
	for i in 0..7 {
		let mut out = Out::default();
		ecl.on_query_channel_range(&mut ctx(&v, &mut rng, i * 1000), &q, &mut out);
		answered += (!out.items.is_empty()) as usize;
	}
	assert_eq!(answered, 5);
	let mut out = Out::default();
	ecl.on_query_channel_range(&mut ctx(&v, &mut rng, 1_000_001), &q, &mut out);
	assert!(!out.items.is_empty(), "a new second accepts queries again");
}

#[test]
fn eclair_range_chunks_and_no_backlog() {
	let p = Profile::builtin(Kind::Eclair);
	let v = view(&p.view);
	let mut rng = ChaCha8Rng::seed_from_u64(1);
	let mut ecl = eclair::Eclair::new(p);
	let q = range_query(0, u32::MAX, Some(WANT_TIMESTAMPS | WANT_CHECKSUMS));
	let mut out = Out::default();
	ecl.on_query_channel_range(&mut ctx(&v, &mut rng, 0), &q, &mut out);
	let rs = replies(&out);
	check_coverage(&q, &rs);
	blocks_whole(&rs);
	assert!(rs.len() > 1);
	assert!(rs.iter().all(|(_, r, _)| r.short_channel_ids.len() <= 1500 && r.checksums.is_some()));
	let mut out = Out::default();
	let f = GossipTimestampFilter { chain_hash: crate::ldk_chain_hash(), first_timestamp: 0, timestamp_range: u32::MAX };
	ecl.on_gossip_timestamp_filter(&mut ctx(&v, &mut rng, 0), &f, &mut out);
	assert!(out.items.is_empty());
}

struct Null;
impl lightning::util::logger::Logger for Null {
	fn log(&self, _: lightning::util::logger::Record) {}
}

/// The LDK responder model produces exactly what the real `P2PGossipSync` sends for the same
/// graph and query.
#[test]
fn ldk_range_matches_real_p2p_gossip_sync() {
	lightning::util::sim_clock::set_skip_sig_verify(true);
	lightning::util::sim_clock::set_unix_now(DUMP as u64);
	let p = Profile::builtin(Kind::Ldk);
	let v = view(&ViewFilter::default());
	let logger = Arc::new(SimLogger::from_env("test"));
	let graph = Arc::new(Graph::new(Network::Bitcoin, logger.clone()));
	for c in v.chans.values() {
		graph.update_channel_from_announcement_no_lookup(&decode::<ChannelAnnouncement>(&c.entry.ann).unwrap()).unwrap();
	}
	let gossip = P2PGossipSync::new(graph.clone(), None::<Arc<NoUtxo>>, logger);
	let them = PublicKey::from_secret_key(&Secp256k1::new(), &SecretKey::from_slice(&[7; 32]).unwrap());
	for q in [range_query(0, u32::MAX, None), range_query(850_000, 60_000, Some(1)), range_query(964_000, 1, None)] {
		gossip.handle_query_channel_range(them, q.clone()).unwrap();
		let real: Vec<ReplyChannelRange> = gossip
			.get_and_clear_pending_msg_events()
			.into_iter()
			.filter_map(|e| match e {
				MessageSendEvent::SendReplyChannelRange { msg, .. } => Some(msg),
				_ => None,
			})
			.collect();
		let mut rng = ChaCha8Rng::seed_from_u64(1);
		let mut ldk = ldk::Ldk::new(p.clone());
		let mut out = Out::default();
		ldk.on_query_channel_range(&mut ctx(&v, &mut rng, 0), &q, &mut out);
		let ours: Vec<ReplyChannelRange> = replies(&out).into_iter().map(|(_, r, _)| r).collect();
		assert_eq!(ours, real, "query {q:?}");
	}
}

fn scid_query(scids: Vec<u64>, flags: Option<Vec<u64>>) -> QueryShortChannelIds {
	QueryShortChannelIds { chain_hash: crate::ldk_chain_hash(), short_channel_ids: scids, query_flags: flags }
}

/// Two channels with both updates sharing a node.
fn two_chans_sharing_a_node(v: &PeerView) -> (u64, u64) {
	let full: Vec<&ViewChan> = v.chans.values().filter(|c| c.has_upd[0] && c.has_upd[1]).collect();
	for a in &full {
		for b in &full {
			if a.entry.scid < b.entry.scid
				&& (a.entry.nodes().contains(&b.entry.node1) || a.entry.nodes().contains(&b.entry.node2))
				&& a.entry.nodes().iter().chain(b.entry.nodes().iter()).all(|n| v.nodes.contains_key(n))
			{
				return (a.entry.scid, b.entry.scid);
			}
		}
	}
	panic!("no pair found");
}

#[test]
fn lnd_scid_reply_order_and_dedup() {
	let p = Profile::builtin(Kind::Lnd);
	let v = view(&p.view);
	let (a, b) = two_chans_sharing_a_node(&v);
	let mut rng = ChaCha8Rng::seed_from_u64(1);
	let mut lnd = lnd::Lnd::new(p);
	let mut out = Out::default();
	lnd.on_query_short_channel_ids(&mut ctx(&v, &mut rng, 0), &scid_query(vec![a, b], Some(vec![1, 1])), &mut out);
	let tys: Vec<u16> = out.items.iter().map(|(_, m)| m.ty).collect();
	use msg_type::*;
	// ann, upd1, node2, upd2, node1 for the first channel; the second repeats one node at most once.
	assert_eq!(&tys[..5], &[CHANNEL_ANNOUNCEMENT, CHANNEL_UPDATE, NODE_ANNOUNCEMENT, CHANNEL_UPDATE, NODE_ANNOUNCEMENT]);
	assert_eq!(tys.iter().filter(|t| **t == NODE_ANNOUNCEMENT).count(), 3, "flags ignored, shared node deduplicated");
	assert_eq!(*tys.last().unwrap(), REPLY_SHORT_CHANNEL_IDS_END);
	let mut out = Out::default();
	lnd.on_query_short_channel_ids(&mut ctx(&v, &mut rng, 0), &scid_query(vec![], None), &mut out);
	assert!(out.items.is_empty(), "LND does not answer an empty query");
}

#[test]
fn cln_one_scid_query_in_flight_and_flags() {
	let p = Profile::builtin(Kind::Cln);
	let v = view(&p.view);
	let (a, b) = two_chans_sharing_a_node(&v);
	let mut rng = ChaCha8Rng::seed_from_u64(1);
	let mut cln = cln::Cln::new(p);
	let mut out = Out::default();
	cln.on_query_short_channel_ids(&mut ctx(&v, &mut rng, 0), &scid_query(vec![a, b], Some(vec![QF_UPD1, QF_ALL])), &mut out);
	let tys: Vec<u16> = out.items.iter().map(|(_, m)| m.ty).collect();
	use msg_type::*;
	assert_eq!(&tys[..5], &[CHANNEL_UPDATE, CHANNEL_ANNOUNCEMENT, CHANNEL_UPDATE, CHANNEL_UPDATE, NODE_ANNOUNCEMENT]);
	assert_eq!(tys.iter().filter(|t| **t == NODE_ANNOUNCEMENT).count(), 2);
	let mut busy = Out::default();
	cln.on_query_short_channel_ids(&mut ctx(&v, &mut rng, 0), &scid_query(vec![a], None), &mut busy);
	assert_eq!(busy.items.len(), 1);
	assert_eq!(busy.items[0].1.ty, WARNING);
	cln.on_sent(REPLY_SHORT_CHANNEL_IDS_END);
	let mut again = Out::default();
	cln.on_query_short_channel_ids(&mut ctx(&v, &mut rng, 0), &scid_query(vec![a], None), &mut again);
	assert_eq!(again.items.last().unwrap().1.ty, REPLY_SHORT_CHANNEL_IDS_END);
}

fn filter(first: u32, range: u32) -> GossipTimestampFilter {
	GossipTimestampFilter { chain_hash: crate::ldk_chain_hash(), first_timestamp: first, timestamp_range: range }
}

fn backlog_types(out: &Out) -> (usize, usize, usize) {
	let n = |t| out.items.iter().filter(|(s, m)| *s == Stream::Backlog && m.ty == t).count();
	(n(msg_type::CHANNEL_ANNOUNCEMENT), n(msg_type::CHANNEL_UPDATE), n(msg_type::NODE_ANNOUNCEMENT))
}

#[test]
fn lnd_backlog_is_the_full_window_once() {
	let p = Profile::builtin(Kind::Lnd);
	let v = view(&p.view);
	let mut rng = ChaCha8Rng::seed_from_u64(1);
	let hour_ago = DUMP - 3600;
	let mut lnd = lnd::Lnd::new(p.clone());
	let mut out = Out::default();
	lnd.on_gossip_timestamp_filter(&mut ctx(&v, &mut rng, 0), &filter(hour_ago, u32::MAX), &mut out);
	let (anns, _, nodes) = backlog_types(&out);
	let expect = v.chans.values().filter(|c| (0..2).any(|d| c.upd(d).map_or(false, |u| u.timestamp >= hour_ago))).count();
	assert_eq!(anns, expect);
	assert_eq!(nodes, v.nodes.values().filter(|n| n.timestamp >= hour_ago).count());
	let mut again = Out::default();
	lnd.on_gossip_timestamp_filter(&mut ctx(&v, &mut rng, 0), &filter(0, u32::MAX), &mut again);
	assert!(again.items.is_empty(), "one backlog per peer");

	let mut lnd = lnd::Lnd::new(p);
	let mut out = Out::default();
	lnd.on_gossip_timestamp_filter(&mut ctx(&v, &mut rng, 0), &filter(0, u32::MAX), &mut out);
	let (anns, upds, nodes) = backlog_types(&out);
	assert_eq!(anns, v.chans.values().filter(|c| c.has_any_update()).count());
	assert_eq!(upds, v.update_count());
	assert_eq!(nodes, v.nodes.len());
}

#[test]
fn cln_filter_modes() {
	let p = Profile::builtin(Kind::Cln);
	let v = view(&p.view);
	let mut rng = ChaCha8Rng::seed_from_u64(1);
	let run = |f: GossipTimestampFilter, rng: &mut ChaCha8Rng| {
		let mut out = Out::default();
		cln::Cln::new(p.clone()).on_gossip_timestamp_filter(&mut ctx(&v, rng, 0), &f, &mut out);
		backlog_types(&out)
	};
	let full = run(filter(0, u32::MAX), &mut rng);
	assert_eq!(full, (v.chans.len(), v.update_count(), v.nodes.len()));
	assert_eq!(run(filter(u32::MAX, u32::MAX), &mut rng), (0, 0, 0));
	let two_weeks = run(filter(DUMP - 14 * 86_400, u32::MAX), &mut rng);
	let recent = DUMP - 7200;
	let recent_upds = v.chans.values().flat_map(|c| (0..2).filter_map(move |d| c.upd(d))).filter(|u| u.timestamp >= recent).count();
	assert_eq!(two_weeks.1, recent_upds, "any nonzero filter only replays the recent store");
	assert!(two_weeks.1 < full.1 / 4);
}

#[test]
fn ldk_filter_threshold() {
	let p = Profile::builtin(Kind::Ldk);
	let v = view(&p.view);
	let mut rng = ChaCha8Rng::seed_from_u64(1);
	let mut out = Out::default();
	ldk::Ldk::new(p.clone()).on_gossip_timestamp_filter(&mut ctx(&v, &mut rng, 0), &filter(DUMP - 3600, u32::MAX), &mut out);
	assert!(out.items.is_empty());
	let mut out = Out::default();
	ldk::Ldk::new(p).on_gossip_timestamp_filter(&mut ctx(&v, &mut rng, 0), &filter(DUMP - 7 * 3600, u32::MAX), &mut out);
	assert_eq!(backlog_types(&out), (v.chans.len(), v.update_count(), v.nodes.len()));
}

#[test]
fn query_flags_select_messages() {
	let p = Profile::builtin(Kind::Eclair);
	let v = view(&p.view);
	let (a, _) = two_chans_sharing_a_node(&v);
	let mut rng = ChaCha8Rng::seed_from_u64(1);
	let mut ecl = eclair::Eclair::new(p);
	let mut out = Out::default();
	ecl.on_query_short_channel_ids(&mut ctx(&v, &mut rng, 0), &scid_query(vec![a], Some(vec![QF_UPD2 | QF_NODE1])), &mut out);
	let tys: Vec<u16> = out.items.iter().map(|(_, m)| m.ty).collect();
	use msg_type::*;
	assert_eq!(tys, vec![CHANNEL_UPDATE, NODE_ANNOUNCEMENT, REPLY_SHORT_CHANNEL_IDS_END]);
	let upd = decode::<ChannelUpdate>(&out.items[0].1.bytes).unwrap();
	assert_eq!(upd.contents.channel_flags & 1, 1);
	let na = decode::<NodeAnnouncement>(&out.items[1].1.bytes).unwrap();
	assert_eq!(na.contents.node_id, v.chans[&a].entry.node1);
}
