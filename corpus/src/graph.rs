//! The graph input format: the JSON written by LND's `lncli describegraph`.
//!
//! uint64 fields are accepted both as strings (as LND writes them) and as numbers.

use serde::{Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Graph {
	#[serde(default)]
	pub nodes: Vec<Node>,
	#[serde(default)]
	pub edges: Vec<Edge>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Node {
	#[serde(default)]
	pub last_update: u32,
	pub pub_key: String,
	#[serde(default)]
	pub alias: String,
	#[serde(default)]
	pub addresses: Vec<Address>,
	#[serde(default)]
	pub color: String,
	#[serde(default)]
	pub features: BTreeMap<String, Feature>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Address {
	#[serde(default)]
	pub network: String,
	pub addr: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Feature {
	#[serde(default)]
	pub name: String,
	#[serde(default)]
	pub is_required: bool,
	#[serde(default)]
	pub is_known: bool,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Edge {
	#[serde(deserialize_with = "de_u64", serialize_with = "ser_u64_str")]
	pub channel_id: u64,
	#[serde(default)]
	pub chan_point: String,
	#[serde(default)]
	pub last_update: u32,
	pub node1_pub: String,
	pub node2_pub: String,
	#[serde(default, deserialize_with = "de_u64", serialize_with = "ser_u64_str")]
	pub capacity: u64,
	#[serde(default)]
	pub node1_policy: Option<Policy>,
	#[serde(default)]
	pub node2_policy: Option<Policy>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Policy {
	#[serde(default)]
	pub time_lock_delta: u32,
	#[serde(default, deserialize_with = "de_u64", serialize_with = "ser_u64_str")]
	pub min_htlc: u64,
	#[serde(default, deserialize_with = "de_u64", serialize_with = "ser_u64_str")]
	pub fee_base_msat: u64,
	#[serde(default, deserialize_with = "de_u64", serialize_with = "ser_u64_str")]
	pub fee_rate_milli_msat: u64,
	#[serde(default)]
	pub disabled: bool,
	#[serde(default, deserialize_with = "de_u64", serialize_with = "ser_u64_str")]
	pub max_htlc_msat: u64,
	#[serde(default)]
	pub last_update: u32,
}

fn de_u64<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
	#[derive(Deserialize)]
	#[serde(untagged)]
	enum StrOrNum {
		S(String),
		N(u64),
		I(i64),
	}
	match StrOrNum::deserialize(d)? {
		StrOrNum::N(n) => Ok(n),
		StrOrNum::I(i) => Ok(i.max(0) as u64),
		StrOrNum::S(s) => s.trim().parse::<u64>().map_err(serde::de::Error::custom),
	}
}

fn ser_u64_str<S: serde::Serializer>(v: &u64, s: S) -> Result<S::Ok, S::Error> {
	s.serialize_str(&v.to_string())
}

impl Graph {
	pub fn from_json_file(path: &std::path::Path) -> crate::Result<Graph> {
		let f = std::fs::File::open(path)?;
		let rd = std::io::BufReader::with_capacity(1 << 20, f);
		Ok(serde_json::from_reader(rd)?)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn accepts_strings_and_numbers() {
		let j = r#"{"nodes":[],"edges":[
			{"channel_id":"553951550347608065","node1_pub":"a","node2_pub":"b","capacity":"37200",
			 "node1_policy":{"min_htlc":"1000","fee_base_msat":"1000","fee_rate_milli_msat":"1","max_htlc_msat":"99000","last_update":5,"time_lock_delta":40,"disabled":false},
			 "node2_policy":null},
			{"channel_id":553951550347608066,"node1_pub":"a","node2_pub":"b","capacity":100}
		]}"#;
		let g: Graph = serde_json::from_str(j).unwrap();
		assert_eq!(g.edges.len(), 2);
		assert_eq!(g.edges[0].channel_id, 553951550347608065);
		assert_eq!(g.edges[0].node1_policy.as_ref().unwrap().min_htlc, 1000);
		assert!(g.edges[0].node2_policy.is_none());
		assert_eq!(g.edges[1].capacity, 100);
	}
}
