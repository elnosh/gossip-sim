//! Peer profiles: which implementation a simulated peer imitates and its responder constants.
//!
//! Built-in defaults come from reading each implementation's source (see `profiles/*.toml` for
//! the citations). A profile file may override any field.

use corpus::{StaleRule, StructuredDrop, ViewFilter};
use serde::Deserialize;
use std::path::Path;

use crate::pace::LimiterCfg;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
	Lnd,
	Cln,
	Eclair,
	Ldk,
}

/// The `gossip_timestamp_filter` a peer sends to us after `init`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OwnFilter {
	None,
	/// first_timestamp = 0, range = u32::MAX (ask for everything).
	Zero,
	/// first_timestamp = now - secs, range = u32::MAX.
	NowMinus { secs: u32 },
	Fixed { first: u32, range: u32 },
}

impl OwnFilter {
	pub fn resolve(&self, now_unix: u32) -> Option<(u32, u32)> {
		match self {
			OwnFilter::None => None,
			OwnFilter::Zero => Some((0, u32::MAX)),
			OwnFilter::NowMinus { secs } => Some((now_unix.saturating_sub(*secs), u32::MAX)),
			OwnFilter::Fixed { first, range } => Some((*first, *range)),
		}
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct Profile {
	pub name: String,
	pub kind: Kind,
	pub own_filter: OwnFilter,
	pub view: ViewFilter,
	pub limiter: LimiterCfg,
	/// Max SCIDs per `reply_channel_range` (LND halves it when timestamps are included).
	pub range_chunk_scids: usize,
	/// Byte cap of a `reply_channel_range` encoding (CLN).
	pub range_max_bytes: usize,
	/// CLN: a nonzero, non-max filter replays only store records newer than this.
	pub recent_window_secs: u32,
	/// Eclair: `query_channel_range` messages accepted per second (0 = unlimited).
	pub range_queries_per_sec: f64,
	/// LDK: a filter older than now minus this triggers a full graph replay.
	pub full_sync_threshold_secs: u32,
	pub supports_timestamps: bool,
	pub supports_checksums: bool,
}

const TWO_WEEKS: u32 = 14 * 24 * 3600;

impl Profile {
	pub fn builtin(kind: Kind) -> Profile {
		match kind {
			Kind::Lnd => Profile {
				name: "lnd".into(),
				kind,
				// Passive syncer: uint32(time.Time{}.Unix()), range 0 (discovery/syncer.go).
				own_filter: OwnFilter::Fixed { first: 2_288_912_640, range: 0 },
				view: ViewFilter {
					include_no_update_chans: true,
					stale_rule: StaleRule::BothSides,
					stale_secs: TWO_WEEKS,
					missing_is_stale: true,
					prune_no_update_chans: false,
					prune_min_age_secs: 0,
					drops: vec![],
				},
				limiter: LimiterCfg::TokenBucket { bytes_per_sec: 51_200.0, burst_bytes: 102_400.0 },
				range_chunk_scids: 8000,
				range_max_bytes: 0,
				recent_window_secs: 0,
				range_queries_per_sec: 0.0,
				full_sync_threshold_secs: 0,
				supports_timestamps: true,
				supports_checksums: false,
			},
			Kind::Cln => Profile {
				name: "cln".into(),
				kind,
				own_filter: OwnFilter::NowMinus { secs: 600 },
				view: ViewFilter {
					include_no_update_chans: false,
					stale_rule: StaleRule::EitherSide,
					stale_secs: TWO_WEEKS,
					missing_is_stale: false,
					prune_no_update_chans: false,
					prune_min_age_secs: 0,
					drops: vec![],
				},
				limiter: LimiterCfg::TokenBucket {
					bytes_per_sec: 1_000_000.0,
					burst_bytes: 1_000_000.0,
				},
				range_chunk_scids: 0,
				range_max_bytes: 65_490,
				recent_window_secs: 7200,
				range_queries_per_sec: 0.0,
				full_sync_threshold_secs: 0,
				supports_timestamps: true,
				supports_checksums: true,
			},
			Kind::Eclair => Profile {
				name: "eclair".into(),
				kind,
				own_filter: OwnFilter::NowMinus { secs: 60 },
				view: ViewFilter {
					include_no_update_chans: true,
					stale_rule: StaleRule::EitherSide,
					stale_secs: TWO_WEEKS,
					missing_is_stale: true,
					prune_no_update_chans: true,
					prune_min_age_secs: 2016 * 600,
					drops: vec![],
				},
				limiter: LimiterCfg::None,
				range_chunk_scids: 1500,
				range_max_bytes: 0,
				recent_window_secs: 0,
				range_queries_per_sec: 5.0,
				full_sync_threshold_secs: 0,
				supports_timestamps: true,
				supports_checksums: true,
			},
			Kind::Ldk => Profile {
				name: "ldk".into(),
				kind,
				own_filter: OwnFilter::NowMinus { secs: 3600 },
				view: ViewFilter {
					include_no_update_chans: true,
					stale_rule: StaleRule::EitherSide,
					stale_secs: TWO_WEEKS,
					missing_is_stale: true,
					prune_no_update_chans: true,
					prune_min_age_secs: TWO_WEEKS,
					drops: vec![],
				},
				limiter: LimiterCfg::PingGated { msgs: 32 },
				range_chunk_scids: 8000,
				range_max_bytes: 0,
				recent_window_secs: 0,
				range_queries_per_sec: 0.0,
				full_sync_threshold_secs: 6 * 3600,
				supports_timestamps: false,
				supports_checksums: false,
			},
		}
	}

	/// Load `profiles/<name>.toml` if it exists, otherwise the built-in profile of that name.
	pub fn load(dir: &Path, name: &str) -> crate::Result<Profile> {
		let path = dir.join(format!("{name}.toml"));
		if path.exists() {
			let o: Overrides = toml::from_str(&std::fs::read_to_string(&path)?)?;
			let mut p = Profile::builtin(o.kind);
			p.name = name.to_string();
			o.apply(&mut p);
			return Ok(p);
		}
		let kind: Kind = toml::Value::String(name.to_string())
			.try_into()
			.map_err(|_| format!("unknown profile `{name}` (no {} and no built-in)", path.display()))?;
		Ok(Profile::builtin(kind))
	}
}

/// Profile file contents; every field but `kind` is optional and overrides the built-in value.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Overrides {
	pub kind: Kind,
	pub own_filter: Option<OwnFilter>,
	pub view: Option<ViewOverrides>,
	pub limiter: Option<LimiterCfg>,
	pub range_chunk_scids: Option<usize>,
	pub range_max_bytes: Option<usize>,
	pub recent_window_secs: Option<u32>,
	pub range_queries_per_sec: Option<f64>,
	pub full_sync_threshold_secs: Option<u32>,
	pub supports_timestamps: Option<bool>,
	pub supports_checksums: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewOverrides {
	pub include_no_update_chans: Option<bool>,
	pub stale_rule: Option<StaleRule>,
	pub stale_secs: Option<u32>,
	pub missing_is_stale: Option<bool>,
	pub prune_no_update_chans: Option<bool>,
	pub prune_min_age_secs: Option<u32>,
	pub drops: Option<Vec<StructuredDrop>>,
}

impl Overrides {
	fn apply(self, p: &mut Profile) {
		macro_rules! set {
			($($f:ident),*) => { $( if let Some(v) = self.$f { p.$f = v; } )* };
		}
		set!(
			own_filter,
			limiter,
			range_chunk_scids,
			range_max_bytes,
			recent_window_secs,
			range_queries_per_sec,
			full_sync_threshold_secs,
			supports_timestamps,
			supports_checksums
		);
		if let Some(v) = self.view {
			let f = &mut p.view;
			if let Some(x) = v.include_no_update_chans { f.include_no_update_chans = x; }
			if let Some(x) = v.stale_rule { f.stale_rule = x; }
			if let Some(x) = v.stale_secs { f.stale_secs = x; }
			if let Some(x) = v.missing_is_stale { f.missing_is_stale = x; }
			if let Some(x) = v.prune_no_update_chans { f.prune_no_update_chans = x; }
			if let Some(x) = v.prune_min_age_secs { f.prune_min_age_secs = x; }
			if let Some(x) = v.drops { f.drops = x; }
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn shipped_profiles_match_builtins() {
		let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../profiles");
		for (name, kind) in [("lnd", Kind::Lnd), ("cln", Kind::Cln), ("eclair", Kind::Eclair), ("ldk", Kind::Ldk)] {
			let loaded = Profile::load(&dir, name).unwrap();
			assert_eq!(loaded, Profile::builtin(kind), "profiles/{name}.toml drifted from the built-in");
		}
	}

	#[test]
	fn unknown_profile_name_without_file_errors() {
		assert!(Profile::load(Path::new("/nonexistent"), "bogus").is_err());
		assert_eq!(Profile::load(Path::new("/nonexistent"), "cln").unwrap().kind, Kind::Cln);
	}
}
