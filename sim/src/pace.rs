//! Outbound pacing applied by a simulated peer to its paced streams.

use serde::Deserialize;

use crate::link::SimTime;

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LimiterCfg {
	None,
	/// Byte token bucket (LND's per-peer gossip rate limiter).
	TokenBucket { bytes_per_sec: f64, burst_bytes: f64 },
	/// Bytes counted per one-second window; once over the limit, wait until the overage is paid
	/// back, but at least one second (CLN's `maybe_throttle_usec`).
	Window { bytes_per_sec: f64 },
	/// `n` messages, then wait one round trip (LDK's backfill waits for a pong every 32 messages).
	PingGated { msgs: u32 },
}

#[derive(Debug, Clone)]
pub enum Limiter {
	None,
	TokenBucket { rate: f64, burst: f64, tokens: f64, last: SimTime },
	Window { limit: f64, start: SimTime, used: f64, until: SimTime },
	PingGated { msgs: u32, rtt_us: u64, count: u32, next: SimTime },
}

impl Limiter {
	pub fn new(cfg: &LimiterCfg, rtt_us: u64) -> Limiter {
		match cfg {
			LimiterCfg::None => Limiter::None,
			LimiterCfg::TokenBucket { bytes_per_sec, burst_bytes } => Limiter::TokenBucket {
				rate: *bytes_per_sec,
				burst: *burst_bytes,
				tokens: *burst_bytes,
				last: 0,
			},
			LimiterCfg::Window { bytes_per_sec } => {
				Limiter::Window { limit: *bytes_per_sec, start: 0, used: 0.0, until: 0 }
			},
			LimiterCfg::PingGated { msgs } => {
				Limiter::PingGated { msgs: *msgs, rtt_us, count: 0, next: 0 }
			},
		}
	}

	fn refill(&mut self, now: SimTime) {
		match self {
			Limiter::TokenBucket { rate, burst, tokens, last } => {
				if now > *last {
					*tokens = (*tokens + *rate * (now - *last) as f64 / 1e6).min(*burst);
					*last = now;
				}
			},
			Limiter::Window { start, used, .. } => {
				if now >= *start + 1_000_000 {
					*start = now;
					*used = 0.0;
				}
			},
			_ => {},
		}
	}

	/// Earliest time a message of `size` bytes may be sent.
	pub fn ready_at(&mut self, now: SimTime, size: usize) -> SimTime {
		self.refill(now);
		match self {
			Limiter::None => now,
			Limiter::TokenBucket { rate, burst, tokens, .. } => {
				let need = (size as f64).min(*burst);
				if *tokens >= need {
					now
				} else {
					now + ((need - *tokens) / *rate * 1e6).ceil() as u64
				}
			},
			Limiter::Window { limit, start, used, until } => {
				if now < *until {
					*until
				} else if *used <= *limit {
					now
				} else {
					let need = (*used * 1e6 / *limit) as u64;
					*until = now + need.saturating_sub(now - *start).max(1_000_000);
					*until
				}
			},
			Limiter::PingGated { next, .. } => now.max(*next),
		}
	}

	pub fn consume(&mut self, now: SimTime, size: usize) {
		self.refill(now);
		match self {
			Limiter::None => {},
			Limiter::TokenBucket { tokens, .. } => *tokens -= size as f64,
			Limiter::Window { used, .. } => *used += size as f64,
			Limiter::PingGated { msgs, rtt_us, count, next } => {
				*count += 1;
				if *count >= *msgs {
					*count = 0;
					*next = now + *rtt_us;
				}
			},
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn token_bucket_rate() {
		let mut l = Limiter::new(&LimiterCfg::TokenBucket { bytes_per_sec: 1000.0, burst_bytes: 2000.0 }, 0);
		let mut t = 0;
		let mut sent = 0;
		// Send 1000-byte messages as fast as allowed for 10 s: burst + 10 s * rate.
		loop {
			let r = l.ready_at(t, 1000);
			if r > 10_000_000 {
				break;
			}
			t = r;
			l.consume(t, 1000);
			sent += 1000;
		}
		assert_eq!(sent, 12_000);
	}

	#[test]
	fn window_waits_a_second_after_going_over() {
		let mut l = Limiter::new(&LimiterCfg::Window { bytes_per_sec: 1000.0 }, 0);
		// 600 + 600 bytes go out at once; the window is then over, so wait at least a second.
		assert_eq!(l.ready_at(0, 600), 0);
		l.consume(0, 600);
		assert_eq!(l.ready_at(0, 600), 0);
		l.consume(0, 600);
		assert_eq!(l.ready_at(900_000, 600), 1_900_000);
		assert_eq!(l.ready_at(1_900_000, 600), 1_900_000);
		// A large overage (3x the limit) waits until it is paid back.
		let mut l = Limiter::new(&LimiterCfg::Window { bytes_per_sec: 1000.0 }, 0);
		l.consume(0, 3000);
		assert_eq!(l.ready_at(500_000, 10), 3_000_000);
	}

	#[test]
	fn ping_gated() {
		let mut l = Limiter::new(&LimiterCfg::PingGated { msgs: 2 }, 500);
		assert_eq!(l.ready_at(0, 10), 0);
		l.consume(0, 10);
		assert_eq!(l.ready_at(0, 10), 0);
		l.consume(0, 10);
		assert_eq!(l.ready_at(0, 10), 500);
	}
}
