//! Outbound pacing applied by a simulated peer to its paced streams.

use serde::Deserialize;

use crate::link::SimTime;

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LimiterCfg {
	None,
	/// Byte token bucket (LND's per-peer gossip rate limiter, CLN's per-second stream cap).
	TokenBucket { bytes_per_sec: f64, burst_bytes: f64 },
	/// `n` messages, then wait one round trip (LDK's backfill waits for a pong every 32 messages).
	PingGated { msgs: u32 },
}

#[derive(Debug, Clone)]
pub enum Limiter {
	None,
	TokenBucket { rate: f64, burst: f64, tokens: f64, last: SimTime },
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
			LimiterCfg::PingGated { msgs } => {
				Limiter::PingGated { msgs: *msgs, rtt_us, count: 0, next: 0 }
			},
		}
	}

	fn refill(&mut self, now: SimTime) {
		if let Limiter::TokenBucket { rate, burst, tokens, last } = self {
			if now > *last {
				*tokens = (*tokens + *rate * (now - *last) as f64 / 1e6).min(*burst);
				*last = now;
			}
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
			Limiter::PingGated { next, .. } => now.max(*next),
		}
	}

	pub fn consume(&mut self, now: SimTime, size: usize) {
		self.refill(now);
		match self {
			Limiter::None => {},
			Limiter::TokenBucket { tokens, .. } => *tokens -= size as f64,
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
	fn ping_gated() {
		let mut l = Limiter::new(&LimiterCfg::PingGated { msgs: 2 }, 500);
		assert_eq!(l.ready_at(0, 10), 0);
		l.consume(0, 10);
		assert_eq!(l.ready_at(0, 10), 0);
		l.consume(0, 10);
		assert_eq!(l.ready_at(0, 10), 500);
	}
}
