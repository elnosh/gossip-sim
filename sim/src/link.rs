//! One direction of a connection: fixed latency plus a serialization rate.

/// Simulation time in microseconds since the start of the run.
pub type SimTime = u64;

#[derive(Debug, Clone)]
pub struct Link {
	pub latency_us: u64,
	pub bytes_per_sec: f64,
	next_free: f64,
}

impl Link {
	pub fn new(latency_us: u64, bytes_per_sec: f64) -> Link {
		Link { latency_us, bytes_per_sec, next_free: 0.0 }
	}

	/// Queue `len` bytes at `now`. Returns (time the last byte left the sender, delivery time).
	pub fn send(&mut self, now: SimTime, len: usize) -> (SimTime, SimTime) {
		let start = self.next_free.max(now as f64);
		let finish = start + len as f64 * 1e6 / self.bytes_per_sec;
		self.next_free = finish;
		let finish_us = finish.ceil() as u64;
		(finish_us, finish_us + self.latency_us)
	}

	/// How long bytes queued now would wait before starting to transmit.
	pub fn backlog_us(&self, now: SimTime) -> u64 {
		(self.next_free - now as f64).max(0.0) as u64
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn serializes_back_to_back() {
		let mut l = Link::new(1000, 1_000_000.0); // 1 MB/s => 1 byte per us
		assert_eq!(l.send(0, 100), (100, 1100));
		assert_eq!(l.send(0, 100), (200, 1200));
		assert_eq!(l.backlog_us(50), 150);
		assert_eq!(l.send(1000, 10), (1010, 2010));
	}
}
