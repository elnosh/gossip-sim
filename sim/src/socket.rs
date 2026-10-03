//! Fake socket handed to LDK's `PeerManager`.
//!
//! `send_data` only appends to an outbox; the engine moves the outbox onto a [`crate::link::Link`]
//! after every LDK call, so `PeerManager` is never re-entered from inside one of its own calls.

use lightning::ln::peer_handler::SocketDescriptor;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
pub struct DescState {
	/// Bytes LDK handed us since the engine last drained the outbox.
	pub outbox: Vec<u8>,
	/// Latched from `continue_read` on every `send_data` call (including empty ones).
	pub read_paused: bool,
	/// Free space in the simulated kernel send buffer.
	pub sock_free: usize,
	/// LDK got a short write and waits for `write_buffer_space_avail`.
	pub pending_space_avail: bool,
	/// LDK asked us to close the connection.
	pub disconnect_requested: bool,
}

#[derive(Clone)]
pub struct SimDescriptor {
	pub id: usize,
	pub st: Arc<Mutex<DescState>>,
}

impl SimDescriptor {
	pub fn new(id: usize, sock_buf: usize) -> SimDescriptor {
		SimDescriptor {
			id,
			st: Arc::new(Mutex::new(DescState { sock_free: sock_buf, ..Default::default() })),
		}
	}
}

impl PartialEq for SimDescriptor {
	fn eq(&self, o: &Self) -> bool {
		self.id == o.id
	}
}
impl Eq for SimDescriptor {}
impl Hash for SimDescriptor {
	fn hash<H: Hasher>(&self, h: &mut H) {
		self.id.hash(h)
	}
}

impl SocketDescriptor for SimDescriptor {
	fn send_data(&mut self, data: &[u8], continue_read: bool) -> usize {
		let mut s = self.st.lock().unwrap();
		s.read_paused = !continue_read;
		if s.disconnect_requested {
			return 0;
		}
		let n = data.len().min(s.sock_free);
		s.outbox.extend_from_slice(&data[..n]);
		s.sock_free -= n;
		if n < data.len() {
			s.pending_space_avail = true;
		}
		n
	}

	fn disconnect_socket(&mut self) {
		self.st.lock().unwrap().disconnect_requested = true;
	}
}
