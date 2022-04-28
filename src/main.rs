// main.rs

use std::time::{Duration, Instant};

pub mod utils;
use utils::{parse_args, rand_in};

pub mod message;
use message::Message;

pub mod state;
use state::{LogEntry, LogIndex, NodeState, TermIndex, ID};

pub mod socket;
use socket::Socket;

pub mod candidate;
pub mod follower;
pub mod leader;

#[derive(Debug, Clone)]
pub struct StorageError(String);

pub type Result<T> = std::result::Result<T, StorageError>;

impl From<serde_json::Error> for StorageError {
    fn from(err: serde_json::Error) -> StorageError {
        StorageError(format!("serfe json err: {}", err.to_string()))
    }
}
impl From<nix::Error> for StorageError {
    fn from(err: nix::Error) -> StorageError {
        StorageError(format!("socket error, {:?}", err))
    }
}

impl From<std::io::Error> for StorageError {
    fn from(err: std::io::Error) -> StorageError {
        StorageError(format!("io error: {}", err.to_string()))
    }
}

/// Have we reached the timeout?
fn timeout_reached(last_time: Instant, timeout: i64) -> bool {
    last_time + Duration::from_millis(timeout as u64) <= Instant::now()
}

/// Begin an election
fn begin_an_election(state: &mut NodeState, socket: &Socket) -> Result<()> {
    state.current_term += 1;
    eprintln!(
        "\n\n{} Starting election for term {}...",
        state.my_id, state.current_term
    );
    return candidate::start(state, socket);
}

// Run kvstore distributed node
fn main() -> Result<()> {
    let (my_id, peer_ids) = parse_args()?;
    let socket = Socket::new_passive_socket(&my_id)?;
    let mut state = NodeState::init(my_id, peer_ids);

    follower::start(&mut state, &socket)
}
