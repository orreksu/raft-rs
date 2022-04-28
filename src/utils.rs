// utils.rs

use rand::Rng;
use std::ops::Range;

use super::{Result, StorageError, ID};

// Sleep current thread for given number of microseconds
pub fn sleep(micros: u64) {
    std::thread::sleep(std::time::Duration::from_micros(micros));
}

/// Return random number in given range
pub fn rand_in(range: Range<i64>) -> i64 {
    rand::thread_rng().gen_range(range)
}

/// Parse arguments and return My ID and IDs of my peer nodes
pub fn parse_args() -> Result<(ID, Vec<ID>)> {
    let my_id: ID = match std::env::args().nth(1) {
        Some(id) => id,
        None => Err(StorageError("My ID is not provided".into()))?,
    };

    let peer_ids = match std::env::args().skip(2).collect::<Vec<ID>>() {
        peer_ids if peer_ids.len() > 0 => peer_ids,
        _ => Err(StorageError("Peer IDs not provided".into()))?,
    };

    Ok((my_id, peer_ids))
}
