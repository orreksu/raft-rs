// state.rs

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::VecDeque;

use crate::message::Message;

/// ID of the node is a String
pub type ID = String;
pub type LogIndex = usize;
pub type TermIndex = u32;

/// Represents one entry in the storage
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogEntry {
    pub index: LogIndex,

    pub key: String,
    pub value: String,

    pub mid: String,
    pub client_id: ID,

    /// term is the epoch of a leader, increases monotocally when leader changes
    pub term: TermIndex,
}

#[derive(Clone, Debug)]
/// Represents log of entries requested by clients
pub struct Log {
    entries: Vec<LogEntry>,
}

impl Log {
    pub fn new() -> Log {
        Log {
            entries: Vec::new(),
        }
    }

    pub fn add(&mut self, key: String, value: String, mid: String, client_id: ID, term: TermIndex) {
        let index = self.last_index() + 1;
        self.entries.push(LogEntry {
            index,
            key,
            value,
            mid,
            client_id,
            term,
        })
    }

    pub fn add_entry(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }

    pub fn add_entries(&mut self, entries: Vec<LogEntry>) {
        for entry in entries {
            self.entries.push(entry);
        }
    }

    pub fn get(&self, index: LogIndex) -> Option<LogEntry> {
        for entry in &self.entries {
            if entry.index == index {
                return Some(entry.clone());
            }
        }
        None
    }

    pub fn get_starting_at(&self, index: LogIndex) -> Option<Vec<LogEntry>> {
        let mut entries: Vec<LogEntry> = Vec::new();

        for entry in &self.entries {
            if entry.index >= index {
                entries.push(entry.clone());
            }
        }

        if entries.is_empty() {
            None
        } else {
            Some(entries)
        }
    }

    pub fn get_client_info(&self, index: LogIndex) -> Option<(ID, String)> {
        for entry in &self.entries {
            if entry.index == index {
                return Some((entry.client_id.clone(), entry.mid.clone()));
            }
        }
        None
    }

    pub fn get_term(&self, index: LogIndex) -> Option<TermIndex> {
        match index {
            0 => Some(0),
            idx => match self.get(idx) {
                Some(entry) => Some(entry.term),
                None => None,
            },
        }
    }

    pub fn remove_all_starting_at(&mut self, index: LogIndex) {
        let mut new_entries = Vec::new();

        for entry in &self.entries {
            if entry.index == index {
                break;
            }
            new_entries.push(entry.clone());
        }

        self.entries = new_entries;
    }

    pub fn last_index(&self) -> LogIndex {
        match self.entries.last() {
            Some(entry) => entry.index,
            None => 0,
        }
    }

    pub fn last_term(&self) -> TermIndex {
        match self.entries.last() {
            Some(entry) => entry.term,
            None => 0,
        }
    }
}

/// State specific to the leader node
#[derive(Clone, Debug)]
pub struct LeaderState {
    /// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
    pub next_index: HashMap<ID, LogIndex>,
    /// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
    pub match_index: HashMap<ID, LogIndex>,
}

/// State of any node in the system
#[derive(Clone, Debug)]
pub struct NodeState {
    // Persistent State on all servers
    pub current_term: TermIndex,
    pub voted_for: Option<ID>,
    pub log: Log,

    // Volatile State on all servers
    /// index of highest log entry known to be committed (initialized to 0, increases monotonically)
    pub commit_index: LogIndex,
    /// index of highest log entry applied to kvstore (initialized to 0, increases monotonically)
    pub last_applied: LogIndex,

    // Other State on all servers
    pub my_id: ID,
    pub leader_id: ID,
    pub peer_ids: Vec<ID>,

    pub message_queue: VecDeque<Message>,
    /// values to serve to clients
    pub kvstore: HashMap<String, String>,

    // Volatile State on Leaders (Reinitialized after election)
    /// `None` if this node is not a leader
    pub leader_state: Option<LeaderState>,
}

impl NodeState {
    /// Initialize new node state with given ids, leader is "FFFF"
    pub fn init(my_id: ID, peer_ids: Vec<ID>) -> NodeState {
        NodeState {
            current_term: 0,
            voted_for: None,
            log: Log::new(),

            commit_index: 0,
            last_applied: 0,

            my_id,
            leader_id: "FFFF".into(),
            peer_ids,

            message_queue: VecDeque::new(),
            kvstore: HashMap::new(),

            leader_state: None,
        }
    }

    /// Initialize leader state.
    pub fn init_leader_state(&mut self) {
        self.leader_state = Some(LeaderState {
            next_index: self
                .peer_ids
                .iter()
                .map(|peer_id| (peer_id.clone(), self.log.last_index() + 1))
                .collect(),
            match_index: self
                .peer_ids
                .iter()
                .map(|peer_id| (peer_id.clone(), 0))
                .collect(),
        })
    }

    /// Get copy of my oid.
    pub fn my_id(&self) -> ID {
        self.my_id.clone()
    }

    /// Get copy of the leader id.
    pub fn leader_id(&self) -> ID {
        self.leader_id.clone()
    }

    /// Get copy of the peer ids.
    pub fn peer_ids(&self) -> Vec<ID> {
        self.peer_ids.clone()
    }

    /// Get mutable leader state
    fn leader_state(&mut self) -> Option<&mut LeaderState> {
        self.leader_state.as_mut()
    }

    /// Get mutable match_index
    fn match_index(&mut self) -> Option<&mut HashMap<String, usize>> {
        match self.leader_state() {
            Some(leader_state) => Some(&mut leader_state.match_index),
            None => None,
        }
    }

    /// Get the match index of given peer_id.
    pub fn match_index_of(&mut self, peer_id: &ID) -> LogIndex {
        *self.match_index().unwrap().get(peer_id).unwrap()
    }

    /// Increment match_index by 1.
    pub fn inc_match_index_of(&mut self, peer_id: &ID) {
        *self.match_index().unwrap().get_mut(peer_id).unwrap() += 1
    }

    /// Increment match_index by `count`.
    pub fn inc_match_index_by(&mut self, count: usize, peer_id: &ID) {
        *self.match_index().unwrap().get_mut(peer_id).unwrap() += count
    }

    pub fn set_match_index_of(&mut self, peer_id: &ID, idx: usize) {
        *self.match_index().unwrap().get_mut(peer_id).unwrap() = idx;
    }

    /// Get mutable next_index.
    fn next_index(&mut self) -> Option<&mut HashMap<String, usize>> {
        match self.leader_state() {
            Some(leader_state) => Some(&mut leader_state.next_index),
            None => None,
        }
    }

    /// Get the match index of given peer_id.
    pub fn next_index_of(&mut self, peer_id: &ID) -> LogIndex {
        match self.next_index() {
            Some(next_index) => match next_index.get(peer_id) {
                Some(next_index) => *next_index,
                None => panic!("[LEADER id: {}] No next_ndex for {}", self.my_id, peer_id),
            },
            None => panic!(
                "[LEADER id: {}] next_index_of() called when no next_index exist",
                self.my_id
            ),
        }
    }

    /// Increment next_index by 1.
    pub fn inc_next_index_of(&mut self, peer_id: &ID) {
        *self.next_index().unwrap().get_mut(peer_id).unwrap() += 1
    }

    /// Increment next_index by `count`.
    pub fn inc_next_index_by(&mut self, count: usize, peer_id: &ID) {
        *self.next_index().unwrap().get_mut(peer_id).unwrap() += count
    }

    /// Decrement next_index by 1, but never to less than 1
    pub fn dec_next_index_of(&mut self, peer_id: &ID) {
        if *self.next_index().unwrap().get_mut(peer_id).unwrap() != 1 {
            *self.next_index().unwrap().get_mut(peer_id).unwrap() -= 1
        }
    }

    /// Add key-value to the kvstore.
    pub fn add_key_value_to_store(&mut self, key: String, value: String) -> Option<String> {
        self.kvstore.insert(key, value)
    }

    /// Add last entry from the log to kvstore
    pub fn apply_last_entry(&mut self) {
        match self.log.get(self.commit_index) {
            Some(entry) => {
                self.inc_last_applied();
                self.add_entry_to_store(entry);
            }
            None => {
                panic!("Cannot apply last entry from the log to kvstore");
            }
        }
    }

    /// Add last entry from the log to kvstore
    pub fn apply_entries_starting_at(&mut self, index: LogIndex) {
        for entry in self.log.entries.clone() {
            if entry.index >= index {
                self.inc_last_applied();
                self.add_entry_to_store(entry);
            }
        }
    }

    /// Add log entry to the kvstore.
    pub fn add_entry_to_store(&mut self, entry: LogEntry) -> Option<String> {
        self.kvstore.insert(entry.key, entry.value)
    }

    /// Get value at key from the kvstore.
    pub fn get_from_store(&mut self, key: &String) -> String {
        match self.kvstore.get(key) {
            Some(val) => val.to_string(),
            None => "".to_string(),
        }
    }

    /// Increment commit index by 1.
    pub fn inc_commit_index(&mut self) {
        self.commit_index += 1;
    }

    /// Increment last applied index by 1.
    pub fn inc_last_applied(&mut self) {
        self.last_applied += 1;
    }

    /// Push a message to the end of the queue
    pub fn push_msg_to_queue(&mut self, msg: Message) {
        self.message_queue.push_front(msg);
    }

    pub fn is_queue_empty(&mut self) -> bool {
        self.message_queue.is_empty()
    }

    // Pops a message from queue
    pub fn pop_msg_from_queue(&mut self) -> Option<Message> {
        self.message_queue.pop_back()
    }

    pub fn queue_size(&mut self) -> usize {
        self.message_queue.len()
    }
}
