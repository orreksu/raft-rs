// message.rs

use serde;
use serde::{Deserialize, Serialize};

use super::{LogEntry, LogIndex, NodeState, Result, TermIndex, ID};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Message {
    /// Request vote from peer nodes during candidate state
    RPCRequestVote {
        src: ID,
        dst: ID,
        leader: ID,
        #[serde(rename = "type")]
        typ: String,

        // Raft Information
        /// candidate’s term
        term: TermIndex,
        /// index of candidate’s last log entry (§5.4)
        last_log_index: LogIndex,
        /// term of candidate’s last log entry (§5.4)
        last_log_term: TermIndex,
    },

    /// Response to the vote request from a peer candidate node
    /// `vote` is true if node is voting for the candidate
    RPCRequestVoteReply {
        src: ID,
        dst: ID,
        leader: ID,
        #[serde(rename = "type")]
        typ: String,

        // Raft Information
        /// currentTerm, for candidate to update itself
        term: TermIndex,
        /// true means candidate received vote
        vote_granted: bool,
    },

    /// Request to append entries from leader to other nodes
    /// Also used for heartbeat, when send with empty entries
    RPCAppendEntries {
        src: ID,
        dst: ID,
        leader: ID,
        #[serde(rename = "type")]
        typ: String,

        // Raft Information
        /// leader’s term
        term: TermIndex,
        /// index of log entry immediately preceding new ones
        prev_log_index: LogIndex,
        /// term of prevLogIndex entry
        prev_log_term: TermIndex,
        /// log entries to store (empty for heartbeat)
        entries: Vec<LogEntry>,
        /// leader’s commitIndex
        leader_commit: LogIndex,
    },

    /// Response to a heartbeat from a node to the leader
    /// Response to a append entries from a node to the leader
    RPCAppendEntriesReply {
        src: ID,
        dst: ID,
        leader: ID,
        #[serde(rename = "type")]
        typ: String,

        // Raft Information
        /// currentTerm, for leader to update itself
        term: TermIndex,
        /// true if follower contained entry matching prevLogIndex and prevLogTerm
        success: bool
    },

    /// Request from a client to a node to put new key-value pair
    /// into the storage. `typ` has to be `put`
    ClientPutRequest {
        src: ID,
        dst: ID,
        leader: ID,
        #[serde(rename = "type")]
        typ: String,
        #[serde(rename = "MID")]
        mid: String,
        key: String,
        value: String,
    },

    /// Request from a client to a node to get the value
    /// associated with the given key from the store.
    /// `typ` has to be `get`
    ClientGetRequest {
        src: ID,
        dst: ID,
        leader: ID,
        #[serde(rename = "type")]
        typ: String,
        #[serde(rename = "MID")]
        mid: String,
        key: String,
    },

    /// Answer from a leader to a client with
    /// the value requested using `Message::Key`.
    /// `typ` has to be `ok`
    NodeAnswerWithValue {
        src: ID,
        dst: ID,
        leader: ID,
        #[serde(rename = "type")]
        typ: String,
        #[serde(rename = "MID")]
        mid: String,
        value: String,
    },

    /// Answer from a node to the client.
    /// `typ` can be `redirect` - redirect to the leader,
    /// `fail` - no such entry exist,
    /// `ok` - succesfully added key-value pair
    NodeAnswer {
        src: ID,
        dst: ID,
        leader: ID,
        #[serde(rename = "type")]
        typ: String,
        #[serde(rename = "MID")]
        mid: String,
    },
}

impl Message {
    /// Transform to a vector of bytes
    pub fn to_vec(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self)?)
    }

    /// Create new RequestVote message
    pub fn new_vote_request(state: &NodeState, peer: ID) -> Message {
        Message::RPCRequestVote {
            src: state.my_id(),
            dst: peer,
            leader: state.leader_id(),
            last_log_index: state.log.last_index(),
            last_log_term: state.log.last_term(),
            typ: "vote".into(),
            term: state.current_term,
        }
    }

    /// Create new Vote message
    pub fn new_vote_reply(dst: String, vote_granted: bool, state: &NodeState) -> Message {
        Message::RPCRequestVoteReply {
            src: state.my_id(),
            dst,
            leader: state.leader_id(),
            typ: "vote".into(),

            // Raft Information
            term: state.current_term,
            vote_granted,
        }
    }

    /// Create new NodeAnswer message with typ=="ok"
    pub fn new_ok_answer(dst: ID, mid: String, state: &NodeState) -> Message {
        Message::NodeAnswer {
            src: state.my_id(),
            dst,
            leader: state.leader_id(),
            typ: "ok".into(),
            mid,
        }
    }

    /// Create new NodeAnswer message with typ=="fail"
    pub fn new_fail_answer(dst: ID, mid: String, state: &NodeState) -> Message {
        Message::NodeAnswer {
            src: state.my_id(),
            dst,
            leader: state.leader_id(),
            typ: "fail".into(),
            mid,
        }
    }

    /// Create new NodeAnswer message with typ=="redirect"
    pub fn new_redirect_answer(dst: ID, mid: String, state: &NodeState) -> Message {
        Message::NodeAnswer {
            src: state.my_id(),
            dst,
            leader: state.leader_id(),
            typ: "redirect".into(),
            mid,
        }
    }

    /// Create new NodeAnswerWithValue message
    pub fn new_val_answer(dst: ID, value: String, mid: String, state: &NodeState) -> Message {
        Message::NodeAnswerWithValue {
            src: state.my_id(),
            dst,
            leader: state.leader_id(),
            typ: "ok".into(),
            mid,
            value,
        }
    }

    /// Create new RPCAppendEntries from one entry
    pub fn new_append_entry(
        dst: ID,
        prev_log_index: LogIndex,
        prev_log_term: TermIndex,
        entry: LogEntry,
        state: &NodeState,
    ) -> Message {
        Message::RPCAppendEntries {
            src: state.my_id(),
            dst,
            leader: state.leader_id(),
            typ: "append".into(),

            term: state.current_term,
            prev_log_term,
            prev_log_index,
            entries: vec![entry],
            leader_commit: state.commit_index,
        }
    }

    /// Create new RPCAppendEntries from multiple enties
    pub fn new_append_entries(
        dst: ID,
        prev_log_index: LogIndex,
        prev_log_term: TermIndex,
        entries: Vec<LogEntry>,
        state: &NodeState,
    ) -> Message {
        Message::RPCAppendEntries {
            src: state.my_id(),
            dst,
            leader: state.leader_id(),
            typ: "append".into(),

            term: state.current_term,
            prev_log_term,
            prev_log_index,
            entries,
            leader_commit: state.commit_index,
        }
    }

    /// Create new AppendEntries message with empty `entries`, that is heartbeat
    pub fn new_heartbeat(dst: ID, state: &NodeState) -> Message {
        Message::RPCAppendEntries {
            src: state.my_id(),
            dst,
            leader: state.leader_id(),
            typ: "append".into(),

            term: state.current_term,
            prev_log_term: state.log.last_term(),
            prev_log_index: match state.log.last_index() {
                0 => 0,
                idx => idx - 1,
            },
            entries: Vec::new(),
            leader_commit: state.commit_index,
        }
    }

    /// Create new RPCAppendEntriesResponse
    pub fn new_append_reply(dst: ID, success: bool, state: &NodeState) -> Message {
        Message::RPCAppendEntriesReply {
            src: state.my_id(),
            dst,
            leader: state.leader_id(),
            typ: "append".into(),
            term: state.current_term,
            success
        }
    }
}
