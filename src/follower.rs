// follower.rs

use std::cmp::min;

use crate::NodeState;
use crate::StorageError;
use crate::{begin_an_election, rand_in, timeout_reached};
use crate::Instant;
use crate::{Message, Result, Socket};

/// ...
fn process_msg_in_queue(state: &mut NodeState, socket: &Socket) -> Result<()> {
    eprintln!(
        "PROCESSING MESSAGE from QUEUE, size of queue: {}",
        state.queue_size()
    );

    if let Some(msg) = state.pop_msg_from_queue() {
        match msg {
            Message::ClientPutRequest {
                src: client, mid, ..
            }
            | Message::ClientGetRequest {
                src: client, mid, ..
            } => {
                socket.send_msg(Message::new_redirect_answer(client, mid, state))?;
            }

            _ => {
                return Err(StorageError(
                    "Tried to process non-client msg with client function".into(),
                ))
            }
        }
    }

    Ok(())
}

/// Follower listens for RPCs from the leader,
/// if no RPCs arrive during `election_timeout`,
/// it becomes a candidate and starts a new election.
/// Redirects requests from clients to the leader.
pub fn start(state: &mut NodeState, socket: &Socket) -> Result<()> {
    let election_timeout = rand_in(400..800);
    
    // Initialize last time we received RPC to now, used for `rpc_timeout_reached`
    // Only update `last_rpc_time` on message from leader and after granting a vote
    // as per Figure 2, Rules for Servers
    let mut last_time = Instant::now();

    'being_follower: loop {
        while !socket.ready_for_read() {
            if !state.is_queue_empty() && state.leader_id != "FFFF" {
                process_msg_in_queue(state, socket)?;
            }

            // Followers (§5.2):
            // If election timeout elapses without receiving AppendEntries
            // RPC from current leader or granting vote to candidate: convert to candidate
            if timeout_reached(last_time, election_timeout) {
                return begin_an_election(state, socket);
            }
        }

        // socket is ready, read a message from it
        let msg = socket.read_msg()?;
        match msg.clone() {
            msg @ Message::ClientPutRequest { .. } | msg @ Message::ClientGetRequest { .. }
                if state.leader_id == "FFFF" =>
            {
                state.push_msg_to_queue(msg);
                continue 'being_follower;
            }

            Message::ClientPutRequest { src, mid, .. }
            | Message::ClientGetRequest { src, mid, .. } => {
                socket.send_msg(Message::new_redirect_answer(src, mid, state))?;
                continue 'being_follower;
            }

            Message::RPCAppendEntries {
                src: leader_id,
                term: leader_term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                ..
            } => {
                // Receiver implementation:
                // 1. Reply false if term < currentTerm (§5.1)
                if leader_term < state.current_term {
                    eprintln!("{} PEER: Leader is from from past", state.my_id);
                    socket.send_msg(Message::new_append_reply(leader_id, false, state))?;
                    continue 'being_follower;
                }

                // Update leader id and last rpc time
                state.leader_id = leader_id.clone();
                last_time = Instant::now();

                // All Servers:
                // If RPC request or response contains term T > currentTerm:
                // set currentTerm = T, convert to follower (§5.1)
                if leader_term > state.current_term {
                    eprintln!("{} PEER: Leader is from from future", state.my_id);
                    state.current_term = leader_term;
                }

                // Heartbeat
                if entries.is_empty() {
                    eprintln!("{} PEER: Heartbeat from {}", state.my_id, leader_id);
                    continue 'being_follower;
                }

                // Receiver implementation:
                // 2. Reply false if log doesn’t contain an entry at prevLogIndex
                // whose term matches prevLogTerm (§5.3)
                let inconsistent = match state.log.get_term(prev_log_index) {
                    Some(term) => term != prev_log_term,
                    None => true,
                };

                if inconsistent {
                    eprintln!(
                        "{} PEER: Inconsistent State prev_log_index: {}, last_index: {}",
                        state.my_id,
                        prev_log_index,
                        state.log.last_index()
                    );
                    socket.send_msg(Message::new_append_reply(leader_id, false, state))?;
                    continue 'being_follower;
                }

                eprintln!("{} PEER: Consistent state", state.my_id);

                let entry = entries.first().unwrap();

                // Receiver implementation:
                // 3. If an existing entry conflicts with a new one (same index
                // but different terms), delete the existing entry and all that
                // follow it (§5.3)
                if let Some(term) = state.log.get_term(entry.index) {
                    // entry already in the log, ignore
                    if term == entry.term {
                        continue 'being_follower;
                    } else {
                        state.log.remove_all_starting_at(entry.index);
                    }
                }

                socket.send_msg(Message::new_append_reply(
                    leader_id,
                    true,
                    state
                ))?;

                // Receiver implementation:
                // 4. Append any new entries not already in the log
                eprintln!(
                    "{} PEER: Adding entry at index: {}",
                    state.my_id, entry.index
                );
                state.log.add_entry(entry.clone());

                // Receiver implementation:
                // 5. If leaderCommit > commitIndex,
                // set commitIndex = min(leaderCommit, index of last new entry)
                if leader_commit > state.commit_index {
                    state.commit_index = min(leader_commit, state.log.last_index());
                }

                // All Servers:
                // If commitIndex > lastApplied: increment lastApplied,
                // apply log[lastApplied] to state machine (§5.3)
                if state.commit_index > state.last_applied {
                    state.apply_last_entry();
                }

                continue 'being_follower;
            }

            Message::RPCRequestVote {
                src: candidate_id,
                term: candidate_term,
                last_log_index,
                last_log_term,
                ..
            } => {
                eprintln!("{} FLWR: Vote request from {}", state.my_id, candidate_id);

                // Reject if we have a leader
                // if state.leader_id != "FFFF" && state.leader_id != candidate_id && !timeout_reached(last_time, election_timeout) {
                //     eprintln!("{} FLWR: We have a leader", state.my_id);
                //     eprintln!("{} FLWR: Did not grant vote to {}", state.my_id, candidate_id);
                //     socket.send_msg(Message::new_vote_reply(candidate_id, false, state))?;
                //     continue 'being_follower;
                // }

                // All Servers:
                // If RPC request or response contains term T > currentTerm:
                // set currentTerm = T, convert to follower (§5.1)
                if candidate_term > state.current_term {
                    eprintln!("{} FLWR: Candidate Term > My Term", state.my_id);
                    state.current_term = candidate_term;
                    state.leader_id = "FFFF".into();
                    // OLEKS: state.voted_for = None;
                }

                // Receiver implementation:
                // 1. Reply false if term < currentTerm (§5.1)
                if candidate_term < state.current_term {
                    eprintln!("{} FLWR: Candidate Term < My Term", state.my_id);
                    eprintln!("{} FLWR: Did not grant vote to {}", state.my_id, candidate_id);
                    socket.send_msg(Message::new_vote_reply(candidate_id, false, state))?;
                    continue 'being_follower;
                }

                // Receiver implementation:
                // 2. If votedFor is null or candidateId, and candidate’s log is at
                // least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
                let is_valid_vote = match state.voted_for.as_deref() {
                    Some(voted_for) => voted_for == candidate_id,
                    None => true,
                };

                eprintln!("{} FLWR: Is vote valid? {}", state.my_id, is_valid_vote);

                // If the logs have last entries with different terms,
                // then the log with the later term is more up-to-date.
                // If the logs end with the same term,
                // then whichever log is longer is more up-to-date.
                let is_up_to_date = {
                    if last_log_term != state.log.last_term() {
                        last_log_term >= state.log.last_term()
                    } else {
                        last_log_index >= state.log.last_index()
                    }
                };

                eprintln!("{} FLWR: Is log as up-to-date? {}", state.my_id, is_up_to_date);

                let vote_granted = is_valid_vote && is_up_to_date;

                if vote_granted {
                    eprintln!("{} FLWR: Granted vote to {}", state.my_id, candidate_id);
                    state.voted_for = Some(candidate_id.clone());
                    last_time = Instant::now();
                } else {
                    eprintln!("{} FLWR: Did not grant vote to {}", state.my_id, candidate_id);
                }

                socket.send_msg(Message::new_vote_reply(candidate_id, vote_granted, state))?;
                continue 'being_follower;
            }

            Message::RPCRequestVoteReply { .. } => {
                continue 'being_follower;
            }

            Message::RPCAppendEntriesReply { .. } => {
                continue 'being_follower;
            }

            Message::NodeAnswerWithValue { .. } => {
                unreachable!()
            }

            Message::NodeAnswer { .. } => {
                unreachable!()
            }
        }
    }
}
