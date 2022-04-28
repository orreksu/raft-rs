// leader.rs

use std::cmp::min;
use std::collections::HashMap;

use crate::{follower, utils::rand_in};
use crate::Instant;
use crate::{ID, Message, NodeState, Result, Socket};
use crate::timeout_reached;

/// Send peer a message.
/// If we have new entry, send append entry request.
/// Otherwise, send a heartbeat.
fn update_peer(peer_id: &ID, state: &mut NodeState, socket: &Socket) -> Result<Instant> {
    // Leaders:
    // If last log index ≥ nextIndex for a follower:
    // send AppendEntries RPC with log entries starting at nextIndex
    let next_index = state.next_index_of(peer_id);

    let entry = match state.log.get(next_index) {
        Some(entry) => entry,
        None => panic!("LEAD No entry in the log starting at next_index"),
    };

    let prev_log_index = next_index - 1;
    let prev_log_term = match state.log.get_term(prev_log_index) {
        Some(term) => term,
        None => 0,
    };

    eprintln!("{} LEAD: Append entries to {}", state.my_id, peer_id);

    socket.send_msg(Message::new_append_entries(
        peer_id.clone(),
        prev_log_index,
        prev_log_term,
        vec![entry],
        state,
    ))?;

    Ok(Instant::now())
}

/// Send empty AppendEntries RPCs (heartbeat) to given peer
fn send_heartbeat_to(peer_id: &ID, state: &mut NodeState, socket: &Socket) -> Result<Instant> {
    eprintln!("{} LEAD: Heartbeat to {}", state.my_id, peer_id);
    socket.send_msg(Message::new_heartbeat(peer_id.clone(), state))?;
    Ok(Instant::now())
}

/// Send empty AppendEntries RPCs (heartbeat) to each peer.
fn send_heartbeat_to_all_peers(state: &mut NodeState, socket: &Socket) -> Result<Instant> {
    for peer_id in state.peer_ids() {
        eprintln!("{} LEAD: Heartbeat to {}", state.my_id, peer_id);
        socket.send_msg(Message::new_heartbeat(peer_id, state))?;
    }
    Ok(Instant::now())
}

/// ...
fn handle_client_message(msg: Message, state: &mut NodeState, socket: &Socket) -> Result<()> {
    match msg {
        Message::ClientPutRequest {
            src: client_id,
            key,
            value,
            mid,
            ..
        } => {
            eprintln!("{} LEAD: PUT", state.my_id);
            state.log.add(key, value, mid, client_id, state.current_term);
            Ok(())
        }

        Message::ClientGetRequest {
            src: client_id,
            key,
            mid,
            ..
        } => {
            eprintln!("{} LEAD: GET", state.my_id);
            let value: String = state.get_from_store(&key);
            socket.send_msg(Message::new_val_answer(client_id, value, mid, state))?;
            Ok(())
        }

        _ => unreachable!(),
    }
}

/// Respondes to client `get` and `put` requests.
/// Updates follower logs, and send heartbeats to remind abi
pub fn start(state: &mut NodeState, socket: &Socket) -> Result<()> {
    // Initialize cluster size to the number of peers + us
    let cluster_size = state.peer_ids.len() + 1;

    // Leaders:
    // Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server;
    let initial_heartbeat_time = send_heartbeat_to_all_peers(state, socket)?;

    // Hashmap to store last time we send msg to each peer
    let mut last_send_times: HashMap<ID, Instant> = HashMap::new();
    for peer_id in state.peer_ids() {
        last_send_times.insert(peer_id, initial_heartbeat_time);
    }

    // Hashmap to store last time we send update msg to each peer
    let mut last_update_times: HashMap<ID, Instant> = HashMap::new();
    for peer_id in state.peer_ids() {
        last_update_times.insert(peer_id, initial_heartbeat_time);
    }

    let mut last_heard_times: HashMap<ID, Instant> = HashMap::new();
    for peer_id in state.peer_ids() {
        last_heard_times.insert(peer_id, initial_heartbeat_time);
    }

    // let mut update_timeouts: HashMap<ID, i64> = HashMap::new();
    // for peer_id in state.peer_ids() {
    //     update_timeouts.insert(peer_id, 0);
    // }

    // let mut dead_peers: HashMap<ID, bool> = HashMap::new();

    // let last_heard_timeout = 1000;

    // timeout for not sending anything to peer in millis,
    // after this timout we have to reach out to peer
    let send_timeout = 300;

    // timeout for not trying to update peer in millis,
    // after this timeout we have to try update peer
    let update_timeout = 10;

    // Drain the queue and handle each of the messages in it
    while !state.is_queue_empty() {
        if let Some(msg) = state.pop_msg_from_queue() {
            handle_client_message(msg, state, socket)?;
        }
    }

    'being_leader: loop {
        while !socket.ready_for_read() {
            for peer_id in state.peer_ids() {
                // let update_timeout = *update_timeouts.get(&peer_id).unwrap();
                let last_update = *last_update_times.get(&peer_id).unwrap();
                let last_send = *last_send_times.get(&peer_id).unwrap();
                // let last_heard = *last_heard_times.get(&peer_id).unwrap();

                // if timeout_reached(last_heard, last_heard_timeout) {
                //     // *update_timeouts.entry(peer_id.clone()).or_insert(0) += 1;
                //     dead_peers.insert(peer_id.clone(), true);
                // }

                // If last log index ≥ nextIndex for a follower
                // and we did not send update to the peer for the last `update_timeout`,
                // update peer with new entries
                if state.log.last_index() >= state.next_index_of(&peer_id)
                    && timeout_reached(last_update, update_timeout)
                    // && !dead_peers.contains_key(&peer_id)
                {
                    let update_time = update_peer(&peer_id, state, socket)?;
                    last_send_times.insert(peer_id.clone(), update_time);
                    last_update_times.insert(peer_id, update_time);
                }
                // Else if we did not send anything to the peer for the last `send_timeout`
                // send peer a heartbeat
                else if timeout_reached(last_send, send_timeout) {
                    let send_time = send_heartbeat_to(&peer_id, state, socket)?;
                    last_send_times.insert(peer_id, send_time);
                }
            }
        }

        match socket.read_msg()? {
            msg @ Message::ClientPutRequest { .. } | msg @ Message::ClientGetRequest { .. } => {
                handle_client_message(msg, state, socket)?;
            }

            Message::RPCAppendEntriesReply {
                src: peer_id,
                term: peer_term,
                success,
                ..
            } => {
                // dead_peers.remove_entry(&peer_id);
                // last_heard_times.insert(peer_id.clone(), Instant::now());

                // All Servers:
                // If RPC request or response contains term T > currentTerm:
                // set currentTerm = T, convert to follower (§5.1)
                if peer_term > state.current_term {
                    state.current_term = peer_term;
                    state.leader_id = "FFFF".into();
                    state.voted_for = None; // OLEKS
                    return follower::start(state, socket);
                }

                // Leaders:
                // If AppendEntries fails because of log inconsistency:
                // decrement nextIndex and retry (§5.3)
                if !success {
                    eprintln!(
                        "{} LEAD: Node {} failed to append entries",
                        state.my_id, peer_id
                    );
                    state.dec_next_index_of(&peer_id);
                    continue 'being_leader;
                }
                
                eprintln!("{} LEAD: Node {} appended entry", state.my_id, peer_id);

                // Leaders:
                // If successful: update nextIndex and matchIndex for follower (§5.3)
                state.inc_next_index_of(&peer_id);
                if state.match_index_of(&peer_id) > 0 {
                    state.inc_match_index_of(&peer_id);
                } else {
                    let new_match_index = state.next_index_of(&peer_id) - 1;
                    state.set_match_index_of(&peer_id, new_match_index);
                }

                let mut count_matched = 1;

                for peer_id in state.peer_ids() {
                    let peer_match_index = state.match_index_of(&peer_id);
                    if peer_match_index >= state.commit_index + 1 {
                        count_matched += 1;
                    } else {
                        eprintln!(
                            "{} LEAD: Node {} is on match index {}",
                            state.my_id, peer_id, peer_match_index
                        );
                    }
                }

                eprintln!(
                    "{} LEAD: Replicated entry {} on {} nodes",
                    state.my_id, state.commit_index, count_matched
                );

                if count_matched >= cluster_size / 2 + 1 {
                    state.inc_commit_index();
                    eprintln!(
                        "{} LEAD: COMMITED index {}",
                        state.my_id, state.commit_index
                    );
                }

                // All Servers:
                // If commitIndex > lastApplied: increment lastApplied,
                // apply log[lastApplied] to state machine (§5.3)
                if state.commit_index > state.last_applied {
                    state.apply_last_entry();
                    let (client_id, mid) = match state.log.get_client_info(state.commit_index) {
                        Some((client_id, mid)) => (client_id, mid),
                        None => panic!("LEAD: No entry at commit_index exist"),
                    };

                    socket.send_msg(Message::new_ok_answer(client_id, mid, state))?;
                }

                continue 'being_leader;
            }

            Message::RPCRequestVote {
                src: candidate_id,
                term: candidate_term,
                ..
            } => {
                // All Servers:
                // If RPC request or response contains term T > currentTerm:
                // set currentTerm = T, convert to follower (§5.1)
                if candidate_term > state.current_term {
                    state.current_term = candidate_term;
                    state.leader_id = "FFFF".into();

                    state.voted_for = Some(candidate_id.clone());
                    socket.send_msg(Message::new_vote_reply(candidate_id.clone(), true, state))?;
                    
                    return follower::start(state, socket);
                }

                socket.send_msg(Message::new_vote_reply(candidate_id.clone(), true, state))?;
                continue 'being_leader;
            }

            Message::RPCAppendEntries {
                src: leader_id,
                term: leader_term,
                entries,
                prev_log_index,
                prev_log_term,
                leader_commit,
                ..
            } => {
                if leader_term < state.current_term {
                    eprintln!("{} LEAD: RPCAppendEntries: Past", state.my_id);
                    socket.send_msg(Message::new_append_reply(leader_id.clone(), false, state))?;
                    continue 'being_leader;
                }

                else if leader_term > state.current_term {
                    state.current_term = leader_term;
                    state.leader_id = leader_id.clone();
                    state.leader_state = None;
                    state.voted_for = None;

                    if entries.is_empty() {
                        return follower::start(state, socket);
                    }

                    let inconsistent = match state.log.get_term(prev_log_index) {
                        Some(term) => {
                            eprintln!(
                                "{} LEAD: term: {}, prev_log_term: {}",
                                state.my_id, term, prev_log_term
                            );
                            term != prev_log_term
                        }
                        None => {
                            eprintln!(
                                "{} LEAD: entry does not exist at given index: {}, log: {:?}",
                                state.my_id, prev_log_index, state.log
                            );
                            true
                        }
                    };

                    if inconsistent {
                        eprintln!("{} LEAD: RPCAppendEntries: Inconsistent State prev_log_index: {}, last_index: {}", state.my_id, prev_log_index, state.log.last_index());
                        socket.send_msg(Message::new_append_reply(leader_id, false, state))?;
                        return follower::start(state, socket);
                    }

                    let entry = entries.first().unwrap();

                    // Receiver implementation:
                    // 3. If an existing entry conflicts with a new one (same index
                    // but different terms), delete the existing entry and all that
                    // follow it (§5.3)
                    if let Some(term) = state.log.get_term(entry.index) {
                        // entry already in the log, ignore
                        if term == entry.term {
                            return follower::start(state, socket);
                        } else {
                            state.log.remove_all_starting_at(entry.index);
                        }
                    }

                    // Receiver implementation:
                    // 4. Append any new entries not already in the log
                    eprintln!(
                        "{} LEAD: Adding entry with index: {}",
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

                    socket.send_msg(Message::new_append_reply(leader_id, true, state))?;

                    return follower::start(state, socket);
                }

                unreachable!()
            }

            Message::RPCRequestVoteReply { .. } => {
                continue 'being_leader;
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