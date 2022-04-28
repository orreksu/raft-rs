// candidate.rs

// use std::cmp::min;

use crate::NodeState;
use crate::{begin_an_election, rand_in, timeout_reached};
use crate::{follower, leader};
use crate::Instant;
use crate::{Message, Result, Socket};

/// Have I won the election by simple majority?
fn have_won_the_election(received_votes: usize, cluster_size: usize) -> bool {
    received_votes >= cluster_size / 2 + 1
}

/// Candidate asks nodes to vote for it.
/// Determines if it wins the election, if so becomes a leader, otherwise a follower.
/// Elections are won by simple majority.
pub fn start(state: &mut NodeState, socket: &Socket) -> Result<()> {
    let cluster_size = state.peer_ids.len() + 1;

    state.leader_id = "FFFF".into();

    state.voted_for = Some(state.my_id());
    let mut my_votes = 1;

    for peer_id in state.peer_ids() {
        eprintln!("{} CAND: Vote Request to {}", state.my_id, peer_id);
        socket.send_msg(Message::new_vote_request(state, peer_id))?;
    }

    let election_timeout = rand_in(400..800);
    let mut time_elapsed = Instant::now();
    
    'count_votes: loop {

        while !socket.ready_for_read() {
            if timeout_reached(time_elapsed, election_timeout) {
                eprintln!("{} CAND: Restarting as candidate in new term", state.my_id);
                return begin_an_election(state, socket);
            }
        }

        match socket.read_msg()? {
            msg @ Message::ClientPutRequest { .. } | msg @ Message::ClientGetRequest { .. } => {
                state.push_msg_to_queue(msg);
                continue 'count_votes;
            }

            Message::RPCRequestVoteReply {
                src: peer_id,
                term: peer_term,
                vote_granted,
                ..
            } => {
                // OLEKS:
                // All Servers:
                // If RPC request or response contains term T > currentTerm:
                // set currentTerm = T, convert to follower (§5.1)
                // if peer_term > state.current_term {
                //     state.current_term = peer_term;
                //     state.leader_id = "FFFF".into();
                //     state.voted_for = None;
                //     return follower::start(state, socket);
                // } else if peer_term < state.current_term {
                //     continue 'count_votes;
                // }

                if vote_granted {
                    eprintln!("{} CAND: Vote from {}", state.my_id, peer_id);
                    my_votes += 1;
                }

                if have_won_the_election(my_votes, cluster_size) {
                    eprintln!(
                        "\n\n{} CAND: I am the leader now, my commit index is: {}",
                        state.my_id, state.commit_index
                    );
                    state.leader_id = state.my_id();
                    state.init_leader_state();
                    return leader::start(state, socket);
                }

                time_elapsed = Instant::now();
                continue 'count_votes;
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
                // If the leader’s term is at least as large as the candidate’s current term,
                // then the candidate recognizes the leader as legitimate and returns to follower state.
                if leader_term >= state.current_term {
                    state.leader_id = leader_id;
                    state.current_term = leader_term;

                    // TODO: something...

                    return follower::start(state, socket);
                }
                // OLEKS:
                // // Receiver implementation:
                // // 1. Reply false if term < currentTerm (§5.1)
                // if leader_term < state.current_term {
                //     eprintln!("{} COND: Leader is from from past", state.my_id);
                //     socket.send_msg(Message::new_append_reply(leader_id, false, 0, state))?;
                //     continue 'count_votes;
                // }

                // // If the leader’s term is at least as large as the candidate’s current term,
                // // then the candidate recognizes the leader as legitimate and returns to follower state.
                // if leader_term >= state.current_term {
                //     state.current_term = leader_term;
                //     state.leader_id = leader_id.clone();
                //     state.leader_state = None;
                //     state.voted_for = None;

                //     if entries.is_empty() {
                //         return follower::start(state, socket);
                //     }

                //     let inconsistent = match state.log.get_term(prev_log_index) {
                //         Some(term) => {
                //             eprintln!(
                //                 "{} CAND: term: {}, prev_log_term: {}",
                //                 state.my_id, term, prev_log_term
                //             );
                //             term != prev_log_term
                //         }
                //         None => {
                //             eprintln!(
                //                 "{} CAND: entry does not exist at given index: {}, log: {:?}",
                //                 state.my_id, prev_log_index, state.log
                //             );
                //             true
                //         }
                //     };

                //     if inconsistent {
                //         eprintln!("{} CAND: RPCAppendEntries: Inconsistent State prev_log_index: {}, last_index: {}", state.my_id, prev_log_index, state.log.last_index());
                //         socket.send_msg(Message::new_append_reply(leader_id, false, 0, state))?;
                //         return follower::start(state, socket);
                //     }

                //     let entries_len = entries.len();

                //     for entry in entries {
                //         // Receiver implementation:
                //         // 3. If an existing entry conflicts with a new one (same index
                //         // but different terms), delete the existing entry and all that
                //         // follow it (§5.3)
                //         if let Some(term) = state.log.get_term(entry.index) {
                //             // entry already in the log, ignore
                //             if term == entry.term {
                //                 // return follower::start(state, socket);
                //                 continue;
                //             } else {
                //                 state.log.remove_all_starting_at(entry.index);
                //             }
                //         }

                //         // Receiver implementation:
                //         // 4. Append any new entries not already in the log
                //         eprintln!(
                //             "{} CAND: Adding entry with index: {}",
                //             state.my_id, entry.index
                //         );
                //         state.log.add_entry(entry.clone());

                //         // Receiver implementation:
                //         // 5. If leaderCommit > commitIndex,
                //         // set commitIndex = min(leaderCommit, index of last new entry)
                //         if leader_commit > state.commit_index {
                //             state.commit_index = min(leader_commit, state.log.last_index());
                //         }

                //         // All Servers:
                //         // If commitIndex > lastApplied: increment lastApplied,
                //         // apply log[lastApplied] to state machine (§5.3)
                //         if state.commit_index > state.last_applied {
                //             state.apply_last_entry();
                //         }
                //     }

                //     socket.send_msg(Message::new_append_reply(leader_id, true, entries_len, state))?;

                //     return follower::start(state, socket);
                // }

                // If the leader_term is smaller than the candidate’s current term,
                // then the candidate rejects the RPC and continues in candidate state.
                time_elapsed = Instant::now();
                continue 'count_votes;
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

                socket.send_msg(Message::new_vote_reply(candidate_id.clone(), false, state))?;
                continue 'count_votes;
            }

            Message::RPCAppendEntriesReply { .. } => {
                continue 'count_votes;
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
