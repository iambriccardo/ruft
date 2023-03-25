use std::{
    hash::Hash,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{
    messages::{MessageRequest, MessageResponse},
    timer::{TimerAction, TimerId, TimersManager},
    transport::TransportInstance,
};

pub type Log<T> = Vec<T>;
pub type LogIndex = usize;
pub type Term = u32;
pub type ServerId = u32;
pub type Command = u32;

#[derive(Debug)]
pub struct LogEntry {
    term: Term,
    command: Command,
}

#[derive(Debug, Clone, Copy)]
pub enum ServerRole {
    NONE,
    FOLLOWER,
    CANDIDATE,
    LEADER,
}

struct PersistentState {
    current_term: Term,
    voted_for: Option<ServerId>,
    voted_by: Option<usize>,
    log: Log<LogEntry>,
}

impl PersistentState {
    pub fn default() -> PersistentState {
        PersistentState {
            current_term: 0,
            voted_for: None,
            voted_by: None,
            log: vec![],
        }
    }
}

struct VolatileState {
    commit_index: LogIndex,
    last_applied: LogIndex,
}

impl VolatileState {
    pub fn default() -> VolatileState {
        VolatileState {
            commit_index: 0,
            last_applied: 0,
        }
    }
}

struct VolatileLeaderState {
    next_index: Vec<LogIndex>,
    match_index: Vec<LogIndex>,
}

#[derive(Clone, Copy)]
pub enum PrepareMessageType {
    EMPTY,
    APPEND_ENTRIES,
    REQUEST_VOTE,
}

pub type RaftServerInstance = Arc<Mutex<RaftServer>>;

pub struct RaftServer {
    // Basic base information.
    pub id: ServerId,
    pub role: ServerRole,
    // Basic state.
    persistent_state: PersistentState,
    volatile_state: VolatileState,
    volatile_leader_state: Option<VolatileLeaderState>,
    servers: u32,
    // Extra information.
    transport: Option<TransportInstance>,
    timers_manager: Option<TimersManager>,
    election_timer_id: Option<TimerId>,
}

impl RaftServer {
    pub fn new(id: ServerId) -> RaftServer {
        RaftServer {
            id,
            // By default the server has no role assigned.
            role: ServerRole::NONE,
            persistent_state: PersistentState::default(),
            volatile_state: VolatileState::default(),
            volatile_leader_state: None,
            servers: 0,
            transport: None,
            timers_manager: None,
            election_timer_id: None,
        }
    }

    pub fn new_instance(id: ServerId) -> RaftServerInstance {
        Arc::new(Mutex::new(Self::new(id)))
    }

    pub fn register_dependencies(
        &mut self,
        transport: TransportInstance,
        timers_manager: TimersManager,
    ) {
        self.transport = Some(transport);
        self.timers_manager = Some(timers_manager);
    }

    pub fn add_command(&mut self, command: Command) {
        // We want to asynchronously notify the caller about the application of this command to the state machine.
        self.persistent_state.log.push(LogEntry {
            term: self.persistent_state.current_term,
            command,
        })
    }

    pub fn prepare_message_request(&self, message_type: PrepareMessageType) -> MessageRequest {
        match message_type {
            PrepareMessageType::EMPTY => MessageRequest::Empty,
            PrepareMessageType::APPEND_ENTRIES => todo!(),
            PrepareMessageType::REQUEST_VOTE => MessageRequest::RequestVote {
                term: self.persistent_state.current_term,
                candidate_id: self.id,
                last_log_index: if self.persistent_state.log.is_empty() {
                    None
                } else {
                    Some(self.persistent_state.log.len() - 1)
                },
                last_log_term: self.persistent_state.current_term,
            },
        }
    }

    pub fn handle_message_request(
        &mut self,
        sender_id: ServerId,
        message: MessageRequest,
    ) -> MessageResponse {
        println!(
            "Server {} received message request {:?} from {}",
            self.id, message, sender_id
        );

        let response = match self.role {
            ServerRole::FOLLOWER => self.handle_message_request_as_follower(sender_id, message),
            ServerRole::CANDIDATE => self.handle_message_request_as_candidate(sender_id, message),
            ServerRole::LEADER => self.handle_message_request_as_leader(sender_id, message),
            ServerRole::NONE => MessageResponse::Empty,
        };

        if self.volatile_state.commit_index > self.volatile_state.last_applied {
            self.volatile_state.last_applied += 1;
            println!(
                "Applying state {:?} to state machine",
                self.persistent_state.log[self.volatile_state.last_applied]
            );
        }

        response
    }

    pub fn handle_message_request_as_follower(
        &mut self,
        sender_id: ServerId,
        message: MessageRequest,
    ) -> MessageResponse {
        match message {
            MessageRequest::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                // TODO: implement this later.
            }
            MessageRequest::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                // If the candidate's term is less than ours or we already voted for another candidate, we don't vote for the candidate.
                if term < self.persistent_state.current_term
                    || self.persistent_state.voted_for.is_some()
                {
                    return MessageResponse::RequestVote {
                        term: self.persistent_state.current_term,
                        vote_granted: false,
                    };
                }

                let last_log_entry = self.persistent_state.log.last();
                match last_log_entry {
                    Some(last_log_entry) => {
                        // We check if the candidate's log is at least as up to date as ours.
                        let candidate_log_newer = match last_log_index {
                            Some(last_log_index) => {
                                last_log_index >= (self.persistent_state.log.len() - 1)
                            }
                            None => true,
                        };

                        if last_log_term > last_log_entry.term
                            || (last_log_term == last_log_entry.term && candidate_log_newer)
                        {
                            // We vote for the candidate.
                            self.persistent_state.voted_for = Some(candidate_id);

                            return MessageResponse::RequestVote {
                                term: self.persistent_state.current_term,
                                vote_granted: true,
                            };
                        }
                    }
                    None => {
                        // In case we have an empty log, the candidate will always be as up to date as us, thus we vote for it.
                        self.persistent_state.voted_for = Some(candidate_id);

                        return MessageResponse::RequestVote {
                            term: self.persistent_state.current_term,
                            vote_granted: true,
                        };
                    }
                }

                return MessageResponse::RequestVote {
                    term: self.persistent_state.current_term,
                    vote_granted: false,
                };
            }
            _ => {}
        }

        MessageResponse::Empty
    }

    pub fn handle_message_request_as_candidate(
        &mut self,
        sender_id: ServerId,
        message: MessageRequest,
    ) -> MessageResponse {
        MessageResponse::Empty
    }

    pub fn handle_message_request_as_leader(
        &mut self,
        sender_id: ServerId,
        message: MessageRequest,
    ) -> MessageResponse {
        MessageResponse::Empty
    }

    pub fn handle_message_response(&mut self, sender_id: ServerId, message: MessageResponse) {
        println!(
            "Server {} received message response {:?} from {}",
            self.id, message, sender_id
        );

        match message {
            MessageResponse::AppendEntries { term, success } => todo!(),
            MessageResponse::RequestVote { term, vote_granted } => {
                if vote_granted {
                    self.persistent_state.voted_by =
                        Some(self.persistent_state.voted_by.map_or(0, |value| value) + 1);

                    // In case we have received a role from the majority, including ourselves, we want to switch to leaders.
                    if let Some(voted_by) = self.persistent_state.voted_by {
                        if voted_by > (self.servers / 2) as usize {
                            self.change_role(ServerRole::LEADER);
                        }
                    }
                    // TODO: here we would need to send a message to all the other servers. We could implement a broadcaster component to simulate
                    // a transport layer.
                }
            }
            MessageResponse::Empty => {}
        }
    }

    pub fn change_role(&mut self, new_role: ServerRole) {
        println!(
            "Changing role of server {:?} from {:?} to {:?}",
            self.id, self.role, new_role
        );

        self.on_change_role(Some(self.role), new_role);
        self.role = new_role;
    }

    fn on_change_role(&mut self, prev_role: Option<ServerRole>, new_role: ServerRole) {
        // We must make sure that any call to transport or timers manager is non-blocking, otherwise we will deadlock, since those
        // services will want to get a lock on this instance of the server.
        match new_role {
            ServerRole::FOLLOWER => {
                self.election_timer_id = Some(
                    // TODO: check properly that the timers manager is added.
                    self.timers_manager
                        .as_mut()
                        .unwrap()
                        .register(Duration::from_secs(2), TimerAction::SWITCH_TO_CANDIDATE),
                );
            }
            ServerRole::CANDIDATE => {
                if let Some(timer_id) = self.election_timer_id {
                    self.timers_manager.as_mut().unwrap().stop(timer_id);
                }

                self.transport
                    .as_ref()
                    .unwrap()
                    .lock()
                    .unwrap()
                    .broadcast(self.id, PrepareMessageType::REQUEST_VOTE)
            }
            _ => {}
        }
    }

    pub fn bind(&mut self, n: u32) {
        self.servers += n
    }
}

impl Eq for RaftServer {}

impl PartialEq for RaftServer {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Hash for RaftServer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

// TODO: implement a Raft client which sends commands to the leader, either by discovery or just by always knowing who is the leader.
struct RaftClient {}

#[cfg(test)]
mod tests {}
