use std::{
    cmp::min,
    collections::HashMap,
    fmt::Display,
    hash::Hash,
    marker::PhantomData,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{
    messages::{MessageRequest, MessageResponse},
    timer::{TimerAction, TimerId, TimersManager},
    transport::Transport,
};

pub type Log<T> = Vec<T>;
pub type LogIndex = usize;
pub type Term = u32;
pub type ServerId = u32;
// For now we don't use command as a generic type but in the future it will be needed.
pub type Command = u32;

#[derive(Debug, Clone, Copy)]
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

    pub fn prev_log_index(&self) -> Option<LogIndex> {
        if let Some(last_log_index) = self.last_log_index() {
            if last_log_index == 0 {
                return None;
            }

            return Some(last_log_index - 1);
        }

        None
    }

    pub fn prev_log_term(&self) -> Term {
        self.current_term
    }

    pub fn last_log_index(&self) -> Option<LogIndex> {
        if !self.log.is_empty() {
            return Some(self.log.len() - 1);
        }

        None
    }

    pub fn log_entries_from(&self, index: LogIndex) -> Vec<LogEntry> {
        self.log.as_slice()[index..].to_vec()
    }
}

struct VolatileState {
    commit_index: Option<LogIndex>,
    last_applied: Option<LogIndex>,
}

impl VolatileState {
    pub fn default() -> VolatileState {
        VolatileState {
            commit_index: None,
            last_applied: None,
        }
    }
}

struct VolatileLeaderState {
    next_index: HashMap<ServerId, LogIndex>,
    match_index: HashMap<ServerId, LogIndex>,
}

impl VolatileLeaderState {
    pub fn init(
        last_log_index: Option<LogIndex>,
        server_ids: Vec<ServerId>,
    ) -> VolatileLeaderState {
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();
        let next_log_index = match last_log_index {
            Some(last_log_index) => last_log_index + 1,
            None => 0,
        };

        server_ids.iter().for_each(|&id| {
            next_index.insert(id, next_log_index);
            match_index.insert(id, 0);
        });

        VolatileLeaderState {
            next_index,
            match_index,
        }
    }

    pub fn next_index(&self, server_id: ServerId) -> Option<LogIndex> {
        self.next_index.get(&server_id).map(|value| *value)
    }
}

#[derive(Clone, Copy)]
pub enum PrepareMessageType {
    EMPTY,
    APPEND_ENTRIES { decrement: usize },
    REQUEST_VOTE,
}

impl PrepareMessageType {
    pub fn retry(&self) -> Self {
        match self {
            PrepareMessageType::APPEND_ENTRIES { decrement } => {
                // In case we retrying the append entries message, we want to decrement the next_index by 1 in order to
                // try to find a index such that the prev_index and prev_term invariants hold.
                PrepareMessageType::APPEND_ENTRIES {
                    decrement: decrement + 1,
                }
            }
            _ => self.clone(),
        }
    }
}

pub struct RaftServer<T, E>
where
    T: Display + Send + Sync + ClientState<E> + Clone + 'static,
    E: Send + Sync + 'static,
{
    // Basic base information.
    pub id: ServerId,
    pub role: ServerRole,
    // Basic state.
    persistent_state: PersistentState,
    volatile_state: VolatileState,
    volatile_leader_state: Option<VolatileLeaderState>,
    server_ids: Vec<ServerId>,
    // Extra information.
    transport: Option<Arc<Mutex<Transport<T, E>>>>,
    timers_manager: Option<TimersManager<T, E>>,
    election_tid: Option<TimerId>,
    append_entires_tid: Option<TimerId>,
    shared_state: Option<Arc<Mutex<T>>>,
    _1: PhantomData<E>,
}

impl<T, E> RaftServer<T, E>
where
    T: Display + Send + Sync + ClientState<E> + Clone + 'static,
    E: Send + Sync + 'static,
{
    pub fn new(id: ServerId) -> RaftServer<T, E> {
        RaftServer {
            id,
            // By default the server has no role assigned.
            role: ServerRole::NONE,
            persistent_state: PersistentState::default(),
            volatile_state: VolatileState::default(),
            volatile_leader_state: None,
            server_ids: vec![],
            transport: None,
            timers_manager: None,
            election_tid: None,
            append_entires_tid: None,
            shared_state: None,
            _1: PhantomData,
        }
    }

    pub fn new_instance(id: ServerId) -> Arc<Mutex<RaftServer<T, E>>> {
        Arc::new(Mutex::new(Self::new(id)))
    }

    pub fn register_dependencies(
        &mut self,
        transport: Arc<Mutex<Transport<T, E>>>,
        timers_manager: TimersManager<T, E>,
        shared_state: Arc<Mutex<T>>,
    ) {
        self.transport = Some(transport);
        self.timers_manager = Some(timers_manager);
        self.shared_state = Some(shared_state);
    }

    pub fn add_command(&mut self, command: Command) {
        // We want to asynchronously notify the caller about the application of this command to the state machine.
        self.persistent_state.log.push(LogEntry {
            term: self.persistent_state.current_term,
            command,
        })
    }

    pub fn prepare_message_request(
        &self,
        message_type: PrepareMessageType,
        receiver_id: ServerId,
    ) -> MessageRequest {
        match message_type {
            PrepareMessageType::EMPTY => MessageRequest::Empty,
            PrepareMessageType::APPEND_ENTRIES { decrement } => {
                if let ServerRole::LEADER = self.role {
                    let last_log_index = self.persistent_state.last_log_index();
                    let next_index = self
                        .volatile_leader_state
                        .as_ref()
                        .unwrap()
                        .next_index(receiver_id);

                    // By default we want to send an empty array of entries.
                    let mut entries = vec![];
                    if let (Some(last_log_index), Some(next_index)) = (last_log_index, next_index) {
                        // We want to check if we have to send new log entries to receiver.
                        if last_log_index >= next_index - decrement {
                            entries = self.persistent_state.log_entries_from(next_index);
                        }
                    }

                    return MessageRequest::AppendEntries {
                        term: self.persistent_state.current_term,
                        leader_id: self.id,
                        prev_log_index: self.persistent_state.prev_log_index(),
                        prev_log_term: self.persistent_state.prev_log_term(),
                        entries,
                        leader_commit: self.volatile_state.commit_index,
                    };
                }

                MessageRequest::Empty
            }
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
        message_request: MessageRequest,
    ) -> MessageResponse {
        println!(
            "Server {} received message request {:?} from {}",
            self.id, message_request, sender_id
        );

        self.check_if_incoming_req_term_is_greater(message_request.clone());

        let response = match message_request {
            MessageRequest::AppendEntries {
                term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                ..
            } => self.handle_append_entries_req(
                term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            ),
            MessageRequest::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => self.handle_request_vote_req(term, candidate_id, last_log_index, last_log_term),
            _ => MessageResponse::Empty,
        };

        response
    }

    pub fn handle_request_vote_req(
        &mut self,
        term: Term,
        candidate_id: ServerId,
        last_log_index: Option<LogIndex>,
        last_log_term: Term,
    ) -> MessageResponse {
        // Only a followe and a candidate can vote.
        match self.role {
            ServerRole::FOLLOWER | ServerRole::CANDIDATE => {
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

                MessageResponse::RequestVote {
                    term: self.persistent_state.current_term,
                    vote_granted: false,
                }
            }
            _ => MessageResponse::Empty,
        }
    }

    pub fn handle_append_entries_req(
        &mut self,
        term: Term,
        prev_log_index: Option<LogIndex>,
        prev_log_term: Term,
        entries: Log<LogEntry>,
        leader_commit: Option<LogIndex>,
    ) -> MessageResponse {
        match self.role {
            ServerRole::CANDIDATE => {
                self.change_role(ServerRole::FOLLOWER);
            }
            ServerRole::FOLLOWER => {
                // Whenever we have an incoming append entries request, we have to reset the timer for switching to candididate,
                // in order to allow the system to always have the same leader until faults occur.
                self.clear_active_timers();
                self.election_tid = Some(
                    self.timers_manager
                        .as_mut()
                        .unwrap()
                        .register(Duration::from_secs(2), TimerAction::SWITCH_TO_CANDIDATE),
                );

                if term < self.persistent_state.current_term {
                    return MessageResponse::AppendEntries {
                        term: self.persistent_state.current_term,
                        success: false,
                    };
                }

                // Only if there is a previous entry we want to check that the terms match.
                if let Some(prev_log_index) = prev_log_index {
                    if prev_log_index < self.persistent_state.log.len() {
                        let log_entry = self.persistent_state.log[prev_log_index];
                        if log_entry.term != prev_log_term {
                            return MessageResponse::AppendEntries {
                                term: self.persistent_state.current_term,
                                success: false,
                            };
                        }
                    } else {
                        // In case the prev_log_index is out of bound, we also bail.
                        return MessageResponse::AppendEntries {
                            term: self.persistent_state.current_term,
                            success: false,
                        };
                    }
                }

                // In case no prev_log_index is present, our append index will start from the first element of the array.
                let mut append_index = prev_log_index.map_or(0, |value| value + 1);
                let direct_access_max_index = if self.persistent_state.log.is_empty() {
                    None
                } else {
                    Some(self.persistent_state.log.len() - 1)
                };
                for entry in entries.into_iter() {
                    if let Some(direct_access_max_index) = direct_access_max_index {
                        if append_index <= direct_access_max_index {
                            self.persistent_state.log[append_index] = entry;
                        } else {
                            self.persistent_state.log.push(entry);
                        }
                    } else {
                        self.persistent_state.log.push(entry);
                    }
                    append_index += 1;
                }

                let leader_commit = leader_commit.map_or(-1 as f32, |value| value as f32);
                let local_commit = self
                    .volatile_state
                    .commit_index
                    .map_or(-1 as f32, |value| value as f32);
                // In case the leader has committed more entries, we want to advance ours too.
                if leader_commit > local_commit {
                    if let Some(last_log_index) = self.persistent_state.last_log_index() {
                        // TODO: properly implement comparable type with total ordering between Option.
                        if leader_commit == -1 as f32 {
                            self.volatile_state.commit_index = Some(last_log_index);
                        } else {
                            self.volatile_state.commit_index =
                                Some(min(leader_commit as usize, last_log_index));
                        }
                    } else {
                        return MessageResponse::AppendEntries {
                            term: self.persistent_state.current_term,
                            success: false,
                        };
                    }
                }

                // TODO: implement logic to apply the log entries.
                return MessageResponse::AppendEntries {
                    term: self.persistent_state.current_term,
                    success: true,
                };
            }
            _ => {}
        }

        MessageResponse::Empty
    }

    pub fn handle_message_response(
        &mut self,
        sender_id: ServerId,
        // We want to also get access to the request that led to the response, in order to capture
        // stateful behavior.
        message_request: MessageRequest,
        message_response: MessageResponse,
    ) -> bool {
        println!(
            "Server {} received message response {:?} from {}",
            self.id, message_response, sender_id
        );

        self.check_if_incoming_res_term_is_greater(message_response.clone());

        let success = match message_response {
            MessageResponse::AppendEntries { term, success } => {
                self.handle_append_entries_res(term, success, sender_id, message_request)
            }
            MessageResponse::RequestVote { term, vote_granted } => {
                self.handle_request_vote_res(term, vote_granted)
            }
            MessageResponse::Empty => true,
        };

        // If we are the leader, after a response we want to advance the commit index and possibly the state.
        if let ServerRole::LEADER = self.role {
            self.try_advance_commit_index();
            self.try_advance_state();
        }

        success
    }

    fn handle_request_vote_res(&mut self, _: Term, vote_granted: bool) -> bool {
        if vote_granted {
            self.persistent_state.voted_by =
                Some(self.persistent_state.voted_by.map_or(0, |value| value) + 1);

            // In case we have received a role from the majority, including ourselves, we want to switch to leaders.
            if let Some(voted_by) = self.persistent_state.voted_by {
                // Since we always vote for ourselves, we require the majority - 1.
                if voted_by > (self.majority() - 1) {
                    self.change_role(ServerRole::LEADER);
                }
            }
        }

        true
    }

    fn handle_append_entries_res(
        &mut self,
        _: Term,
        success: bool,
        sender_id: ServerId,
        message_request: MessageRequest,
    ) -> bool {
        if let MessageRequest::AppendEntries {
            term: _,
            leader_id: _,
            prev_log_index: _,
            prev_log_term: _,
            entries,
            leader_commit: _,
        } = message_request
        {
            if success && !entries.is_empty() {
                let entries_len = entries.len();
                *self
                    .volatile_leader_state
                    .as_mut()
                    .unwrap()
                    .next_index
                    .get_mut(&sender_id)
                    .unwrap() += entries_len;
                // The match index is always next_index - 1.
                *self
                    .volatile_leader_state
                    .as_mut()
                    .unwrap()
                    .match_index
                    .get_mut(&sender_id)
                    .unwrap() = *self
                    .volatile_leader_state
                    .as_mut()
                    .unwrap()
                    .next_index
                    .get_mut(&sender_id)
                    .unwrap()
                    - 1;

                println!(
                    "Entries#{:?} applied successfully on replica {:?}, updating volatile leader state.",
                    entries_len, sender_id
                );
            }
        }

        success
    }

    fn majority(&self) -> usize {
        // The majority will always been half of the replicas to which we are connected + 1. This works under the assumption
        // that server_ids contains an even number of elements that point to all the replicas.
        (self.server_ids.len() / 2) + 1
    }

    fn check_if_incoming_req_term_is_greater(&mut self, message: MessageRequest) {
        match message {
            MessageRequest::AppendEntries { term, .. }
            | MessageRequest::RequestVote { term, .. }
                if term > self.persistent_state.current_term =>
            {
                self.change_role(ServerRole::FOLLOWER);
            }
            _ => {}
        }
    }

    fn check_if_incoming_res_term_is_greater(&mut self, message: MessageResponse) {
        match message {
            MessageResponse::AppendEntries { term, .. }
            | MessageResponse::RequestVote { term, .. }
                if term > self.persistent_state.current_term =>
            {
                self.change_role(ServerRole::FOLLOWER)
            }
            _ => {}
        }
    }

    fn try_advance_commit_index(&mut self) {
        // We want to find an index N such that:
        // N > self.volatile_state.commit_index
        // A majority of self.volatile_leader_state.match_index >= N
        // self.persistent_state.log[N].term == self.persistent_state.current_term

        // It is importnat to note that this function is designed to be used incrementally and will not converge automatically
        // to the highest commit index that satisfies the aforementioned conditions.

        // The next most likely commit index is the next commit index.
        let next_commit_index = self
            .volatile_state
            .commit_index
            .map_or(0, |value| value + 1);

        // We can run this logic only if we are a leader, which means we have a volatile leader state.
        if next_commit_index < self.persistent_state.log.len() {
            if let Some(volatile_leader_state) = self.volatile_leader_state.as_mut() {
                // TODO: find problem in this function for why the commit index is not updated.
                let at_least_up_to_date = volatile_leader_state
                    .match_index
                    .iter()
                    .filter(|(_, value)| **value >= next_commit_index)
                    .count();

                // We exclude ourselves from the count of the majority.
                if at_least_up_to_date >= (self.majority() - 1)
                    && self.persistent_state.log[next_commit_index].term
                        == self.persistent_state.current_term
                {
                    self.volatile_state.commit_index = Some(next_commit_index);
                }
            }
        }
    }

    fn try_advance_state(&mut self) {
        let last_applied = self
            .volatile_state
            .last_applied
            .map_or(-1 as f32, |value| value as f32);

        if let Some(commit_index) = self.volatile_state.commit_index {
            if commit_index as f32 > last_applied {
                self.volatile_state.last_applied = Some((last_applied + 1 as f32) as usize);
                let applied_log_entry =
                    self.persistent_state.log[self.volatile_state.last_applied.unwrap()];
                println!("Applying state {:?} to state machine", applied_log_entry);
                // In case there is some shared state, we want to apply the newly added log entry.
                if let Some(shared_state) = self.shared_state.as_ref() {
                    shared_state
                        .lock()
                        .unwrap()
                        .apply_command(applied_log_entry.command);
                }
            }
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

    fn on_change_role(&mut self, _: Option<ServerRole>, new_role: ServerRole) {
        // When changing role, we want to clear timers that were active in the past, in order to avoid having problems with messages
        // being sent when they shouldn't.
        self.clear_active_timers();
        // In case we have a leader state we want to delete it.
        if self.volatile_leader_state.is_some() {
            self.volatile_leader_state = None;
        }
        // We must make sure that any call to transport or timers manager is non-blocking, otherwise we will deadlock, since those
        // services will want to get a lock on this instance of the server.
        match new_role {
            ServerRole::FOLLOWER => {
                self.election_tid = Some(
                    self.timers_manager
                        .as_mut()
                        .unwrap()
                        .register(Duration::from_secs(2), TimerAction::SWITCH_TO_CANDIDATE),
                );
            }
            ServerRole::CANDIDATE => self
                .transport
                .as_ref()
                .unwrap()
                .lock()
                .unwrap()
                .broadcast(self.id, PrepareMessageType::REQUEST_VOTE),
            ServerRole::LEADER => {
                self.append_entires_tid = Some(
                    self.timers_manager
                        .as_mut()
                        .unwrap()
                        .register(Duration::from_secs(1), TimerAction::SEND_APPEND_ENTRIES),
                );
                self.volatile_leader_state = Some(VolatileLeaderState::init(
                    self.persistent_state.last_log_index(),
                    self.server_ids.iter().cloned().collect(),
                ));
                self.transport
                    .as_mut()
                    .unwrap()
                    .lock()
                    .unwrap()
                    .notify_new_leader(self.id);
            }
            _ => {}
        }
    }

    fn clear_active_timers(&mut self) {
        if let Some(tid) = self.election_tid {
            self.timers_manager.as_mut().unwrap().stop(tid);
            self.election_tid = None
        }

        if let Some(tid) = self.append_entires_tid {
            self.timers_manager.as_mut().unwrap().stop(tid);
            self.append_entires_tid = None
        }
    }

    pub fn bind(&mut self, server_id: ServerId) {
        self.server_ids.push(server_id);
    }

    pub fn bind_multiple(&mut self, server_ids: &mut Vec<ServerId>) {
        self.server_ids.append(server_ids);
    }
}

impl<T, E> Eq for RaftServer<T, E>
where
    T: Display + Send + Sync + ClientState<E> + Clone + 'static,
    E: Send + Sync + 'static,
{
}

impl<T, E> PartialEq for RaftServer<T, E>
where
    T: Display + Send + Sync + ClientState<E> + Clone + 'static,
    E: Send + Sync + 'static,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T, E> Hash for RaftServer<T, E>
where
    T: Display + Send + Sync + ClientState<E> + Clone + 'static,
    E: Send + Sync + 'static,
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

pub trait ClientState<T> {
    fn init() -> Self;

    fn apply_command(&mut self, command: Command);

    fn get_state(&self) -> T;
}

// TODO: implement a Raft client which sends commands to the leader, either by discovery or just by always knowing who is the leader.
pub struct RaftClient<T, E>
where
    T: Display + Send + Sync + ClientState<E> + Clone + 'static,
    E: Send + Sync + 'static,
{
    transport: Arc<Mutex<Transport<T, E>>>,
    pub shared_state: Arc<Mutex<T>>,
    _1: PhantomData<E>,
}

impl<T, E> RaftClient<T, E>
where
    T: Display + Send + Sync + ClientState<E> + Clone + 'static,
    E: Send + Sync + 'static,
{
    pub fn new(transport: Arc<Mutex<Transport<T, E>>>, shared_state: T) -> RaftClient<T, E> {
        RaftClient {
            transport,
            shared_state: Arc::new(Mutex::new(shared_state.clone())),
            _1: PhantomData,
        }
    }

    pub fn add_command(&self, command: Command) {
        self.transport.lock().unwrap().add_command(command);
    }

    pub fn inspect_state(&self) {
        println!(
            "The state of the client is {}",
            self.shared_state.lock().unwrap()
        )
    }
}
