use crate::core::{Log, LogEntry, LogIndex, RaftServer, ServerId, Term};

pub trait Message {
    fn handle(handler: &mut RaftServer);
}

#[derive(Debug, Clone)]
pub enum MessageRequest {
    Empty,
    AppendEntries {
        term: Term,
        // Technically we don't need the leader_id for this implementation, but we will keep it here in case we decide
        // to allow for leader discovery the servers directly and not a higher level transport layer.
        leader_id: ServerId,
        prev_log_index: Option<LogIndex>,
        prev_log_term: Term,
        entries: Log<LogEntry>,
        leader_commit: Option<LogIndex>,
    },
    RequestVote {
        term: Term,
        candidate_id: ServerId,
        last_log_index: Option<LogIndex>,
        last_log_term: Term,
    },
}

#[derive(Debug, Clone)]
pub enum MessageResponse {
    Empty,
    AppendEntries { term: Term, success: bool },
    RequestVote { term: Term, vote_granted: bool },
}

#[cfg(test)]
mod tests {}
