use crate::core::{Log, LogIndex, RaftServer, ServerId, Term};

pub trait Message {
    fn handle(handler: &mut RaftServer);
}

#[derive(Debug, Clone)]
pub enum MessageRequest {
    Empty,
    AppendEntries {
        term: Term,
        leader_id: ServerId,
        prev_log_index: Option<LogIndex>,
        prev_log_term: Term,
        entries: Log<u32>,
        leader_commit: LogIndex,
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
