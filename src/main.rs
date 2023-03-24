use std::{
    borrow::{Borrow, BorrowMut},
    sync::{Arc, Mutex},
    time::Duration,
};

use timer::ServerTimers;
use transport::Transport;

use crate::core::{Server, ServerRole};

pub mod core;
pub mod messages;
pub mod timer;
pub mod transport;

fn main() {
    let mut transport = Arc::new(Mutex::new(Transport::new()));
    for server in vec![Server::init(1), Server::init(2), Server::init(3)] {
        transport.lock().unwrap().add_server(server);
    }

    transport
        .lock()
        .unwrap()
        .broadcast(1, core::PrepareMessageType::REQUEST_VOTE);

    let mut timers = ServerTimers::new(1, transport);
    timers.register(Duration::from_secs(1), core::PrepareMessageType::EMPTY);
    timers.join_all();
}
