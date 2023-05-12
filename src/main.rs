use crate::core::{ClientState, RaftClient};
use std::{
    thread::{self, sleep},
    time::Duration,
};

use timer::TimersManager;
use transport::Transport;

use crate::core::{RaftServer, ServerRole};

pub mod core;
pub mod messages;
pub mod timer;
pub mod transport;

#[derive(Debug, Clone)]
struct ReplicatedList {
    list: Vec<u32>,
}

impl ClientState<Vec<u32>> for ReplicatedList {
    fn init() -> Self {
        ReplicatedList { list: vec![] }
    }

    fn apply_command(&mut self, command: core::Command) {
        self.list.push(command);
    }

    fn get_state(&self) -> Vec<u32> {
        self.list.clone()
    }
}

fn main() {
    let transport = Transport::new_instance();
    let client = RaftClient::new(transport.clone(), ReplicatedList::init());
    let client_shared_state = client.shared_state.clone();

    let inner_transport = transport.clone();
    thread::spawn(move || {
        for server in vec![
            RaftServer::new_instance(1),
            RaftServer::new_instance(2),
            RaftServer::new_instance(3),
        ] {
            server.lock().unwrap().register_dependencies(
                inner_transport.clone(),
                TimersManager::new(server.clone(), inner_transport.clone()),
                client_shared_state.clone(),
            );
            server.lock().unwrap().change_role(ServerRole::FOLLOWER);

            inner_transport.lock().unwrap().add_server(server);
        }
    });

    loop {
        transport.lock().unwrap().add_command(1);
        sleep(Duration::from_secs(5));
    }
}
