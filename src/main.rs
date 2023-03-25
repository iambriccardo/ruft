use std::{
    borrow::{Borrow, BorrowMut},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use timer::TimersManager;
use transport::Transport;

use crate::core::{RaftServer, ServerRole};

pub mod core;
pub mod messages;
pub mod timer;
pub mod transport;

fn main() {
    thread::spawn(move || {
        let transport = Transport::new_instance();
        for server in vec![
            RaftServer::new_instance(1),
            RaftServer::new_instance(2),
            RaftServer::new_instance(3),
        ] {
            server.lock().unwrap().register_dependencies(
                transport.clone(),
                TimersManager::new(server.clone(), transport.clone()),
            );
            server.lock().unwrap().change_role(ServerRole::FOLLOWER);

            transport.lock().unwrap().add_server(server);
        }
    });

    thread::sleep(Duration::from_secs(100));
}
