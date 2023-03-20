use std::borrow::{Borrow, BorrowMut};

use crate::core::{bind_servers, broadcast, Server, ServerRole};

pub mod core;
pub mod messages;

fn main() {
    let servers = vec![Server::init(1), Server::init(2), Server::init(3)];
    bind_servers(servers.iter().cloned().collect());
    broadcast(servers.iter().cloned().collect());

    servers[0]
        .lock()
        .unwrap()
        .change_role(ServerRole::CANDIDATE);

    // Simulating a voting request.
    let mut index = 0;
    'outer: loop {
        let server = servers[index].lock().unwrap();
        let server_id = server.id;
        let message = server.prepare_message_request(core::PrepareMessageType::REQUEST_VOTE);
        drop(server);

        for i in 0..servers.len() {
            if i != index {
                let mut other_server = servers[i].lock().unwrap();
                let other_server_id = other_server.id;
                let response = other_server.handle_message_request(server_id, message.clone());
                drop(other_server);

                println!("{:?}", response);
                let mut server = servers[index].lock().unwrap();
                server.handle_message_response(other_server_id, response);
                if let ServerRole::LEADER = server.role {
                    println!("Election successful, new leader is server {}", server.id);
                    break 'outer;
                }
            }
        }

        index = (index + 1) % 3
    }
}
