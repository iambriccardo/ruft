use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread,
};

use crate::core::{ClientState, PrepareMessageType, RaftServer, ServerId};

pub struct Transport<T, E>
where
    T: Send + Sync + ClientState<E> + Clone + 'static,
    E: Send + Sync + 'static,
{
    servers: Arc<Mutex<HashMap<ServerId, Arc<Mutex<RaftServer<T, E>>>>>>,
    leader_id: Option<ServerId>,
}

impl<T, E> Transport<T, E>
where
    T: Send + Sync + ClientState<E> + Clone + 'static,
    E: Send + Sync + 'static,
{
    pub fn new() -> Transport<T, E> {
        Transport {
            servers: Arc::new(Mutex::new(HashMap::new())),
            leader_id: None,
        }
    }

    pub fn new_instance() -> Arc<Mutex<Transport<T, E>>> {
        Arc::new(Mutex::new(Self::new()))
    }

    pub fn add_server(&mut self, server: Arc<Mutex<RaftServer<T, E>>>) {
        let mut _server = server.lock().unwrap();
        let mut server_ids: Vec<ServerId> = self
            .servers
            .lock()
            .unwrap()
            .iter()
            .map(|(&id, server)| {
                // For each of the other servers we bind the new member.
                server.lock().unwrap().bind(_server.id);
                id
            })
            .collect();
        // Once all the other servers are notified of the new member, we notify the new member the presence of the other members.
        _server.bind_multiple(&mut server_ids);
        self.servers
            .lock()
            .unwrap()
            .insert(_server.id, server.clone());
    }

    pub fn add_command(&self, command: u32) {
        if let Some(leader_id) = self.leader_id {
            if let Some(leader) = self.servers.lock().unwrap().get_mut(&leader_id) {
                leader.lock().unwrap().add_command(command);
            } else {
                println!("No leader has been elected, thus the command can't be issued.");
            }
        } else {
            println!("No leader has been elected, thus the command can't be issued.");
        }
    }

    pub fn notify_new_leader(&mut self, new_leader_id: ServerId) {
        self.leader_id = Some(new_leader_id);
    }

    pub fn broadcast(&mut self, sender_id: ServerId, message_type: PrepareMessageType) {
        let mut message_type = message_type;
        let servers = self.servers.clone();
        thread::spawn(move || {
            let servers = servers.lock().unwrap();
            if let Some(sender_server) = servers.get(&sender_id) {
                servers.iter().for_each(|(receiver_id, receiver_server)| {
                    if *receiver_id != sender_id {
                        loop {
                            let message_request = sender_server
                                .lock()
                                .unwrap()
                                .prepare_message_request(message_type, *receiver_id);

                            let message_response = receiver_server
                                .lock()
                                .unwrap()
                                .handle_message_request(sender_id, message_request.clone());

                            let handled_with_success =
                                sender_server.lock().unwrap().handle_message_response(
                                    *receiver_id,
                                    message_request,
                                    message_response.clone(),
                                );

                            // Because we want to keep the communication logic abstracted away from the RaftServer, we want to implement
                            // a retrying mechanism. This implementation is very simplistic and should require a better API which should leverage
                            // retriable objects that change their state after each retry.
                            if handled_with_success {
                                break;
                            } else {
                                println!(
                                    "The message response {:?} is not successful, retrying...",
                                    message_response
                                );
                                message_type = message_type.retry()
                            }
                        }
                    }
                })
            }
        });
    }
}
