use std::{
    collections::HashMap,
    rc::Rc,
    sync::{Arc, Mutex},
    thread,
};

use crate::core::{PrepareMessageType, RaftServer, RaftServerInstance, ServerId};

pub type TransportInstance = Arc<Mutex<Transport>>;

pub struct Transport {
    servers: Arc<Mutex<HashMap<ServerId, RaftServerInstance>>>,
}

impl Transport {
    pub fn new() -> Transport {
        Transport {
            servers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn new_instance() -> TransportInstance {
        Arc::new(Mutex::new(Self::new()))
    }

    pub fn add_server(&mut self, server: RaftServerInstance) {
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
