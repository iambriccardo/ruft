use std::{
    collections::HashMap,
    rc::Rc,
    sync::{Arc, Mutex},
};

use crate::core::{PrepareMessageType, Server, ServerId};

pub type ServerInstance = Arc<Mutex<Server>>;

pub struct Transport {
    servers: HashMap<ServerId, ServerInstance>,
}

impl Transport {
    pub fn new() -> Transport {
        Transport {
            servers: HashMap::new(),
        }
    }

    pub fn add_server(&mut self, mut server: Server) {
        self.propagate_membership_change();
        server.bind(self.servers.len() as u32);
        self.servers.insert(server.id, Arc::new(Mutex::new(server)));
    }

    pub fn propagate_membership_change(&mut self) {
        self.servers
            .iter()
            .for_each(|(_, server)| server.lock().unwrap().bind(1))
    }

    pub fn broadcast(&self, sender_id: ServerId, message_type: PrepareMessageType) {
        if let Some(sender_server) = self.servers.get(&sender_id) {
            self.servers
                .iter()
                .for_each(|(receiver_id, receiver_server)| {
                    if *receiver_id != sender_id {
                        let message_request = sender_server
                            .lock()
                            .unwrap()
                            .prepare_message_request(message_type);

                        let message_response = receiver_server
                            .lock()
                            .unwrap()
                            .handle_message_request(sender_id, message_request);

                        sender_server
                            .lock()
                            .unwrap()
                            .handle_message_response(*receiver_id, message_response)
                    }
                })
        }
    }
}
