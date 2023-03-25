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
        self.propagate_membership_change();
        server
            .lock()
            .unwrap()
            .bind(self.servers.lock().unwrap().len() as u32);
        self.servers
            .lock()
            .unwrap()
            .insert(server.lock().unwrap().id, server.clone());
    }

    pub fn propagate_membership_change(&mut self) {
        self.servers
            .lock()
            .unwrap()
            .iter()
            .for_each(|(_, server)| server.lock().unwrap().bind(1))
    }

    pub fn broadcast(&mut self, sender_id: ServerId, message_type: PrepareMessageType) {
        let servers = self.servers.clone();
        thread::spawn(move || {
            let servers = servers.lock().unwrap();
            if let Some(sender_server) = servers.get(&sender_id) {
                servers.iter().for_each(|(receiver_id, receiver_server)| {
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
        });
    }
}
