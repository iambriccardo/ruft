use std::{
    collections::HashMap,
    mem,
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
    time::Duration,
};

use crate::{
    core::{PrepareMessageType, ServerId},
    transport::Transport,
};

pub type TimerId = u32;

pub struct ServerTimers {
    server_id: ServerId,
    transport: Arc<Mutex<Transport>>,
    active_timers: HashMap<TimerId, JoinHandle<()>>,
    next_id: TimerId,
}

impl ServerTimers {
    pub fn new(server_id: ServerId, transport: Arc<Mutex<Transport>>) -> ServerTimers {
        ServerTimers {
            server_id,
            transport,
            active_timers: HashMap::new(),
            next_id: 0,
        }
    }

    pub fn register(&mut self, sleep: Duration, message_type: PrepareMessageType) {
        let transport = self.transport.clone();
        let server_id = self.server_id;

        self.active_timers.insert(
            self.next_id,
            thread::spawn(move || loop {
                thread::sleep(sleep);
                transport.lock().unwrap().broadcast(server_id, message_type);
            }),
        );
    }

    pub fn join_all(&mut self) {
        let active_timers = mem::replace(&mut self.active_timers, HashMap::new());
        active_timers.into_iter().for_each(|(_, timer)| {
            timer.join();
        });
    }
}
