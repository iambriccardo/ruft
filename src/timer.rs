use std::{
    borrow::Borrow,
    collections::HashMap,
    mem,
    sync::{
        mpsc::{self, Sender},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use crate::{
    core::{PrepareMessageType, RaftServerInstance, ServerId},
    transport::{Transport, TransportInstance},
};

pub type TimerId = u32;

#[derive(Debug, Clone, Copy)]
pub enum TimerAction {
    SWITCH_TO_CANDIDATE,
    SEND_EMPTY_APPEND_ENTRIES,
}

pub struct TimersManager {
    server: RaftServerInstance,
    transport: TransportInstance,
    active_timers: HashMap<TimerId, (JoinHandle<()>, Sender<()>)>,
    next_id: TimerId,
}

impl TimersManager {
    pub fn new(server: RaftServerInstance, transport: TransportInstance) -> TimersManager {
        TimersManager {
            server,
            transport,
            active_timers: HashMap::new(),
            next_id: 0,
        }
    }

    pub fn register(&mut self, sleep: Duration, timer_action: TimerAction) -> u32 {
        let server = self.server.clone();
        let transport = self.transport.clone();
        // TODO: maybe we want to directly store the id, to avoid having to lock the server.

        let (tx, rx): (mpsc::Sender<()>, mpsc::Receiver<()>) = mpsc::channel();
        let handle = thread::spawn(move || loop {
            if rx.try_recv().is_ok() {
                break;
            }

            thread::sleep(sleep);
            match timer_action {
                TimerAction::SWITCH_TO_CANDIDATE => server
                    .lock()
                    .unwrap()
                    .change_role(crate::core::ServerRole::CANDIDATE),
                TimerAction::SEND_EMPTY_APPEND_ENTRIES => {
                    println!("REGISTER");
                    let server_id = server.lock().unwrap().id;
                    println!("LOCK");
                    transport
                        .lock()
                        .unwrap()
                        .broadcast(server_id, PrepareMessageType::EMPTY)
                }
            }
        });

        let id = self.next_id;
        self.active_timers.insert(id, (handle, tx));
        self.next_id += 1;

        return id;
    }

    pub fn stop(&mut self, timer_id: TimerId) {
        if let Some((_, tx)) = self.active_timers.get(&timer_id) {
            tx.send(()).unwrap();
        }
    }

    pub fn join_all(&mut self) {
        let active_timers = mem::replace(&mut self.active_timers, HashMap::new());
        active_timers.into_iter().for_each(|(_, (timer, _))| {
            timer.join();
        });
    }
}
