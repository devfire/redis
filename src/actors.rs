// Inspired by https://ryhl.io/blog/actors-with-tokio/

use std::collections::HashMap;

use log::info;
use tokio::sync::{mpsc, oneshot};

struct SetCommandActor {
    receiver: mpsc::Receiver<ActorMessage>,
    kv_hash: HashMap<String, String>,
}

impl SetCommandActor {
    fn new(receiver: mpsc::Receiver<ActorMessage>, kv_hash: HashMap<String, String>) -> Self {
        let mut kv_hash = HashMap::new();
        Self { receiver, kv_hash }
    }

    fn handle_message(&mut self, msg: ActorMessage) {
        match msg {
            ActorMessage::GetValue { key, respond_to } => {
                if let Some(value) = self.kv_hash.get(&key) {
                    let _ = respond_to.send(value.clone());
                } else {
                    let _ = respond_to.send(String::from("No such key exists"));
                }
            }
            ActorMessage::SetValue { input_kv } => {
                if let Some(_) = self.kv_hash.insert(input_kv.0, input_kv.1) {
                    info!("Successfully inserted kv pair.");
                }
            }
        }
    }
}

enum ActorMessage {
    GetValue {
        key: String,
        respond_to: oneshot::Sender<String>,
    },
    SetValue {
        input_kv: (String, String),
    }, // tuple
}
