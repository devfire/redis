// Inspired by https://ryhl.io/blog/actors-with-tokio/

use std::collections::HashMap;

// use anyhow::Error;
use log::info;
use resp::Value;
use tokio::sync::mpsc;

use crate::messages::ActorMessage;

pub struct SetCommandActor {
    receiver: mpsc::Receiver<ActorMessage>,
    kv_hash: HashMap<String, Value>,
}

impl SetCommandActor {
    pub fn new(receiver: mpsc::Receiver<ActorMessage>) -> Self {
        let kv_hash = HashMap::new();
        Self { receiver, kv_hash }
    }

    pub async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg);
        }
    }

    pub fn handle_message(&mut self, msg: ActorMessage) {
        match msg {
            ActorMessage::GetValue { key, respond_to } => {
                if let Some(value) = self.kv_hash.get(&key) {
                    let _ = respond_to.send(value.clone());
                } else {
                    let _ = respond_to.send(resp::Value::Error("Key not found".to_string()));
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

