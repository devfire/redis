// Inspired by https://ryhl.io/blog/actors-with-tokio/

use std::collections::HashMap;

// use anyhow::Error;
use log::info;

use tokio::sync::mpsc;

use crate::messages::SetActorMessage;

pub struct SetCommandActor {
    receiver: mpsc::Receiver<SetActorMessage>,
    kv_hash: HashMap<String, String>,
}

impl SetCommandActor {
    pub fn new(receiver: mpsc::Receiver<SetActorMessage>) -> Self {
        let kv_hash = HashMap::new();
        Self { receiver, kv_hash }
    }

    pub async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg);
        }
    }

    pub fn handle_message(&mut self, msg: SetActorMessage) {
        match msg {
            SetActorMessage::GetValue { key, respond_to } => {
                if let Some(value) = self.kv_hash.get(&key) {
                    let _ = respond_to.send(Some(value.clone()));
                } else {
                    // Move this to a proper error type
                    let _ = respond_to.send(None);
                }
            }
            SetActorMessage::SetValue { input } => {
                self.kv_hash.insert(input.key, input.value);
                info!("Successfully inserted kv pair.");
            }
            SetActorMessage::ExpireValue { expiry } => {
                info!("Expiring {:?}", expiry);
                self.kv_hash.remove(&expiry.key);
            }
        }
    }
}
