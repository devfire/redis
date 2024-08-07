use std::collections::HashMap;

use tokio::sync::mpsc;
// use tracing::{debug, info};

use crate::protocol::{ReplicationDataStore, ReplicationParameter};

use super::messages::ReplicationActorMessage;

pub struct ReplicatorActor {
    // The receiver for incoming messages
    receiver: mpsc::Receiver<ReplicationActorMessage>,

    // The section-key-value hash map for storing data.
    // There are multiple sections, each has multiple keys, each key with one value.
    kv_hash: HashMap<ReplicationParameter, ReplicationDataStore>,
}

impl ReplicatorActor {
    // Constructor for the actor
    pub fn new(receiver: mpsc::Receiver<ReplicationActorMessage>) -> Self {
        let kv_hash = HashMap::new();

        // Return a new actor
        Self { receiver, kv_hash }
    }

    // Run the actor
    pub async fn run(&mut self) {
        // Continuously receive messages and handle them
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }

    // Handle a message.
    pub async fn handle_message(&mut self, msg: ReplicationActorMessage) {
        // Match on the type of the message

        match msg {
            ReplicationActorMessage::GetReplicationValue {
                replication_key,
                respond_to,
            } => {
                // If the key exists in the hash map, send the value back
                if let Some(value) = self.kv_hash.get(&replication_key) {
                    let _ = respond_to.send(Some(value.clone()));
                } else {
                    // If the key does not exist in the hash map, send None
                    let _ = respond_to.send(None);
                }
            }
            ReplicationActorMessage::SetReplicationValue {
                replication_key,
                replication_value,
            } => {
                // Insert the key-value pair into the hash map
                {
                    self.kv_hash.insert(replication_key, replication_value);
                }
            }
        }
    }
}
