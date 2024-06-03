use crate::{actors::messages::InfoActorMessage, protocol::InfoCommandParameter};
use log::{debug, error, info};

use std::collections::HashMap;
use tokio::sync::mpsc;

/// Handles CONFIG command. Receives message from the InfoCommandActorHandle and processes them accordingly.
pub struct InfoCommandActor {
    // The receiver for incoming messages
    receiver: mpsc::Receiver<InfoActorMessage>,

    // The key-value hash map for storing data
    kv_hash: HashMap<InfoCommandParameter, String>,
}

impl InfoCommandActor {
    // Constructor for the actor
    pub fn new(receiver: mpsc::Receiver<InfoActorMessage>) -> Self {
        // Initialize the key-value hash map. The key is an enum of two types, dir and dbfilename.
        let kv_hash = HashMap::new();

        // Return a new actor with the given receiver and an empty key-value hash map
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
    // NOTE: This is an async function due to TCP connect. The others are not async.
    pub async fn handle_message(&mut self, msg: InfoActorMessage) {
        // Match on the type of the message
        match msg {
            // Handle a GetValue message
            InfoActorMessage::GetInfoValue { key, respond_to } => {
                let _ = respond_to.send(Some("None".to_string()));
                // If the key exists in the hash map, send the value back
                // info!("Processing {:?}", msg);
            }
            InfoActorMessage::SetInfoValue {
                info_key,
                info_value,
            } =>
            // Insert the key-value pair into the hash map
            {
                self.kv_hash.insert(info_key, info_value);
            }
        }
    }
}
