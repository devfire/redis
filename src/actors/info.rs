use crate::{actors::messages::InfoActorMessage, protocol::{InfoCommandParameter, InfoSectionData}};

use std::collections::HashMap;
use tokio::sync::mpsc;

/// Handles INFO command. Receives message from the InfoCommandActorHandle and processes them accordingly.
pub struct InfoCommandActor {
    // The receiver for incoming messages
    receiver: mpsc::Receiver<InfoActorMessage>,

    // The section-key-value hash map for storing data.
    // There are multiple sections, each has multiple keys, each key with one value.
    kv_hash: HashMap<InfoCommandParameter, InfoSectionData>,
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
            self.handle_message(msg);
        }
    }

    // Handle a message.
    pub fn handle_message(&mut self, msg: InfoActorMessage) {
        // Match on the type of the message
        match msg {
            // Handle a GetValue message
            InfoActorMessage::GetInfoValue {
                info_key,
                respond_to,
            } => {
                // If the key exists in the hash map, send the value back
                if let Some(value) = self.kv_hash.get(&info_key) {
                    let _ = respond_to.send(Some(value.clone()));
                } else {
                    // If the key does not exist in the hash map, send None
                    let _ = respond_to.send(None);
                }
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
