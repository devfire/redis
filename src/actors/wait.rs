// Import necessary modules and types
use crate::actors::messages::WaitActorMessage;
use std::collections::HashMap;
use tokio::sync::mpsc;

/// Handles redis SET command. Receives message from the SetCommandActorHandle and processes them accordingly.
pub struct WaitCommandActor {
    // The receiver for incoming messages
    receiver: mpsc::Receiver<WaitActorMessage>,

    // // channel for key expiration
    // expiry_channel: mpsc::Receiver<String>,

    // The key-value hash map for storing data
    kv_hash: HashMap<String, String>,
}

impl WaitCommandActor {
    // Constructor for the actor
    pub fn new(receiver: mpsc::Receiver<WaitActorMessage>) -> Self {
        // Initialize the key-value hash map
        let kv_hash = HashMap::new();

        // Return a new actor with the given receiver and an empty key-value hash map
        Self {
            receiver,
            // expiry_channel,
            kv_hash,
        }
    }

    // Run the actor
    pub async fn run(&mut self) {
        // Continuously receive messages and handle them
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg);
        }
    }

    // Handle a message
    pub fn handle_message(&mut self, msg: WaitActorMessage) {
        // Match on the type of the message
        match msg {
            // Handle a GetValue message
            WaitActorMessage::GetReplicas { key, respond_to } => {
                // If the key exists in the hash map, send the value back
                if let Some(value) = self.kv_hash.get(&key) {
                    let _ = respond_to.send(Some(value.clone()));
                } else {
                    // If the key does not exist in the hash map, send None
                    let _ = respond_to.send(None);
                }
            }

        }
    }
}
