// Import necessary modules and types
use crate::protocol::RedisCommand;





// use log::info;

use resp::Value;
use tokio::sync::mpsc;

/// Handles CONFIG command. Receives message from the ConfigCommandActorHandle and processes them accordingly.
pub struct ProcessActor {
    // The receiver for incoming messages
    receiver: mpsc::Receiver<RedisCommand>,

    // The key-value hash map for storing data
    // kv_hash: HashMap<String, String>,
}

impl ProcessActor {
    // Constructor for the actor
    pub fn new(receiver: mpsc::Receiver<RedisCommand>) -> Self {
        // Initialize the key-value hash map. The key is an enum of two types, dir and dbfilename.
        // let kv_hash = HashMap::new();

        // Return a new actor with the given receiver and an empty key-value hash map
        Self { receiver }
    }

    // Run the actor
    pub async fn run(&mut self) {
        // Continuously receive messages and handle them
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg);
        }
    }

    // Handle a message
    pub async fn handle_message(&mut self, msg: RedisCommand) -> Vec<u8>{
        // Match on the type of the message
        
    }
}

