use crate::actors::messages::ProcessorActorMessage;

use tokio::sync::mpsc;

/// Handles CONFIG command. Receives message from the ProcessorActorHandle and processes them accordingly.
pub struct ProcessorActor {
    // The receiver for incoming messages
    receiver: mpsc::Receiver<ProcessorActorMessage>,
}

impl ProcessorActor {
    // Constructor for the actor
    pub fn new(receiver: mpsc::Receiver<ProcessorActorMessage>) -> Self {
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

    // Handle a message.
    pub fn handle_message(&mut self, msg: ProcessorActorMessage) {
        // Match on the type of the message
        match msg {
            // Handle a GetValue message
            ProcessorActorMessage::Process { request } => {
                // If the key exists in the hash map, send the value back
                todo!()
            }
        }
    }
}
