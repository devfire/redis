use tokio::sync::mpsc;

use crate::{actors::process::ProcessActor, protocol::RedisCommand};

#[derive(Clone)]
pub struct ProcessActorHandle {
    sender: mpsc::Sender<RedisCommand>,
}

// Gives you access to the underlying actor.
impl ProcessActorHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = ProcessActor::new(receiver);
        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    /// Processes a valid redis command. Returns nothing.
    pub async fn process_value(&self, command: RedisCommand) {
        // Ignore send errors.
        let _ = self
            .sender
            .send(command)
            .await
            .expect("Failed to process value.");
    }
}
