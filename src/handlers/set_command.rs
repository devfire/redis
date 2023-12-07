use resp::Value;
use tokio::sync::{mpsc, oneshot};

use crate::{actors::SetCommandActor, messages::ActorMessage};

#[derive(Clone)]
pub struct SetCommandActorHandle {
    sender: mpsc::Sender<ActorMessage>,
}

// Gives you access to the underlying actor.
impl SetCommandActorHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = SetCommandActor::new(receiver);
        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    /// implements the redis GET command, taking a key as input and returning a value.
    /// https://redis.io/commands/get/
    pub async fn get_value(&self, key: &str) -> Value {
        let (send, recv) = oneshot::channel();
        let msg = ActorMessage::GetValue {
            key: key.to_string(),
            respond_to: send,
        };

        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check the
        // failure twice.
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    /// implements the redis SET command, taking a key, value pair as input. Returns nothing.
    pub async fn set_value(&self, key_value_pair: (String, Value)) {
        let msg = ActorMessage::SetValue {
            input_kv: key_value_pair,
        };

        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check the
        // failure twice.
        let _ = self.sender.send(msg).await.expect("Failed to set value.");
    }
}
