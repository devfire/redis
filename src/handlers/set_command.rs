use tokio::sync::{mpsc, oneshot};

use crate::{actors::SetCommandActor, messages::SetActorMessage, protocol::SetCommandParameters};

#[derive(Clone)]
pub struct SetCommandActorHandle {
    sender: mpsc::Sender<SetActorMessage>,
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
    pub async fn get_value(&self, key: &str) -> Option<String> {
        let (send, recv) = oneshot::channel();
        let msg = SetActorMessage::GetValue {
            key: key.to_string(),
            respond_to: send,
        };

        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check the
        // failure twice.
        let _ = self.sender.send(msg).await;

        // this is going back once the msg comes back from the actor.
        // NOTE: we might get None back, i.e. no value for the given key.
        if let Some(value) = recv.await.expect("Actor task has been killed") {
            Some(value)
        } else {
            None
        }
    }

    /// implements the redis SET command, taking a key, value pair as input. Returns nothing.
    pub async fn set_value(&self, parameters: SetCommandParameters) {
        let msg = SetActorMessage::SetValue {
            input: parameters.clone(),
        };

        // Ignore send errors.
        let _ = self.sender.send(msg).await.expect("Failed to set value.");
    }

    /// implements immediate removal of keys. This is triggered by a tokio::spawn sleep thread in main.rs
    pub async fn expire_value(&self, key: &String) {
        let msg = SetActorMessage::ExpireValue {
            expiry: key.clone(),
        };

        // Ignore send errors.
        let _ = self.sender.send(msg).await.expect("Failed to set value.");
    }
}
