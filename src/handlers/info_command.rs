use log::info;
use tokio::sync::{mpsc, oneshot};

use crate::{
    actors::{info::InfoCommandActor, messages::InfoActorMessage},
    protocol::InfoCommandParameter,
};

#[derive(Clone)]
pub struct InfoCommandActorHandle {
    sender: mpsc::Sender<InfoActorMessage>,
}

// Gives you access to the underlying actor.
impl InfoCommandActorHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = InfoCommandActor::new(receiver);

        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    /// Gets sections from INFO command, taking a key as input and returning a value.
    /// https://redis.io/commands/config-get/
    pub async fn get_value(&self, key: InfoCommandParameter) -> Option<String> {
        log::info!("Getting value for key: {:?}", key);
        let (send, recv) = oneshot::channel();
        let msg = InfoActorMessage::GetInfoValue {
            info_key: key,
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

    /// Stores sections for redis INFO command, taking a key, value pair as input. Returns nothing.
    /// https://redis.io/commands/info/
    pub async fn set_value(&self, info_key: InfoCommandParameter, info_value: &str) {
        let msg = InfoActorMessage::SetInfoValue {
            info_key,
            info_value: info_value.to_owned(),
        };

        info!(
            "Setting value for key: {:?}, value: {}",
            info_key, info_value
        );
        // Ignore send errors.
        let _ = self.sender.send(msg).await.expect("Failed to set value.");
    }
}
