use log::info;
use tokio::sync::{mpsc, oneshot};

use crate::{
    actors::{info::InfoCommandActor, messages::InfoActorMessage},
    protocol::{InfoCommandOption, InfoCommandParameter},
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

    /// implements the redis CONFIG GET command, taking a key as input and returning a value.
    /// https://redis.io/commands/config-get/
    pub async fn get_value(&self, key: InfoCommandParameter) -> Option<String> {
        log::info!("Getting value for key: {:?}", key);
        let (send, recv) = oneshot::channel();
        let msg = InfoActorMessage::GetInfoValue {
            key,
            respond_to: todo!(),
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

        /// implements the redis CONFIG SET command, taking a key, value pair as input. Returns nothing.
    /// https://redis.io/commands/config-set/
    pub async fn set_value(&self, info_key: InfoCommandOption, info_value: &str) {
        let msg = InfoCommandOption::SetInfoValue {
            info_key,
            info_value,
        };

        info!(
            "Setting value for key: {:?}, value: {}",
            info_key, info_value
        );
        // Ignore send errors.
        let _ = self.sender.send(msg).await.expect("Failed to set value.");
    }

}
