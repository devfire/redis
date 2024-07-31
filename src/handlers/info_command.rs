use tokio::sync::{mpsc, oneshot};

use crate::{
    actors::{info::InfoCommandActor, messages::InfoActorMessage},
    protocol::{InfoCommandParameter, ReplicationSectionData},
};

#[derive(Clone, Debug)]
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
    pub async fn get_value(
        &self,
        info_key: InfoCommandParameter,
    ) -> Option<ReplicationSectionData> {
        tracing::debug!("Getting info value for key: {:?}", info_key);
        let (send, recv) = oneshot::channel();
        let msg = InfoActorMessage::GetInfoValue {
            info_key,
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
    pub async fn set_value(
        &self,
        info_key: InfoCommandParameter,
        info_value: ReplicationSectionData,
    ) {
        let msg = InfoActorMessage::SetInfoValue {
            info_key,
            info_value,
        };

        // debug!("Setting INFO key: {:?}, value: {}", info_key.clone(), info_value);
        // Ignore send errors.
        let _ = self.sender.send(msg).await.expect("Failed to set value.");
    }
}
