use tokio::sync::{mpsc, oneshot};

use crate::{
    actors::{messages::{HostId, ReplicatorActorMessage}, replicator::ReplicatorActor},
    protocol::{InfoCommandParameter, ReplicationSectionData},
};

#[derive(Clone, Debug)]
pub struct ReplicationActorHandle {
    sender: mpsc::Sender<ReplicatorActorMessage>,
}

// Gives you access to the underlying actor.
impl ReplicationActorHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = ReplicatorActor::new(receiver);

        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    /// Gets sections from INFO REPLICATION command, taking a key as input and returning a value.
    /// https://redis.io/commands/info/
    pub async fn get_value(
        &self,
        info_key: InfoCommandParameter,
        host_id: HostId, //hostIP:port combo
    ) -> Option<ReplicationSectionData> {
        tracing::debug!("Getting info value for key: {:?}", info_key);
        let (send, recv) = oneshot::channel();
        let msg = ReplicatorActorMessage::GetInfoValue {
            info_key,
            host_id,
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
        host_id: HostId,
        info_value: ReplicationSectionData,
    ) {
        let msg = ReplicatorActorMessage::SetInfoValue {
            info_key,
            host_id,
            info_value,
        };

        // debug!("Setting INFO key: {:?}, value: {}", info_key.clone(), info_value);
        // Ignore send errors.
        let _ = self.sender.send(msg).await.expect("Failed to set value.");
    }
}
