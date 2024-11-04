use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info};

use crate::{
    actors::{
        messages::{HostId, ReplicatorActorMessage},
        replicator::ReplicatorActor,
    },
    protocol::ReplicationSectionData,
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
    /// https://redis.io/commands/replication/
    pub async fn get_value(
        &self,
        host_id: HostId, //hostIP:port combo
    ) -> Option<ReplicationSectionData> {
        debug!("Getting info value for key: {:?}", host_id);
        let (send, recv) = oneshot::channel();
        let msg = ReplicatorActorMessage::GetReplicationValue {
            // info_key,
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

    /// Resets the master's current replica tracked offset to 0.
    pub async fn reset_replica_offset(&self, host_id: HostId) {
        let msg = ReplicatorActorMessage::ResetReplicaOffset { host_id };
        // Ignore send errors.
        let _ = self
            .sender
            .send(msg)
            .await
            .expect("Should have reset the replication value.");
    }

    /// Updates sections for redis REPLICATION command, taking a key, value pair as input. Returns nothing.
    /// https://redis.io/commands/replication/
    pub async fn update_value(
        &self,
        // info_key: InfoCommandParameter,
        host_id: HostId,
        replication_value: ReplicationSectionData,
    ) {
        info!(
            "HANDLER: Setting REPLICATION key: {:?}, value: {}",
            host_id, replication_value
        );
        let msg = ReplicatorActorMessage::UpdateReplicationValue {
            // info_key,
            host_id,
            replication_value,
        };

        // Ignore send errors.
        let _ = self.sender.send(msg).await.expect("Failed to set value.");
    }

    /// Returns the number of replicas that are in sync.
    pub async fn get_synced_replica_count(&self) -> usize {
        let (send, recv) = oneshot::channel();
        let msg = ReplicatorActorMessage::GetReplicaCount { respond_to: send };

        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check the
        // failure twice.
        let _ = self.sender.send(msg).await;

        // this is going back once the msg comes back from the actor.
        // NOTE: we might get None back, i.e. no value for the given key.
        recv.await.expect("Actor task has been killed")
    }
}
