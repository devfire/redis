use tokio::sync::mpsc;

use crate::{
    actors::{messages::ReplicationActorMessage, replicator::ReplicatorActor},
    protocol::{ReplicationDataStore, ReplicationParameter},
};

#[derive(Clone)]
pub struct ReplicationActorHandle {
    sender: mpsc::Sender<ReplicationActorMessage>,
}

// Gives you access to the underlying actor.
impl ReplicationActorHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = ReplicatorActor::new(receiver);

        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    /// Stores replication settings, taking a (String of serverIP:Port, value) pair as key. Returns nothing.
    /// https://redis.io/commands/info/
    pub async fn set_value(&self, replication_key: ReplicationParameter, replication_value: ReplicationDataStore) {
        let msg = ReplicationActorMessage::SetReplicationValue {
            replication_key,
            replication_value,
        };

        // debug!("Setting INFO key: {:?}, value: {}", info_key.clone(), info_value);
        // Ignore send errors.
        let _ = self.sender.send(msg).await.expect("Failed to set value.");
    }
}
