use tokio::sync::{mpsc, oneshot};
use tracing::info;

use crate::{
    actors::{
        messages::{HostId, ReplicatorActorMessage},
        replicator::ReplicatorActor,
    },
    protocol::ReplicationSectionData,
    resp::value::RespValue,
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
        host_id: HostId, //hostIP:port combo
    ) -> Option<ReplicationSectionData> {
        tracing::debug!("Getting info value for key: {:?}", host_id);
        let (send, recv) = oneshot::channel();
        let msg = ReplicatorActorMessage::GetInfoValue {
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

    /// Stores sections for redis INFO command, taking a key, value pair as input. Returns nothing.
    /// https://redis.io/commands/info/
    pub async fn set_value(&self, host_id: HostId, replication_value: ReplicationSectionData) {
        let msg = ReplicatorActorMessage::SetInfoValue {
            // info_key,
            host_id,
            replication_value,
        };

        // debug!("Setting INFO key: {:?}, value: {}", info_key.clone(), info_value);
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

    /// Resets the number of replicas that are in sync.
    // pub async fn reset_synced_replica_count(&self) {
    //     let msg = ReplicatorActorMessage::ResetReplicaCount;

    //     // Ignore send errors. If this send fails, so does the
    //     // recv.await below. There's no reason to check the
    //     // failure twice.
    //     let _ = self.sender.send(msg).await;
    // }

    /// Update the replica's offset. Easier wrapper around set_value to avoid the whole
    /// get current offset, encode value, update offset everywhere.
    /// There's no ReplicatorActorMessage for this because it's just a wrapper.
    pub async fn update_master_offset(&self, writeable_cmd: &RespValue) {
        // // we need to update master's offset because we are sending writeable commands to replicas
        if let Some(mut current_replication_data) = self.get_value(HostId::Myself).await {
            // we need to convert the command to a RESP string to count the bytes.
            let value_as_string = writeable_cmd
                .to_encoded_string()
                .expect("Failed to encode replicated command");

            // calculate how many bytes are in the value_as_string
            let value_as_string_num_bytes = value_as_string.len() as i16;

            // extract the current offset value.
            let current_offset = current_replication_data.master_repl_offset;

            // update the offset value.
            let new_offset = current_offset + value_as_string_num_bytes;

            current_replication_data.master_repl_offset = new_offset;

            // update the offset value in the replication actor.
            self.set_value(HostId::Myself, current_replication_data).await;

            info!(
                "Current master offset: {} new offset: {}",
                current_offset, new_offset
            );
        }
        // This is a helper method, so it sends no messages but relies on set_value to serialize updates.
        // let _ = self.sender.send(msg).await;
    }
}
