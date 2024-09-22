use crate::{
    actors::messages::ReplicatorActorMessage,
    protocol::ReplicationSectionData,
};

use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::info;

use super::messages::HostId;

/// Handles INFO command. Receives message from the InfoCommandActorHandle and processes them accordingly.
pub struct ReplicatorActor {
    // The receiver for incoming messages
    receiver: mpsc::Receiver<ReplicatorActorMessage>,

    // Note the special value of HostId::Myself that stores server's own data.
    kv_hash: HashMap<HostId, ReplicationSectionData>,
}

impl ReplicatorActor {
    // Constructor for the actor
    pub fn new(receiver: mpsc::Receiver<ReplicatorActorMessage>) -> Self {
        // Initialize the key-value hash map.
        let mut kv_hash = HashMap::new();

        let replication_data: ReplicationSectionData = ReplicationSectionData {
            role: None,
            master_replid: None,
            master_repl_offset: Some(0),
        };

        // initialize the offset to 0
        kv_hash.insert(HostId::Myself, replication_data);

        // Return a new actor with the given receiver and an empty key-value hash map
        Self { receiver, kv_hash }
    }

    // Run the actor
    pub async fn run(&mut self) {
        // Continuously receive messages and handle them
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg);
        }
    }

    // Handle a message.
    pub fn handle_message(&mut self, msg: ReplicatorActorMessage) {
        tracing::debug!("Handling message: {:?}", msg);

        // Match on the type of the message
        match msg {
            // Handle a GetValue message
            ReplicatorActorMessage::GetReplicationValue {
                // info_key,
                host_id,
                respond_to,
            } => {
                // If the key exists in the hash map, send the value back

                if let Some(value) = self.kv_hash.get(&host_id) {
                    let _ = respond_to.send(Some(value.clone()));
                } else {
                    let _ = respond_to.send(None);
                }

                // If the key exists in the hash map, send the value back
                // debug!("Processing {:?}", msg);
            }
            // This updates the values in place.
            // Allows for partial update of either replID or offset or role.
            // Avoids the messy get-change-update loop causing race conditions.
            ReplicatorActorMessage::UpdateReplicationValue {
                host_id,
                replication_value,
            } =>
            // Updating the key-value pair in place
            {
                if let Some(offset_increment) = replication_value.master_repl_offset {
                    info!("Increasing offset by {offset_increment}");
                    self.kv_hash
                        .entry(host_id)
                        .and_modify(|replication_section_data| {
                            replication_section_data.increment_offset(offset_increment);
                        });
                }

                // self.kv_hash.insert(host_id, replication_value);
            }
            ReplicatorActorMessage::GetReplicaCount { respond_to } => {
                // first, let's get the master offset. It's ok to panic here because this should never fail.
                // if it were to fail, we can't proceed anyway.
                let master_offset = self
                    .kv_hash
                    .get(&HostId::Myself)
                    .expect("Something is wrong, expected to find master offset.")
                    .master_repl_offset;

                // now, let's count how many replicas have this offset
                // Again, avoid counting HostId::Myself
                let replica_count = self
                    .kv_hash
                    .iter()
                    .filter(|(k, v)| v.master_repl_offset == master_offset && **k != HostId::Myself)
                    .count();

                let _ = respond_to.send(replica_count);
            }
            ReplicatorActorMessage::ResetReplicaOffset {
                host_id,
            } => {
                self.kv_hash
                    .entry(host_id)
                    .and_modify(|replication_section_data| {
                        replication_section_data.reset_replica_offset();
                    });
            }
        }
    }
}
