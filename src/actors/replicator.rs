use crate::{actors::messages::ReplicatorActorMessage, protocol::ReplicationSectionData};

use std::collections::HashMap;
use tokio::sync::mpsc;

use super::messages::HostId;

/// Handles INFO command. Receives message from the InfoCommandActorHandle and processes them accordingly.
pub struct ReplicatorActor {
    // The receiver for incoming messages
    receiver: mpsc::Receiver<ReplicatorActorMessage>,

    // The section-key-value hash map for storing data.
    // There are multiple sections, each has multiple keys, each key with one value.
    // Hash<ServerIP:Port,Offset> for example.
    // Note the special value of HostId::Myself that stores server's own data.
    kv_hash: HashMap<HostId, ReplicationSectionData>,
}

impl ReplicatorActor {
    // Constructor for the actor
    pub fn new(receiver: mpsc::Receiver<ReplicatorActorMessage>) -> Self {
        // Initialize the key-value hash map.
        let kv_hash = HashMap::new();

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
        tracing::info!("Handling message: {:?}", msg);

        // Match on the type of the message
        match msg {
            // Handle a GetValue message
            ReplicatorActorMessage::GetInfoValue {
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
            ReplicatorActorMessage::SetInfoValue {
                host_id,
                replication_value: info_value,
            } =>
            // Insert the key-value pair into the hash map
            {
                // this is a temp var to store the Hash
                // let mut server_replication_data = HashMap::new();
                // server_replication_data.insert(host_id, info_value);
                self.kv_hash.insert(host_id, info_value);
            }
            ReplicatorActorMessage::GetReplicaCount { respond_to } => {
                // we need to -1 because Host::Myself doesn't count, and
                // we need to return 0 if there are no replicas to avoid returning 0-1=-1

                // first, let's get the master offset
                let master_offset = self.kv_hash.get(&HostId::Myself)
                .expect("Something is wrong, no master offset found")
                    .master_repl_offset;

                // now, let's count how many replicas have this offset
                let replica_count = self.kv_hash.iter().filter(|(_, v)| v.master_repl_offset == master_offset).count();

                let _ = respond_to.send(replica_count);
            }
        }
    }
}
