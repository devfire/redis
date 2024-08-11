use crate::{
    actors::messages::ReplicatorActorMessage,
    protocol::{InfoCommandParameter, ReplicationSectionData},
};

use std::collections::HashMap;
use tokio::sync::mpsc;

use super::messages::HostId;

/// Handles INFO command. Receives message from the InfoCommandActorHandle and processes them accordingly.
pub struct ReplicatorActor {
    // The receiver for incoming messages
    receiver: mpsc::Receiver<ReplicatorActorMessage>,

    // The section-key-value hash map for storing data.
    // There are multiple sections, each has multiple keys, each key with one value.
    // Here is a Hash of Hashes. Replication -> Hash<ServerIP:Port,Offset> for example.
    // Note the special value of HostId::Myself that stores server's own data.
    kv_hash: HashMap<InfoCommandParameter, HashMap<HostId, ReplicationSectionData>>,
}

impl ReplicatorActor {
    // Constructor for the actor
    pub fn new(receiver: mpsc::Receiver<ReplicatorActorMessage>) -> Self {
        // Initialize the key-value hash map. The key is an enum of two types, dir and dbfilename.
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
        // Match on the type of the message
        match msg {
            // Handle a GetValue message
            ReplicatorActorMessage::GetInfoValue {
                info_key,
                host_id,
                respond_to,
            } => {
                // If the key exists in the hash map, send the value back
                if let Some(value) = self.kv_hash.get(&info_key) {
                    if let Some(value) = value.get(&host_id) {
                        let _ = respond_to.send(Some(value.clone()));
                    }
                } else {
                    // If the key does not exist in the hash map, send None
                    let _ = respond_to.send(None);
                }
                // If the key exists in the hash map, send the value back
                // debug!("Processing {:?}", msg);
            }
            ReplicatorActorMessage::SetInfoValue {
                info_key,
                host_id,
                info_value,
            } =>
            // Insert the key-value pair into the hash map
            {
                // this is a temp var to store the Hash
                let mut server_replication_data = HashMap::new();
                server_replication_data.insert(host_id, info_value);
                self.kv_hash.insert(info_key, server_replication_data);
            }
        }
    }
}
