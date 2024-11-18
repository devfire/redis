use crate::{
    actors::messages::ReplicatorActorMessage,
    protocol::{ReplicationSectionData, ServerRole},
};

use std::collections::HashMap;

use tokio::sync::mpsc;
use tracing::debug;

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
        let kv_hash = HashMap::new();

        // let replication_data: ReplicationSectionData = ReplicationSectionData {
        //     role: None,
        //     master_replid: None,
        //     master_repl_offset: Some(0),
        // };

        // initialize the offset to 0
        // kv_hash.insert(HostId::Myself, replication_data);

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
                    debug!("For {host_id} retrieved {value}");
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
                debug!("Updating for {host_id} {replication_value}");

                if let Some(offset_increment) = replication_value.master_repl_offset {
                    // we were passed an offset increment, let's update the existing value
                    debug!("Increasing offset by {offset_increment} for {host_id}");

                    match self.kv_hash.get_mut(&host_id) {
                        Some(replication_data) => {
                            // we have an entry already but may or may not have an offset
                            match replication_data.master_repl_offset.take() {
                                // we need to take() in order to avoid trying to
                                // directly manipulate the inner value of an Option, which is not allowed.
                                Some(current_offset) => {
                                    debug!("Current {host_id} offset is {current_offset}, increasing by {offset_increment}");
                                    // we have an offset, let's add the new one to this one
                                    replication_data.master_repl_offset =
                                        Some(current_offset + offset_increment);
                                }
                                None => {
                                    // we do not have an existing offset, just insert the offset_increment
                                    replication_data.master_repl_offset = Some(offset_increment);
                                }
                            }
                        }
                        None => {
                            // this is a new entry
                            let mut new_replication_entry = ReplicationSectionData::new();
                            new_replication_entry.master_repl_offset = Some(offset_increment);

                            // NOTE: All other entries are None because new() sets everything to None by default
                            self.kv_hash.insert(host_id.clone(), new_replication_entry);
                        }
                    }
                } else {
                    debug!("No offset passed for {host_id}.");
                }

                if let Some(replid) = replication_value.master_replid {
                    debug!("Setting replid {replid} for {host_id}");

                    match self.kv_hash.get_mut(&host_id) {
                        Some(replication_data) => {
                            // We have an entry but whether we have a replid already or not, doesn't matter,
                            // let's replace with the new one
                            replication_data.master_replid = Some(replid);
                        }
                        None => {
                            // this is a new entry
                            let mut new_replication_entry = ReplicationSectionData::new();
                            new_replication_entry.master_replid = Some(replid);

                            // NOTE: All other entries are None because new() sets everything to None by default
                            self.kv_hash.insert(host_id.clone(), new_replication_entry);
                        }
                    }
                }

                if let Some(new_role) = replication_value.role {
                    debug!("Setting role {new_role} for {host_id}");
                    match self.kv_hash.get_mut(&host_id) {
                        Some(replication_data) => {
                            // We have an entry but whether we have a role already or not, doesn't matter,
                            // let's replace with the new one
                            replication_data.role = Some(new_role);
                        }
                        None => {
                            // this is a new entry
                            let mut new_replication_entry = ReplicationSectionData::new();
                            new_replication_entry.role = Some(new_role);

                            // NOTE: All other entries are None because new() sets everything to None by default
                            self.kv_hash.insert(host_id.clone(), new_replication_entry);
                        }
                    }
                }

                // dump the contents of the hashmap to the console
                // debug!("AFTER UPDATE:kv_hash: {:#?}", self.kv_hash);

                // self.kv_hash.insert(host_id, replication_value);
            }
            ReplicatorActorMessage::GetReplicaCount { respond_to, target_offset } => {
                // first, let's get the master offset. It's ok to panic here because this should never fail.
                // if it were to fail, we can't proceed anyway.
                // let master_offset = self
                //     .kv_hash
                //     .get(&HostId::Myself)
                //     .expect("Something is wrong, expected to find master offset.")
                //     .master_repl_offset
                //     .expect("Expected master to have an offset, panic otherwise.") - 37; // -37 is REPLCONF GETACK *



                // dump the contents of the hashmap to the console
                // debug!("kv_hash: {:?}", self.kv_hash);

                tracing::info!("Looking for replicas with offset of {:?}", target_offset.max(0));

                // now, let's count how many replicas have this offset
                // Again, avoid counting HostId::Myself
                // let replica_count = self
                //     .kv_hash
                //     .iter()
                //     .filter(|(k, v)| {
                //         v.master_repl_offset.expect("Replicas must have offsets.")
                //             == master_offset.expect("Master must have an offset.")
                //             && **k != HostId::Myself
                //     })
                //     .count();

                let mut replica_count = 0;

                for (k, v) in self.kv_hash.iter() {
                    debug!("host: {k} value: {v}");
                    if let Some(my_role) = &v.role {
                        // we need to filter out redis-cli and other non replica clients.
                        // redis-cli will not have a role at all and master will be master which we can ignore
                        if *my_role == ServerRole::Slave {
                            // we are only counting slaves now
                            // next, let's check for offset
                            if let Some(slave_offset) = v.master_repl_offset {
                                tracing::info!("Comparing target offset {} with {} ", target_offset.max(0), slave_offset);
                                // ok, this replica does have an offset, let's compare
                                if slave_offset == target_offset.max(0) {
                                    replica_count += 1;
                                }
                            }
                        }
                    }
                }

                tracing::debug!("Final replica count: {replica_count}");
                let _ = respond_to.send(replica_count);
            }
            ReplicatorActorMessage::ResetReplicaOffset { host_id } => {
                self.kv_hash
                    .entry(host_id)
                    .and_modify(|replication_section_data| {
                        replication_section_data.reset_replica_offset();
                    });
            }
        }
    }
}
