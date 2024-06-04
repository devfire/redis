use crate::protocol::SetCommandParameter;
use crate::rdb::codec::RdbCodec;

use crate::rdb::format::Rdb::KeyValuePair;
// Import necessary modules and types
use crate::{actors::messages::ConfigActorMessage, protocol::ConfigCommandParameter};
// use bytes::Buf;
// use futures_util::io::BufReader;

use futures::StreamExt;
use log::{debug, error, info};
// use resp::Value;
use tokio::fs::File;
// use tokio::io::AsyncWriteExt;
use tokio_util::codec::FramedRead;

use std::{collections::HashMap, path::Path};

// use tokio::net::TcpStream;
use tokio::sync::mpsc;

// include the format.rs file from rdb
// use crate::rdb::format;

/// Handles CONFIG command. Receives message from the ConfigCommandActorHandle and processes them accordingly.
pub struct ConfigCommandActor {
    // The receiver for incoming messages
    receiver: mpsc::Receiver<ConfigActorMessage>,

    // The key-value hash map for storing data
    kv_hash: HashMap<ConfigCommandParameter, String>,
}

impl ConfigCommandActor {
    // Constructor for the actor
    pub fn new(receiver: mpsc::Receiver<ConfigActorMessage>) -> Self {
        // Initialize the key-value hash map. The key is an enum of two types, dir and dbfilename.
        let kv_hash = HashMap::new();

        // Return a new actor with the given receiver and an empty key-value hash map
        Self { receiver, kv_hash }
    }

    // Run the actor
    pub async fn run(&mut self) {
        // Continuously receive messages and handle them
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }

    // Handle a message.
    // NOTE: This is an async function due to TCP connect. The others are not async.
    pub async fn handle_message(&mut self, msg: ConfigActorMessage) {
        // Match on the type of the message
        match msg {
            // Handle a GetValue message
            ConfigActorMessage::GetConfigValue {
                config_key,
                respond_to,
            } => {
                // If the key exists in the hash map, send the value back
                if let Some(value) = self.kv_hash.get(&config_key) {
                    let _ = respond_to.send(Some(value.clone()));
                } else {
                    // If the key does not exist in the hash map, send None
                    let _ = respond_to.send(None);
                }
            }

            // Handle a SetValue message
            ConfigActorMessage::SetConfigValue {
                config_key,
                config_value,
            } => {
                // Insert the key-value pair into the hash map
                self.kv_hash.insert(config_key, config_value);
            }

            ConfigActorMessage::LoadConfig {
                dir,
                dbfilename,
                set_command_actor_handle,
                expire_tx,
            } => {
                let fullpath = format!("{}/{}", dir, dbfilename);

                // check to see if the file exists.
                if !Path::new(&fullpath).exists() {
                    log::error!("Config file does not exist.");
                } else {
                    // file exists, let's proceed.
                    // Log the attempt
                    info!("Loading config {}", fullpath);

                    let rdb_file = File::open(fullpath)
                        .await
                        .expect("Failed to open RDB file.");

                    // stream the rdb file, decoding and parsing the saved entries.
                    let mut rdb_file_stream_reader = FramedRead::new(rdb_file, RdbCodec::new());

                    // the reader is a file but the writer is a TCP stream.
                    // let mut redis_stream_writer = FramedWrite::new(writer, RdbCodec::new());
                    while let Some(result) = rdb_file_stream_reader.next().await {
                        debug!("RDB decoder returned: {:?}", result);

                        match result {
                            Ok(KeyValuePair {
                                key_expiry_time,
                                value_type: _,
                                key,
                                value,
                            }) => {
                                info!(
                                    "Loading {} {} {:?} from local db.",
                                    key, value, key_expiry_time
                                );

                                let mut set_params = SetCommandParameter {
                                    key: key.clone(),
                                    value: value.clone(),
                                    option: None,
                                    get: None,
                                    expire: None,
                                };

                                // Check to see if expiry was attached to this RDB entry
                                if let Some(expiry) = key_expiry_time {
                                    set_params.expire = Some(expiry);
                                    debug!("Set parameters: {:?}", set_params);
                                };

                                set_command_actor_handle
                                    .set_value(expire_tx.clone(), set_params.clone())
                                    .await;
                            }
                            Ok(_) => {
                                debug!("Ignoring other things.")
                            }
                            Err(e) => error!("Something bad happened: {}.", e),
                            // Ok(KeyValuePair { key_expiry_time, value_type, key, value }) => todo!,
                            // Ok(_) => info!("Skipping over OpCodes"),
                            // Err(_) => error!("{}",e),
                        }
                    }
                }
            }
        }
    }
}
