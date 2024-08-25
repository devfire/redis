use crate::errors::RedisError;
use crate::protocol::SetCommandParameter;
use crate::rdb::codec::RdbCodec;

use crate::rdb::format::Rdb::KeyValuePair;
// Import necessary modules and types
use crate::{actors::messages::ConfigActorMessage, protocol::ConfigCommandParameter};
// use bytes::Buf;
// use futures_util::io::BufReader;

use anyhow::Error;
use futures::StreamExt;
use tracing::{debug, error};
// use resp::Value;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
// use tokio::io::AsyncWriteExt;
use tokio_util::codec::FramedRead;

use std::{collections::HashMap, path::Path};

// use tokio::net::TcpStream;
use tokio::fs;
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
    pub async fn handle_message(&mut self, msg: ConfigActorMessage) -> anyhow::Result<()> {
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

                Ok(())
            }

            // Handle a SetValue message
            ConfigActorMessage::SetConfigValue {
                config_key,
                config_value,
            } => {
                // Insert the key-value pair into the hash map
                self.kv_hash.insert(config_key, config_value);

                Ok(())
            }

            ConfigActorMessage::ImportRdb {
                set_command_actor_handle,
                import_from_memory,
                expire_tx,
            } => {
                // check if we are loading from memory or disk.
                // let mut rdb_file_stream_reader;

                match import_from_memory {
                    Some(buffer) => {
                        debug!("Loading config from memory.");
                        // Create a cursor over the Vec<u8>
                        let cursor = std::io::Cursor::new(buffer);

                        // stream the rdb file, decoding and parsing the saved entries.
                        let mut rdb_memory_stream_reader = FramedRead::new(cursor, RdbCodec::new());

                        while let Some(result) = rdb_memory_stream_reader.next().await {
                            debug!("RDB decoder returned: {:?}", result);

                            match result {
                                Ok(KeyValuePair {
                                    key_expiry_time,
                                    value_type: _,
                                    key,
                                    value,
                                }) => {
                                    debug!(
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
                                Err(e) => error!("Failure trying to load config: {}.", e),
                                // Ok(KeyValuePair { key_expiry_time, value_type, key, value }) => todo!,
                                // Ok(_) => debug!("Skipping over OpCodes"),
                                // Err(_) => error!("{}",e),
                            }
                        }

                        Ok(())
                    }
                    None => {
                        debug!("Loading config from disk.");
                        let dir = self.kv_hash.get(&ConfigCommandParameter::Dir).unwrap();
                        let dbfilename = self
                            .kv_hash
                            .get(&ConfigCommandParameter::DbFilename)
                            .unwrap();

                        let fullpath = format!("{}/{}", dir, dbfilename);

                        // check to see if the file exists.
                        if !Path::new(&fullpath).exists() {
                            Err(RedisError::ConfigFileOpenError(fullpath.to_string()).into())
                        } else {
                            // file exists, let's proceed.
                            // Log the attempt
                            debug!("Loading config {} from disk", fullpath);

                            let rdb_file = File::open(fullpath)
                                .await
                                .expect("Failed to open RDB file.");

                            // stream the rdb file, decoding and parsing the saved entries.
                            let mut rdb_file_stream_reader =
                                FramedRead::new(rdb_file, RdbCodec::new());
                            while let Some(result) = rdb_file_stream_reader.next().await {
                                debug!("RDB decoder returned: {:?}", result);

                                match result {
                                    Ok(KeyValuePair {
                                        key_expiry_time,
                                        value_type: _,
                                        key,
                                        value,
                                    }) => {
                                        debug!(
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
                                    Err(e) => error!("Failure trying to load config: {}.", e),
                                    // Ok(KeyValuePair { key_expiry_time, value_type, key, value }) => todo!,
                                    // Ok(_) => debug!("Skipping over OpCodes"),
                                    // Err(_) => error!("{}",e),
                                }
                            }
                        }
                    }
                }
            }
            // Loads on disk RDB file into memory and returns as Vec<u8> of bytes.
            // Checks if the specified config file exists. If it does, attempts to load its contents into memory.
            //
            // Upon successful reading, the file's contents are sent through the `respond_to` channel.
            // If the file does not exist, logs an error message and sends a `None` value through the `respond_to` channel.
            //
            // This involves opening the file asynchronously, reading its entire content into a byte vector (`buffer`),
            // and returning it through the `respond_to` channel back to the handler.
            ConfigActorMessage::GetRdb { respond_to } => {
                let dir = self.kv_hash.get(&ConfigCommandParameter::Dir).unwrap();
                let dbfilename = self
                    .kv_hash
                    .get(&ConfigCommandParameter::DbFilename)
                    .unwrap();

                let fullpath = format!("{}/{}", dir, dbfilename);
                // list all the files in the directory

                let current_dir = Path::new(".");
                let mut entries = fs::read_dir(current_dir)
                    .await
                    .expect("Failed to read directory");

                while let Some(entry) = entries.next_entry().await.expect("Failed to read entry") {
                    let path = entry.path();
                    if path.is_file() {
                        debug!("File: {:?}", path);
                    } else if path.is_dir() {
                        debug!("Directory: {:?}", path);
                    }
                }

                // check to see if the file exists.
                if !Path::new(&fullpath).exists() {
                    error!("Config file {} does not exist.", fullpath);
                    let _ = respond_to.send(None); // this will be turned into an Err in the handler
                } else {
                    // file exists, let's proceed.
                    // Log the attempt
                    debug!("Loading config {} into memory.", fullpath);

                    let mut rdb_file = File::open(fullpath)
                        .await
                        .expect("Failed to open RDB file.");

                    let mut buffer: Vec<u8> = Vec::new();

                    rdb_file
                        .read_to_end(&mut buffer)
                        .await
                        .expect("Failed to read config into memory");

                    let _ = respond_to.send(Some(buffer));
                }
            }
        }
    }
}
