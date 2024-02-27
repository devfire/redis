// Import necessary modules and types
use crate::{messages::ConfigActorMessage, protocol::ConfigCommandParameters};
// use bytes::Buf;
// use futures_util::io::BufReader;

use log::info;

use std::{collections::HashMap, path::Path};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;



// include the format.rs file from rdb
// use crate::rdb::format;


/// Handles CONFIG command. Receives message from the ConfigCommandActorHandle and processes them accordingly.
pub struct ConfigCommandActor {
    // The receiver for incoming messages
    receiver: mpsc::Receiver<ConfigActorMessage>,

    // The key-value hash map for storing data
    kv_hash: HashMap<ConfigCommandParameters, String>,
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
            ConfigActorMessage::GetValue {
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
            ConfigActorMessage::SetValue {
                config_key,
                config_value,
            } => {
                // Insert the key-value pair into the hash map
                self.kv_hash
                    .insert(config_key.clone(), config_value.clone());

                // Log a success message
                info!(
                    "Successfully inserted key {:?} value {}.",
                    config_key, config_value
                );
            }

            ConfigActorMessage::LoadConfig { dir, dbfilename } => {
                let fullpath = format!("{}/{}", dir, dbfilename);

                // check to see if the file exists.
                if !Path::new(&fullpath).exists() {
                    log::error!("Config file does not exist.");
                } else {
                    // file exists, let's proceed.
                    // Log the attempt
                    info!("Loading config {}", fullpath);

                    

                   // establish a TCP connection to local host
                    let stream = TcpStream::connect("127.0.0.1:6379")
                        .await
                        .expect("Unable to connect to localhost.");

                    let (mut _reader, mut writer) = stream.into_split();

                    // writer
                    //     .write_all(&bytes)
                    //     .await
                    //     .expect("Failed to write to TCP writer.");
                }
            }
        }
    }
}
