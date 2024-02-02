// Import necessary modules and types
use crate::protocol::RedisCommand;



use log::info;

use resp::Value;
use tokio::sync::mpsc;

/// Handles CONFIG command. Receives message from the ConfigCommandActorHandle and processes them accordingly.
pub struct ProcessActor {
    // The receiver for incoming messages
    receiver: mpsc::Receiver<RedisCommand>,

    // The key-value hash map for storing data
    // kv_hash: HashMap<String, String>,
}

impl ProcessActor {
    // Constructor for the actor
    pub fn new(receiver: mpsc::Receiver<RedisCommand>) -> Self {
        // Initialize the key-value hash map. The key is an enum of two types, dir and dbfilename.
        // let kv_hash = HashMap::new();

        // Return a new actor with the given receiver and an empty key-value hash map
        Self { receiver }
    }

    // Run the actor
    pub async fn run(&mut self) {
        // Continuously receive messages and handle them
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg);
        }
    }

    // Handle a message
    pub async fn handle_message(&mut self, msg: RedisCommand) -> Vec<u8>{
        // Match on the type of the message
        match msg {
            RedisCommand::Ping => {
                // Encode the value to RESP binary buffer.
                return Value::String("PONG".to_string()).encode();
                // let _ = writer.write_all(&response).await?;
            }
            // return Err(RedisError::ParseFailure.into()) closes the connection so let's not do that
            // Err(_) => {
            //     return Value::Error(RedisError::ParseFailure.to_string()).encode();

            //     // let _ = writer.write_all(&err_response).await?;
            // }
            Ok((_, RedisCommand::Echo(message))) => {
                // Encode the value to RESP binary buffer.
                let response = Value::String(message).encode();

                // let _ = writer.write_all(&response).await.expect("Failed to write to writer");
            }
            Ok((_, RedisCommand::Command)) => {
                // Encode the value to RESP binary buffer.
                let response = Value::String("+OK".to_string()).encode();
                // let _ = writer.write_all(&response).await?;
            }
            Ok((_, RedisCommand::Set(set_parameters))) => {
                info!("Set command parameters: {:?}", set_parameters);

                set_command_actor_handle
                    .set_value(set_parameters.clone())
                    .await;

                // don't even bother checking whether there is an expiry field or not.
                // Reason is, this will always send to the channel and the while loop will figure out if there's an expiry field or not.
                // if not, this is a noop.
                expire_tx
                    .send(set_parameters.clone())
                    .await
                    .expect("Unable to start the expiry thread.");
                // Encode the value to RESP binary buffer.
                let response = Value::String("OK".to_string()).encode();
                // let _ = writer.write_all(&response).await?;
            }
            Ok((_, RedisCommand::Get(key))) => {
                // we may or may not get a value for the supplied key.
                // if we do, we return it. If not, we encode Null and send that back.
                if let Some(value) = set_command_actor_handle.get_value(&key).await {
                    let response = Value::String(value).encode();
                    // Encode the value to RESP binary buffer.
                    // let _ = writer.write_all(&response).await?;
                } else {
                    let response = Value::Null.encode();
                    // let _ = writer.write_all(&response).await?;
                }
            }
            Ok((_, RedisCommand::Del(keys))) => {
                // iterate over all the keys, deleting them one by one
                // https://redis.io/commands/del/

                for key in &keys {
                    set_command_actor_handle.expire_value(key).await;
                }

                let response = Value::Integer(keys.len() as i64).encode();
                let _ = writer.write_all(&response).await?;
            }
            Ok((_, RedisCommand::Mget(keys))) => {
                // Returns the values of all specified keys.
                // For every key that does not hold a string value or does not exist, the special value nil is returned.
                // Because of this, the operation never fails.
                // https://redis.io/commands/mget/

                let mut key_collection: Vec<Value> = Vec::new();

                for key in &keys {
                    if let Some(value) = set_command_actor_handle.get_value(&key).await {
                        let response = Value::String(value);
                        key_collection.push(response);
                    } else {
                        let response = Value::Null; // key does not exist, return nil
                        key_collection.push(response);
                        // let _ = writer.write_all(&response).await?;
                    }
                }
                let response = Value::Array(key_collection).encode();

                let _ = writer.write_all(&response).await?;
            }
            Ok((_, RedisCommand::Strlen(key))) => {
                // we may or may not get a value for the supplied key.
                // if we do, we return the length. If not, we encode 0 and send that back.
                // https://redis.io/commands/strlen/
                if let Some(value) = set_command_actor_handle.get_value(&key).await {
                    let response = Value::Integer(value.len() as i64).encode();
                    // Encode the value to RESP binary buffer.
                    let _ = writer.write_all(&response).await?;
                } else {
                    let response = Value::Integer(0 as i64).encode();
                    let _ = writer.write_all(&response).await?;
                }
            }

            // If key already exists and is a string, this command appends the value at the end of the string.
            // If key does not exist it is created and set as an empty string,
            // so APPEND will be similar to SET in this special case.
            Ok((_, RedisCommand::Append(key, value_to_append))) => {
                // we may or may not already have a value for the supplied key.
                // if we do, we append. If not, we create via a SET
                // https://redis.io/commands/append/

                // Initialize an empty string for the future.
                let new_value: String;
                if let Some(original_value) = set_command_actor_handle.get_value(&key).await
                {
                    new_value = original_value + &value_to_append;
                } else {
                    new_value = value_to_append;
                }

                // populate the set parameters struct.
                // All the extraneous options are None since this is a pure APPEND op.
                let set_parameters = SetCommandParameters {
                    key,
                    value: new_value.clone(),
                    expire: None,
                    get: None,
                    option: None,
                };

                set_command_actor_handle.set_value(set_parameters).await;

                let response = Value::Integer(new_value.len() as i64).encode();
                // Encode the value to RESP binary buffer.
                let _ = writer.write_all(&response).await?;
            }
            Ok((_, RedisCommand::Config(key))) => {
                // we may or may not get a value for the supplied key.
                // if we do, we return it. If not, we encode Null and send that back.
                if let Some(value) = config_command_actor_handle.get_value(&key).await {
                    // let response = Value::String(value).encode();
                    let mut response: Vec<Value> = Vec::new();
                    response.push(Value::String(key));

                    response.push(Value::String(value));

                    let response_encoded = Value::Array(response).encode();

                    // Encode the value to RESP binary buffer.
                    let _ = writer.write_all(&response_encoded).await?;
                } else {
                    let response = Value::Null.encode();
                    let _ = writer.write_all(&response).await?;
                }
            }
        }
    }
}

