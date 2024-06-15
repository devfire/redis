use crate::{
    actors::messages::ProcessorActorMessage, errors::RedisError, parsers::parse_command,
    protocol::RedisCommand,
};

use log::info;
use resp::Value;
use tokio::sync::mpsc;

/// Handles CONFIG command. Receives message from the ProcessorActorHandle and processes them accordingly.
pub struct ProcessorActor {
    // The receiver for incoming messages
    receiver: mpsc::Receiver<ProcessorActorMessage>,
}

impl ProcessorActor {
    // Constructor for the actor
    pub fn new(receiver: mpsc::Receiver<ProcessorActorMessage>) -> Self {
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

    // Handle a message.
    pub fn handle_message(&mut self, msg: ProcessorActorMessage) {
        // Match on the type of the message
        match msg {
            // Handle a GetValue message
            ProcessorActorMessage::Process { request } => {
                // Process the message from RESP Decoder
                match request {
                    Value::Null => todo!(),
                    Value::NullArray => todo!(),
                    Value::String(_) => todo!(),
                    Value::Error(_) => todo!(),
                    Value::Integer(_) => todo!(),
                    Value::Bulk(_) => todo!(),
                    Value::BufBulk(_) => todo!(),
                    Value::Array(_) => {
                        // it's a bit clunky here but we need the original request, not what's inside Value::Array().
                        // Reason is, nom parser operates on str not Vec<Value>, so sending request as an encoded string,
                        // we can avoid recreating the original RESP array and just encode the request.
                        //
                        // NOTE: array of arrays is not supported at this time.
                        let request_as_encoded_string = request
                            .to_encoded_string()
                            .expect("Failed to to encode request as a string.");

                        info!("RESP request: {:?}", request_as_encoded_string);

                        // OK, what we get back from the parser is a command with all of its parameters.
                        // Now we get to do stuff with the command.
                        //
                        // If it's something simple like PING, we handle it immediately and return.
                        // If not, we get an actor handle and send it to the actor to process.
                        match parse_command(&request_as_encoded_string) {
                            Ok((_remaining_bytes, RedisCommand::Ping)) => {
                                // Encode the value to RESP binary buffer.
                                let response = Value::String("PONG".to_string()).encode();
                                let _ = writer.write_all(&response).await?;
                                writer.flush().await?;
                            }
                            Err(_) => {
                                let err_response =
                                    Value::Error(RedisError::ParseFailure.to_string()).encode();

                                let _ = writer.write_all(&err_response).await?;
                                writer.flush().await?;
                                // return Err(RedisError::ParseFailure.into()) closes the connection so let's not do that
                            }
                            Ok((_, RedisCommand::Echo(message))) => {
                                // Encode the value to RESP binary buffer.
                                let response = Value::String(message).encode();

                                let _ = writer.write_all(&response).await?;
                                writer.flush().await?;
                            }
                            Ok((_, RedisCommand::Command)) => {
                                // Encode the value to RESP binary buffer.
                                let response = Value::String("+OK".to_string()).encode();
                                let _ = writer.write_all(&response).await?;
                                writer.flush().await?;
                            }
                            Ok((_, RedisCommand::Set(set_parameters))) => {
                                info!("Set command parameters: {:?}", set_parameters);

                                // Sets the value for the key in the set parameters in the set command actor handle.
                                // Awaits the result.
                                set_command_actor_handle
                                    .set_value(expire_tx.clone(), set_parameters.clone())
                                    .await;

                                // Encode the value to RESP binary buffer.
                                let response = Value::String("OK".to_string()).encode();
                                let _ = writer.write_all(&response).await?;
                                writer.flush().await?;
                            }
                            Ok((_, RedisCommand::Get(key))) => {
                                // we may or may not get a value for the supplied key.
                                // if we do, we return it. If not, we encode Null and send that back.
                                if let Some(value) = set_command_actor_handle.get_value(&key).await
                                {
                                    let response = Value::String(value).encode();
                                    // Encode the value to RESP binary buffer.
                                    let _ = writer.write_all(&response).await?;
                                } else {
                                    let response = Value::Null.encode();
                                    let _ = writer.write_all(&response).await?;
                                }
                            }
                            Ok((_, RedisCommand::Del(keys))) => {
                                // iterate over all the keys, deleting them one by one
                                // https://redis.io/commands/del/

                                for key in &keys {
                                    set_command_actor_handle.delete_value(key).await;
                                }

                                let response = Value::Integer(keys.len() as i64).encode();
                                let _ = writer.write_all(&response).await?;
                                writer.flush().await?;
                            }
                            Ok((_, RedisCommand::Mget(keys))) => {
                                // Returns the values of all specified keys.
                                // For every key that does not hold a string value or does not exist,
                                // the special value nil is returned.
                                // Because of this, the operation never fails.
                                // https://redis.io/commands/mget/

                                let mut key_collection: Vec<Value> = Vec::new();

                                for key in &keys {
                                    if let Some(value) =
                                        set_command_actor_handle.get_value(&key).await
                                    {
                                        let response = Value::String(value);
                                        key_collection.push(response);
                                    } else {
                                        let response = Value::Null; // key does not exist, return nil
                                        key_collection.push(response);
                                    }
                                }
                                let response = Value::Array(key_collection).encode();

                                let _ = writer.write_all(&response).await?;
                                writer.flush().await?;
                            }
                            Ok((_, RedisCommand::Strlen(key))) => {
                                // we may or may not get a value for the supplied key.
                                // if we do, we return the length. If not, we encode 0 and send that back.
                                // https://redis.io/commands/strlen/
                                if let Some(value) = set_command_actor_handle.get_value(&key).await
                                {
                                    let response = Value::Integer(value.len() as i64).encode();
                                    // Encode the value to RESP binary buffer.
                                    let _ = writer.write_all(&response).await?;
                                    writer.flush().await?;
                                } else {
                                    let response = Value::Integer(0 as i64).encode();
                                    let _ = writer.write_all(&response).await?;
                                    writer.flush().await?;
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
                                if let Some(original_value) =
                                    set_command_actor_handle.get_value(&key).await
                                {
                                    new_value = original_value + &value_to_append;
                                } else {
                                    new_value = value_to_append;
                                }

                                // populate the set parameters struct.
                                // All the extraneous options are None since this is a pure APPEND op.
                                let set_parameters = SetCommandParameter {
                                    key,
                                    value: new_value.clone(),
                                    expire: None,
                                    get: None,
                                    option: None,
                                };

                                set_command_actor_handle
                                    .set_value(expire_tx.clone(), set_parameters)
                                    .await;

                                let response = Value::Integer(new_value.len() as i64).encode();
                                // Encode the value to RESP binary buffer.
                                let _ = writer.write_all(&response).await?;
                                writer.flush().await?;
                            }
                            Ok((_, RedisCommand::Config(config_key))) => {
                                // we may or may not get a value for the supplied key.
                                // if we do, we return it. If not, we encode Null and send that back.
                                if let Some(value) =
                                    config_command_actor_handle.get_value(config_key).await
                                {
                                    // let response = Value::String(value).encode();
                                    let mut response: Vec<Value> = Vec::new();

                                    // convert enum variant to String
                                    response.push(Value::String(config_key.to_string()));

                                    response.push(Value::String(value));

                                    let response_encoded = Value::Array(response).encode();

                                    // Encode the value to RESP binary buffer.
                                    let _ = writer.write_all(&response_encoded).await?;
                                    writer.flush().await?;
                                } else {
                                    let response = Value::Null.encode();
                                    let _ = writer.write_all(&response).await?;
                                    writer.flush().await?;
                                }
                            }

                            Ok((_, RedisCommand::Keys(pattern))) => {
                                // Returns the values of all specified keys matching the pattern.
                                //
                                // https://redis.io/commands/keys/

                                let mut keys_collection: Vec<Value> = Vec::new();

                                // see if there were any keys in the hashmap that match the pattern.
                                if let Some(keys) =
                                    set_command_actor_handle.get_keys(&pattern).await
                                {
                                    for key in keys {
                                        let response = Value::Bulk(key);
                                        keys_collection.push(response);
                                    }
                                } else {
                                    let response = Value::Null; // key does not exist, return nil
                                    keys_collection.push(response);
                                }

                                info!("Returning keys: {:?}", keys_collection);
                                let response = Value::Array(keys_collection).encode();

                                let _ = writer.write_all(&response).await?;
                                writer.flush().await?;
                            }

                            Ok((_, RedisCommand::Info(info_parameter))) => {
                                // we may or may not get a value for the INFO command.
                                //
                                // init the response to an empty string.
                                // We'll override it with something if we need to.
                                let mut response = Value::String("".to_string()).encode();

                                // first, let's see if this INFO section exists
                                if let Some(param) = info_parameter {
                                    let info = info_command_actor_handle.get_value(param).await;

                                    info!("Retrieved INFO value: {:?}", info);

                                    // then, let's see if the section contains data.
                                    if let Some(info_section) = info {
                                        response = Value::String(info_section.to_string()).encode();
                                    }
                                } else {
                                }

                                let _ = writer.write_all(&response).await?;
                                writer.flush().await?;
                            }
                        }
                    }
                }
            }
        }
    }
}
