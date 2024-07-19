use crate::{
    actors::messages::ProcessorActorMessage,
    errors::RedisError,
    parsers::parse_command,
    protocol::{InfoCommandParameter, RedisCommand, SetCommandParameter},
};

use log::{debug, error, info};
use resp::{encode_slice, Value};
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
            self.handle_message(msg).await;
        }
    }

    // Handle a message.
    pub async fn handle_message(&mut self, msg: ProcessorActorMessage) {
        debug!("Received message: {:?}", msg);
        // Match on the type of the message
        match msg {
            // Handle a Process message
            ProcessorActorMessage::Process {
                request,
                set_command_actor_handle,
                config_command_actor_handle,
                info_command_actor_handle,
                expire_tx,
                master_tx,
                respond_to,
                replica_tx,
            } => {
                // Process the message from RESP Decoder
                match request {
                    Value::Null => todo!(),
                    Value::NullArray => todo!(),
                    Value::String(s) => {
                        // client commands *to* redis server come as Arrays, so this must be
                        // a response from the master server.
                        info!("Received string: {}, sending it to the channel.", s);
                        let _ = master_tx
                            .send(s)
                            .await
                            .expect("Unable to send master replies.");
                        let _ = respond_to.send(None);
                    }
                    Value::Error(e) => {
                        error!("Received error: {}", e);
                        let _ = respond_to.send(None);
                    }
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
                            .expect("Failed to encode request as a string.");

                        info!("RESP request: {:?}", request_as_encoded_string);

                        // OK, what we get back from the parser is a command with all of its parameters.
                        // Now we get to do stuff with the command.
                        //
                        // If it's something simple like PING, we handle it immediately and return.
                        // If not, we get an actor handle and send it to the actor to process.
                        match parse_command(&request_as_encoded_string) {
                            Ok((_remaining_bytes, RedisCommand::Ping)) => {
                                // Send the RESP Value back to the handler, ignore send errors
                                let _ = respond_to
                                    .send(Some(vec![(Value::String("PONG".to_string()).encode())]));
                            }
                            Err(_) => {
                                // let err_response =
                                let _ = respond_to.send(Some(vec![
                                    (Value::Error(RedisError::ParseFailure.to_string()).encode()),
                                ]));
                            }
                            Ok((_, RedisCommand::Echo(message))) => {
                                // Encode the value to RESP binary buffer.
                                let _ =
                                    respond_to.send(Some(vec![(Value::String(message).encode())]));
                            }
                            Ok((_, RedisCommand::Command)) => {
                                // Encode the value to RESP binary buffer.
                                let _ = respond_to
                                    .send(Some(vec![(Value::String("+OK".to_string()).encode())]));
                            }
                            Ok((_, RedisCommand::Set(set_parameters))) => {
                                // info!("Set command parameters: {:?}", set_parameters);

                                // Sets the value for the key in the set parameters in the set command actor handle.
                                // Awaits the result.
                                set_command_actor_handle
                                    .set_value(expire_tx.clone(), set_parameters.clone())
                                    .await;

                                // Encode the value to RESP binary buffer.
                                let _ = respond_to
                                    .send(Some(vec![(Value::String("OK".to_string()).encode())]));

                                // forward this to the replicas
                                if let Some(replica_tx) = replica_tx {
                                    let _ = replica_tx
                                        .send(request_as_encoded_string.into_bytes())
                                        .expect("Unable to send replica replies.");
                                }
                            }
                            Ok((_, RedisCommand::Get(key))) => {
                                // we may or may not get a value for the supplied key.
                                // if we do, we return it. If not, we encode Null and send that back.
                                if let Some(value) = set_command_actor_handle.get_value(&key).await
                                {
                                    let _ = respond_to
                                        .send(Some(vec![(Value::String(value).encode())]));
                                } else {
                                    let _ = respond_to.send(Some(vec![(Value::Null.encode())]));
                                }
                            }
                            Ok((_, RedisCommand::Del(keys))) => {
                                // iterate over all the keys, deleting them one by one
                                // https://redis.io/commands/del/

                                for key in &keys {
                                    set_command_actor_handle.delete_value(key).await;
                                }

                                let _ = respond_to
                                    .send(Some(vec![(Value::Integer(keys.len() as i64).encode())]));
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
                                let _ = respond_to
                                    .send(Some(vec![(Value::Array(key_collection).encode())]));
                            }
                            Ok((_, RedisCommand::Strlen(key))) => {
                                // we may or may not get a value for the supplied key.
                                // if we do, we return the length. If not, we encode 0 and send that back.
                                // https://redis.io/commands/strlen/
                                if let Some(value) = set_command_actor_handle.get_value(&key).await
                                {
                                    let _ = respond_to.send(Some(vec![
                                        (Value::Integer(value.len() as i64).encode()),
                                    ]));
                                } else {
                                    let _ = respond_to
                                        .send(Some(vec![(Value::Integer(0 as i64).encode())]));
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

                                let _ = respond_to.send(Some(vec![
                                    (Value::Integer(new_value.len() as i64).encode()),
                                ]));
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

                                    let _ = respond_to
                                        .send(Some(vec![(Value::Array(response).encode())]));
                                } else {
                                    let _ = respond_to.send(Some(vec![(Value::Null.encode())]));
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

                                // info!("Returning keys: {:?}", keys_collection);
                                let _ = respond_to
                                    .send(Some(vec![(Value::Array(keys_collection).encode())]));
                            }

                            Ok((_, RedisCommand::Info(info_parameter))) => {
                                // we may or may not get a value for the INFO command.

                                // first, let's see if this INFO section exists
                                if let Some(param) = info_parameter {
                                    let info = info_command_actor_handle.get_value(param).await;

                                    debug!("Retrieved INFO value: {:?}", info);

                                    // then, let's see if the section contains data.
                                    if let Some(info_section) = info {
                                        let _ = respond_to.send(Some(vec![Value::String(
                                            info_section.to_string(),
                                        )
                                        .encode()]));
                                    } else {
                                        let _ = respond_to.send(Some(vec![Value::Null.encode()]));
                                    }
                                } else {
                                    let _ = respond_to.send(Some(vec![Value::Null.encode()]));
                                }
                            }

                            Ok((_, RedisCommand::ReplConf)) => {
                                let _ = respond_to
                                    .send(Some(vec![(Value::String("OK".to_string()).encode())]));
                            }

                            Ok((_, RedisCommand::Psync(_replication_id, offset))) => {
                                // ignore the _replication_id coming from the replica since we will supply our own
                                let info = info_command_actor_handle
                                    .get_value(InfoCommandParameter::Replication)
                                    .await;

                                if let Some(info_section) = info {
                                    // initialize the reply of Vec<Vec<u8>>
                                    //
                                    let mut reply = Vec::new();

                                    // initial fullresync reply
                                    let new_offset = 0;
                                    // check if the replica is asking for a full resynch
                                    if offset == -1 {
                                        info!("Full resync triggered with offset {}", new_offset);
                                    }

                                    reply.push(
                                        Value::String(format!(
                                            "FULLRESYNC {} {}",
                                            info_section.master_replid, new_offset
                                        ))
                                        .encode(),
                                    );

                                    // append the file contents immediately after
                                    // this is the format: $<length_of_file>\r\n<contents_of_file>

                                    let mut rdb_in_memory: Vec<u8> = Vec::new();

                                    // let rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

                                    // let rdb_file_contents =
                                    //     hex::decode(rdb_hex).expect("Failed to decode hex");

                                    let rdb_file_contents = config_command_actor_handle
                                        .get_config()
                                        .await
                                        .expect("Unable to load RDB file into memory");

                                    info!(
                                        "Retrieved config file contents {:?}.",
                                        rdb_file_contents
                                    );

                                    rdb_in_memory.extend(Vec::from("$"));
                                    rdb_in_memory
                                        .extend(rdb_file_contents.len().to_string().as_bytes()); // length of file
                                    rdb_in_memory.extend(Vec::from("\r\n"));
                                    rdb_in_memory.extend(rdb_file_contents);

                                    // add the rdb file to the reply
                                    reply.push(rdb_in_memory);

                                    let _ = respond_to.send(Some(reply));
                                } else {
                                    error!("Failed to retrieve replication information");
                                }
                            } // end of match
                        }
                    }
                }
            }
        }
    }
}
