use crate::{
    actors::messages::ProcessorActorMessage,
    errors::RedisError,
    parsers::parse_command,
    protocol::{InfoCommandParameter, RedisCommand, SetCommandParameter},
    resp::value::RespValue,
};

use tokio::sync::mpsc;
use tracing::{debug, error};

// use rand::distributions::Alphanumeric;
// use rand::Rng;
// use std::io::Write;
// use std::iter::{self};

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

    // Handle a RespValue message, parse it into a RedisCommand, and reply back with the appropriate response.
    pub async fn handle_message(&mut self, msg: ProcessorActorMessage) {
        tracing::debug!("Handling message: {:?}", msg);
        // Match on the type of the message
        match msg {
            // Handle a Process message
            ProcessorActorMessage::Process {
                request,
                set_command_actor_handle,
                config_command_actor_handle,
                replication_actor_handle,
                host_id,
                expire_tx,
                master_tx,
                replica_tx,
                client_or_replica_tx,
                respond_to,
            } => {
                // Process the message from RESP Decoder
                match request {
                    RespValue::Null => todo!(),
                    RespValue::NullArray => todo!(),
                    RespValue::SimpleString(_) => {
                        // client commands *to* redis server come as Arrays, so this must be
                        // a response from the master server.
                        let request_as_encoded_string = request
                            .to_encoded_string()
                            .expect("Failed to encode request as a string.");

                        debug!("RESP request: {:?}", request_as_encoded_string);

                        match parse_command(&request_as_encoded_string) {
                            Ok((_remaining_bytes, RedisCommand::Fullresync(repl_id, offset))) => {
                                // we got RDB mem dump, time to load it
                                debug!(
                                    "Received FULLRESYNC repl_id: {} offset: {}.",
                                    repl_id, offset
                                );
                                let _ = respond_to.send(None);
                            }
                            _ => {
                                debug!(
                                    "Unknown string {}, forwarding to replica.",
                                    request_as_encoded_string
                                );
                                let _ = master_tx
                                    .send(request_as_encoded_string)
                                    .await
                                    .expect("Unable to send master replies.");
                                let _ = respond_to.send(None);
                            }
                        }
                    }
                    RespValue::Error(e) => {
                        error!("Received error: {}", e);
                        let _ = respond_to.send(None);
                    }
                    RespValue::Integer(_) => todo!(),
                    RespValue::Array(_) => {
                        // it's a bit clunky here but we need the original request, not what's inside RespValue::Array().
                        // Reason is, nom parser operates on str not Vec<Value>, so sending request as an encoded string,
                        // we can avoid recreating the original RESP array and just encode the request.
                        //
                        // NOTE: array of arrays is not supported at this time.
                        let request_as_encoded_string = request
                            .to_encoded_string()
                            .expect("Failed to encode request as a string.");

                        debug!("RESP request: {:?}", request_as_encoded_string);

                        // OK, what we get back from the parser is a command with all of its parameters.
                        // Now we get to do stuff with the command.
                        //
                        // If it's something simple like PING, we handle it immediately and return.
                        // If not, we get an actor handle and send it to the actor to process.
                        match parse_command(&request_as_encoded_string) {
                            Ok((_remaining_bytes, RedisCommand::Ping)) => {
                                // Send the RESP Value back to the handler, ignore send errors
                                let _ = respond_to.send(Some(vec![
                                    (RespValue::SimpleString("PONG".to_string())),
                                ]));
                            }
                            Err(_) => {
                                // let err_response =
                                let _ = respond_to.send(Some(vec![
                                    (RespValue::Error(RedisError::ParseFailure.to_string())),
                                ]));
                            }
                            Ok((_, RedisCommand::Echo(message))) => {
                                // Encode the value to RESP binary buffer.
                                let _ =
                                    respond_to.send(Some(vec![(RespValue::SimpleString(message))]));
                            }
                            Ok((_, RedisCommand::Command)) => {
                                // Encode the value to RESP binary buffer.
                                let _ = respond_to
                                    .send(Some(vec![(RespValue::SimpleString("OK".to_string()))]));
                            }
                            Ok((_, RedisCommand::Set(set_parameters))) => {
                                debug!("Set command parameters: {:?}", set_parameters);

                                // Sets the value for the key in the set parameters in the set command actor handle.
                                // Awaits the result.
                                set_command_actor_handle
                                    .set_value(expire_tx.clone(), set_parameters.clone())
                                    .await;

                                // Encode the value to RESP binary buffer.
                                let _ = respond_to
                                    .send(Some(vec![(RespValue::SimpleString("OK".to_string()))]));

                                // forward this to the replicas
                                if let Some(replica_tx_sender) = replica_tx {
                                    debug!(
                                        "Forwarding {} command to replicas.",
                                        request_as_encoded_string
                                    );
                                    let _ = replica_tx_sender
                                        .send(request)
                                        .expect("Unable to send replica replies.");
                                }
                            }
                            Ok((_, RedisCommand::Get(key))) => {
                                // we may or may not get a value for the supplied key.
                                // if we do, we return it. If not, we encode Null and send that back.
                                if let Some(value) = set_command_actor_handle.get_value(&key).await
                                {
                                    let _ = respond_to
                                        .send(Some(vec![(RespValue::SimpleString(value))]));
                                } else {
                                    let _ = respond_to.send(Some(vec![(RespValue::Null)]));
                                }
                            }
                            Ok((_, RedisCommand::Del(keys))) => {
                                // iterate over all the keys, deleting them one by one
                                // https://redis.io/commands/del/

                                for key in &keys {
                                    set_command_actor_handle.delete_value(key).await;
                                }

                                let _ = respond_to
                                    .send(Some(vec![(RespValue::Integer(keys.len() as i64))]));
                            }
                            Ok((_, RedisCommand::Mget(keys))) => {
                                // Returns the values of all specified keys.
                                // For every key that does not hold a string value or does not exist,
                                // the special value nil is returned.
                                // Because of this, the operation never fails.
                                // https://redis.io/commands/mget/

                                let mut key_collection: Vec<RespValue> = Vec::new();

                                for key in &keys {
                                    if let Some(value) =
                                        set_command_actor_handle.get_value(&key).await
                                    {
                                        let response = RespValue::SimpleString(value);
                                        key_collection.push(response);
                                    } else {
                                        let response = RespValue::Null; // key does not exist, return nil
                                        key_collection.push(response);
                                    }
                                }
                                let _ =
                                    respond_to.send(Some(vec![(RespValue::Array(key_collection))]));
                            }
                            Ok((_, RedisCommand::Strlen(key))) => {
                                // we may or may not get a value for the supplied key.
                                // if we do, we return the length. If not, we encode 0 and send that back.
                                // https://redis.io/commands/strlen/
                                if let Some(value) = set_command_actor_handle.get_value(&key).await
                                {
                                    let _ = respond_to
                                        .send(Some(vec![(RespValue::Integer(value.len() as i64))]));
                                } else {
                                    let _ =
                                        respond_to.send(Some(vec![(RespValue::Integer(0 as i64))]));
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

                                let _ = respond_to
                                    .send(Some(vec![(RespValue::Integer(new_value.len() as i64))]));
                            }
                            Ok((_, RedisCommand::Config(config_key))) => {
                                // we may or may not get a value for the supplied key.
                                // if we do, we return it. If not, we encode Null and send that back.
                                if let Some(value) =
                                    config_command_actor_handle.get_value(config_key).await
                                {
                                    // let response = RespValue::String(value).encode();
                                    let mut response: Vec<RespValue> = Vec::new();

                                    // convert enum variant to String
                                    response.push(RespValue::SimpleString(config_key.to_string()));

                                    response.push(RespValue::SimpleString(value));

                                    let _ =
                                        respond_to.send(Some(vec![(RespValue::Array(response))]));
                                } else {
                                    let _ = respond_to.send(Some(vec![(RespValue::Null)]));
                                }
                            }

                            Ok((_, RedisCommand::Keys(pattern))) => {
                                // Returns the values of all specified keys matching the pattern.
                                //
                                // https://redis.io/commands/keys/

                                let mut keys_collection = Vec::new();

                                // see if there were any keys in the hashmap that match the pattern.
                                if let Some(keys) =
                                    set_command_actor_handle.get_keys(&pattern).await
                                {
                                    for key in keys {
                                        let response =
                                            RespValue::BulkString(Some(key.into_bytes()));
                                        keys_collection.push(response);
                                    }
                                    // we need to convert Vec<String> to &[&str]
                                    // let slices = keys.as_slice();

                                    // for slice_str in slices {
                                    //     let response = slice_str.as_str();
                                    //     keys_collection.push(response);
                                    // }
                                } else {
                                    let response = RespValue::Null;
                                    // .to_encoded_string()
                                    // .expect("Failed to encode"); // key does not exist, return nil
                                    keys_collection.push(response);
                                }

                                // debug!("Returning keys: {:?}", keys_collection);
                                let _ = respond_to
                                    .send(Some(vec![(RespValue::Array(keys_collection))]));
                            }

                            Ok((_, RedisCommand::Info(info_parameter))) => {
                                // we may or may not get a value for the INFO command.

                                // first, let's see if this INFO section exists.
                                // For now, we are assuming it's a Replication query but there may be others.
                                if let Some(param) = info_parameter {
                                    let replication_data =
                                        replication_actor_handle.get_value(param, host_id).await;

                                    tracing::debug!(
                                        "Retrieved INFO RespValue: {:?}",
                                        replication_data
                                    );

                                    // then, let's see if the section contains data.
                                    if let Some(replication_section) = replication_data {
                                        let _ =
                                            respond_to.send(Some(vec![RespValue::SimpleString(
                                                replication_section.to_string(),
                                            )]));
                                    } else {
                                        let _ = respond_to.send(Some(vec![RespValue::Null]));
                                    }
                                } else {
                                    let _ = respond_to.send(Some(vec![RespValue::Null]));
                                }
                            }

                            Ok((_, RedisCommand::ReplConf(replconf_params))) => {
                                // initialize the reply of Vec<RespValue>
                                // let mut response: Vec<RespValue> = Vec::new();

                                // a simple OK to start with.
                                // response.push(RespValue::SimpleString("OK".to_string()));

                                // inform the handler_client that this is a replica
                                if let Some(client_or_replica_tx_sender) = client_or_replica_tx {
                                    let _ = client_or_replica_tx_sender
                                        .send(true)
                                        .await
                                        .expect("Unable to update replica client status.");
                                }

                                // Check what replconf parameter we have and act accordingly
                                // https://redis.io/commands/replconf
                                match replconf_params {
                                    crate::protocol::ReplConfCommandParameter::Getack(ackvalue) => {
                                        // typically this is FROM the master, and is received by the replica.
                                        // So, if we are processing this, we are a replica.
                                        // Replies to this will go back over the OUTBOUND tcp connection to the master.
                                        // NOTE: Most replies are suppressed but these we need to send back to the master.
                                        debug!("Received GETACK: {}", ackvalue);

                                        // check to make sure ackvalue is actually *
                                        if ackvalue == "*" {
                                            // We need to get the current offset value and send it back to the master.
                                            //
                                            // First, let's prepare the correct key message:
                                            let info_key = InfoCommandParameter::Replication;

                                            // get the current replication data.
                                            let current_replication_data = replication_actor_handle
                                                .get_value(info_key, host_id.clone())
                                                .await
                                                .expect("Unable to get current replication data.");

                                            // extract the current offset value.
                                            let current_offset =
                                                current_replication_data.master_repl_offset;

                                            let repl_conf_ack = RespValue::array_from_slice(&[
                                                "REPLCONF",
                                                "ACK",
                                                &current_offset.to_string(),
                                            ]);

                                            // convert it to an encoded string purely for debugging purposes
                                            let repl_conf_ack_encoded = repl_conf_ack
                                                .to_encoded_string()
                                                .expect("Failed to encode repl_conf_ack");

                                            tracing::debug!(
                                                "Sending REPLCONF ACK: {}",
                                                repl_conf_ack_encoded
                                            );

                                            // response.push(repl_conf_ack);

                                            // send the current offset value back to the master
                                            // NOTE: this does NOT go over the master_tx channel, which is only for replies TO the master.
                                            let _ = respond_to.send(Some(vec![repl_conf_ack]));
                                        }
                                    }
                                    crate::protocol::ReplConfCommandParameter::Ack(ack) => {
                                        tracing::debug!("Received ACK: {}", ack);

                                        // this is only ever received by the master, after REPLCONF GETACK *,
                                        // so we don't need to do anything here.
                                        let _ = respond_to.send(None);
                                    }
                                    crate::protocol::ReplConfCommandParameter::Capa => {
                                        let _ = respond_to.send(Some(vec![
                                            (RespValue::SimpleString("OK".to_string())),
                                        ]));
                                    }
                                    crate::protocol::ReplConfCommandParameter::ListeningPort(_) => {
                                        let _ = respond_to.send(Some(vec![
                                            (RespValue::SimpleString("OK".to_string())),
                                        ]));
                                    }
                                }
                            }

                            Ok((_, RedisCommand::Psync(_replication_id, mut offset))) => {
                                // ignore the _replication_id for now. There are actually two of them:
                                // https://redis.io/docs/latest/operate/oss_and_stack/management/replication/#replication-id-explained

                                // Let's get the current replication values.
                                let info = replication_actor_handle
                                    .get_value(InfoCommandParameter::Replication, host_id.clone())
                                    .await;

                                if let Some(info_section) = info {
                                    // initialize the reply of Vec<Vec<u8>>
                                    //
                                    let mut reply: Vec<RespValue> = Vec::new();

                                    // check if the replica is asking for a full resynch
                                    if offset == -1 {
                                        // initial fullresync reply, reset offset to 0
                                        offset = 0;
                                    }

                                    // persist the offset in the info section
                                    replication_actor_handle
                                        .set_value(
                                            host_id.clone(),
                                            crate::protocol::ReplicationSectionData {
                                                role: info_section.role,
                                                master_replid: info_section.master_replid.clone(),
                                                master_repl_offset: offset,
                                            },
                                        )
                                        .await;

                                    debug!("Full resync triggered with offset {}", offset);

                                    reply.push(RespValue::SimpleString(format!(
                                        "FULLRESYNC {} {}",
                                        info_section.master_replid, offset
                                    )));

                                    let rdb_file_contents = config_command_actor_handle
                                        .get_rdb()
                                        .await
                                        .expect("Unable to load RDB file into memory");

                                    debug!(
                                        "Retrieved config file contents {:?}.",
                                        rdb_file_contents
                                    );

                                    // add the rdb file to the reply, at this point reply has 2 elements, each Vec<u8>
                                    reply.push(RespValue::Rdb(rdb_file_contents));

                                    let _ = respond_to.send(Some(reply));
                                } else {
                                    error!("Failed to retrieve replication information");
                                }
                            } // end of psync
                            Ok((_, RedisCommand::Wait(numreplicas, timeout))) => {
                                debug!("Processing WAIT {} {}", numreplicas, timeout);

                                // get the replica count
                                let replica_count =
                                    replication_actor_handle.get_connected_replica_count().await;
                                let _ = respond_to
                                    .send(Some(vec![(RespValue::Integer(replica_count as i64))]));
                            }
                            _ => {
                                debug!("Unsupported command: {:?}", request_as_encoded_string);
                            }
                        }
                    }
                    RespValue::BulkString(_) => todo!(),
                    RespValue::Rdb(rdb) => {
                        debug!("Received RDB file: {:?}", rdb);

                        // Import it into the config actor
                        config_command_actor_handle
                            .import_config(set_command_actor_handle.clone(), Some(rdb), expire_tx)
                            .await;

                        let _ = respond_to.send(None);
                    }
                }
            }
        }
    }
}
