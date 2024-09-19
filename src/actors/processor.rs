use std::time::Duration;

use crate::{
    actors::messages::{HostId, ProcessorActorMessage},
    parsers::parse_command,
    protocol::{
        RedisCommand, ReplConfCommandParameter, ReplicationSectionData, SetCommandParameter,
    },
    resp::value::RespValue,
};

use anyhow::{anyhow, Context};
use tokio::{
    sync::mpsc,
    time::{sleep, Instant},
};
use tracing::{debug, error, info};

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
    pub async fn run(&mut self) -> anyhow::Result<()> {
        // Continuously receive messages and handle them
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await.context("Unknown command")?;
        }

        Ok(())
    }

    // Handle a RespValue message, parse it into a RedisCommand, and reply back with the appropriate response.
    pub async fn handle_message(&mut self, msg: ProcessorActorMessage) -> anyhow::Result<()> {
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
                            .context("Failed to encode request as a string.")?;

                        tracing::info!(
                            "Simple string from master: {:?}",
                            request_as_encoded_string
                        );

                        match parse_command(&request_as_encoded_string) {
                            Ok((_remaining_bytes, RedisCommand::Fullresync(repl_id, offset))) => {
                                // we got RDB mem dump, time to load it
                                tracing::info!(
                                    "Received FULLRESYNC repl_id: {} offset: {} from master.",
                                    repl_id,
                                    offset
                                );
                                let _ = master_tx.send(repl_id).await?;
                                let _ = respond_to.send(None);

                                Ok(())
                            }
                            _ => {
                                tracing::info!(
                                    "Unknown string {}, forwarding to replica.",
                                    request_as_encoded_string
                                );
                                let _ = master_tx.send(request_as_encoded_string).await?;
                                let _ = respond_to.send(None);

                                Ok(())
                            }
                        }
                    }
                    RespValue::Error(_) => {
                        let _ = respond_to.send(None);
                        Ok(()) // NOTE: we are returning Ok here instead of Err because a RespValue::Error is not a program error.
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
                            .context("Failed to encode request as a string.")?;

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

                                Ok(())
                            }
                            Err(e) => {
                                // let err_response =
                                let _ =
                                    respond_to.send(Some(vec![(RespValue::Error(e.to_string()))]));

                                Ok(()) // NOTE: a parsing errror is not a Rust error, so we are returning Ok here.
                            }
                            Ok((_, RedisCommand::Echo(message))) => {
                                // Encode the value to RESP binary buffer.
                                let _ =
                                    respond_to.send(Some(vec![(RespValue::SimpleString(message))]));

                                Ok(())
                            }
                            Ok((_, RedisCommand::Command)) => {
                                // Encode the value to RESP binary buffer.
                                let _ = respond_to
                                    .send(Some(vec![(RespValue::SimpleString("OK".to_string()))]));

                                Ok(())
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

                                tracing::info!(
                                    "Current subscriber count: {}",
                                    replica_tx.receiver_count()
                                );

                                let _active_client_count = replica_tx.send(request)?;

                                tracing::info!(
                                    "Forwarding {:?} command to replicas.",
                                    request_as_encoded_string
                                );

                                Ok(())
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

                                Ok(())
                            }
                            Ok((_, RedisCommand::Del(keys))) => {
                                // iterate over all the keys, deleting them one by one
                                // https://redis.io/commands/del/

                                for key in &keys {
                                    set_command_actor_handle.delete_value(key).await;
                                }

                                let _ = respond_to
                                    .send(Some(vec![(RespValue::Integer(keys.len() as i64))]));

                                let _active_client_count = replica_tx.send(request)?;

                                tracing::info!(
                                    "Forwarding {:?} command to the replicas.",
                                    request_as_encoded_string
                                );

                                Ok(())
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

                                Ok(())
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

                                Ok(())
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

                                Ok(())
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

                                Ok(())
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

                                    keys_collection.push(response);
                                }

                                // debug!("Returning keys: {:?}", keys_collection);
                                let _ = respond_to
                                    .send(Some(vec![(RespValue::Array(keys_collection))]));

                                Ok(())
                            }

                            Ok((_, RedisCommand::Info(info_parameter))) => {
                                // we may or may not get a value for the INFO command.

                                // first, let's see if this INFO section exists.
                                // For now, we are assuming it's a Replication query but there may be others.
                                if let Some(_param) = info_parameter {
                                    // TODO: match on param
                                    let replication_data =
                                        replication_actor_handle.get_value(HostId::Myself).await;

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

                                Ok(())
                            }

                            Ok((_, RedisCommand::ReplConf(replconf_params))) => {
                                // initialize the reply of Vec<RespValue>
                                // let mut response: Vec<RespValue> = Vec::new();

                                // a simple OK to start with.
                                // response.push(RespValue::SimpleString("OK".to_string()));

                                // inform the handler_client that this is a replica
                                if let Some(client_or_replica_tx_sender) = client_or_replica_tx {
                                    let _ = client_or_replica_tx_sender.send(true).await?;
                                }

                                // Check what replconf parameter we have and act accordingly
                                // https://redis.io/commands/replconf
                                match replconf_params {
                                    ReplConfCommandParameter::Getack(ackvalue) => {
                                        // typically this is FROM the master, and is received by the replica.
                                        // So, if we are processing this, we are a replica.
                                        // Replies to this will go back over the OUTBOUND tcp connection to the master.
                                        // NOTE: Most replies are suppressed but these we need to send back to the master.
                                        info!("Replica received GETACK: {}", ackvalue);

                                        // check to make sure ackvalue is actually *
                                        if ackvalue == "*" {
                                            // We need to get the current offset value and send it back to the master.
                                            //
                                            // First, let's prepare the correct key message:
                                            // let info_key = InfoCommandParameter::Replication;

                                            // get the current replication data.
                                            if let Some(current_replication_data) =
                                                replication_actor_handle
                                                    .get_value(HostId::Myself)
                                                    .await
                                            {
                                                debug!(
                                                    "REPLICA: retrieving replication data {:?}",
                                                    current_replication_data
                                                );

                                                // extract the current offset value.
                                                let current_offset =
                                                    current_replication_data.master_repl_offset;

                                                let repl_conf_ack = RespValue::array_from_slice(&[
                                                    "REPLCONF",
                                                    "ACK",
                                                    &current_offset.to_string(),
                                                ]);

                                                // convert it to an encoded string purely for debugging purposes
                                                let repl_conf_ack_encoded =
                                                    repl_conf_ack.to_encoded_string()?;

                                                info!(
                                                    "REPLICA: returning {:?} from processor to main.rs loop.",
                                                    repl_conf_ack_encoded
                                                );

                                                // response.push(repl_conf_ack);

                                                // send the current offset value back to the master
                                                // NOTE: this does NOT go over the master_tx channel, which is only for replies TO the master.
                                                // NOTE: this is the offset BEFORE the latest
                                                let _ = respond_to.send(Some(vec![repl_conf_ack]));
                                            } else {
                                                error!(
                                                    "Unable to find replication data for {:?}",
                                                    host_id
                                                );
                                            }
                                        }

                                        Ok(())
                                    }
                                    ReplConfCommandParameter::Ack(ack) => {
                                        // These are received by the master from the replica slaves.
                                        tracing::info!("Received ACK: {} from {:?}", ack, host_id);

                                        // get the current replica's replication data.
                                        // This is master's POV into this specific replica's offset.
                                        if let Some(mut current_replication_data) =
                                            replication_actor_handle
                                                .get_value(host_id.clone())
                                                .await
                                        {
                                            // we need to convert the request to a RESP string to count the bytes.
                                            // let value_as_string = request.to_encoded_string()?;

                                            // calculate how many bytes are in the value_as_string
                                            // let value_as_string_bytes =
                                            //     value_as_string.len() as i16;

                                            // extract the current offset value.
                                            // let current_offset =
                                            //     current_replication_data.master_repl_offset;

                                            // update the offset value.
                                            current_replication_data.master_repl_offset =
                                                ack as i16;

                                            // update the offset value in the replication actor.
                                            replication_actor_handle
                                                .set_value(
                                                    host_id.clone(),
                                                    current_replication_data,
                                                )
                                                .await;
                                        } else {
                                            // We don't have an offset value for this replica, possibly this was after a WAIT global reset.
                                            error!(
                                                "Missing offset value for replica {:?}.",
                                                host_id
                                            );
                                        }
                                        // this is only ever received by the master, after REPLCONF GETACK *,
                                        // so we don't need to do anything here.
                                        let _ = respond_to.send(None);

                                        Ok(())
                                    }
                                    ReplConfCommandParameter::Capa => {
                                        let _ = respond_to.send(Some(vec![
                                            (RespValue::SimpleString("OK".to_string())),
                                        ]));

                                        Ok(())
                                    }
                                    ReplConfCommandParameter::ListeningPort(_port) => {
                                        // create a new record for a new replica, under the current master's repl ID.
                                        tracing::info!(
                                            "Creating a new replication record for {:?}.",
                                            host_id
                                        );

                                        if let Some(replication_data) =
                                            replication_actor_handle.get_value(HostId::Myself).await
                                        {
                                            replication_actor_handle
                                                .set_value(
                                                    host_id.clone(), // assuming the port we got and the port supplied are the same!
                                                    ReplicationSectionData {
                                                        role: crate::protocol::ServerRole::Slave, // msg comes from a replica, so it's a slave
                                                        master_replid: replication_data
                                                            .master_replid,
                                                        master_repl_offset: 0,
                                                    },
                                                )
                                                .await;
                                            let _ = respond_to.send(Some(vec![
                                                (RespValue::SimpleString("OK".to_string())),
                                            ]));
                                        }

                                        Ok(())
                                    }
                                }
                            }

                            Ok((_, RedisCommand::Psync(_replication_id, mut offset))) => {
                                // ignore the _replication_id for now. There are actually two of them:
                                // https://redis.io/docs/latest/operate/oss_and_stack/management/replication/#replication-id-explained

                                info!("PSYNC: Getting replication data for {:?}", host_id);

                                // Let's get the current replication values.
                                // let replication_data =
                                //     replication_actor_handle.get_value(host_id.clone()).await;

                                if let Some(mut replication_section_data) =
                                    replication_actor_handle.get_value(host_id.clone()).await
                                {
                                    // initialize the reply of Vec<Vec<u8>>
                                    //
                                    let mut reply: Vec<RespValue> = Vec::new();

                                    // check if the replica is asking for a full resynch
                                    if offset == -1 {
                                        // initial fullresync reply, reset offset to 0
                                        offset = 0;
                                    }

                                    tracing::info!(
                                        "For client {:?} storing offset {}",
                                        host_id,
                                        offset
                                    );

                                    // update the offset
                                    replication_section_data.master_repl_offset = offset;

                                    // persist the offset in the replication actor
                                    replication_actor_handle
                                        .set_value(
                                            host_id.clone(),
                                            replication_section_data.clone(),
                                        )
                                        .await;

                                    info!("Full resync triggered with offset {}", offset);

                                    reply.push(RespValue::SimpleString(format!(
                                        "FULLRESYNC {} {}",
                                        replication_section_data.master_replid,
                                        replication_section_data.master_repl_offset
                                    )));

                                    let rdb_file_contents = config_command_actor_handle
                                        .get_rdb()
                                        .await
                                        .context("Unable to load RDB file into memory")?;

                                    debug!(
                                        "Retrieved config file contents {:?}.",
                                        rdb_file_contents
                                    );

                                    // add the rdb file to the reply, at this point reply has 2 elements, each Vec<u8>
                                    reply.push(RespValue::Rdb(rdb_file_contents));

                                    let _ = respond_to.send(Some(reply));
                                } else {
                                    error!("Failed to retrieve replication information");
                                    let _ = respond_to.send(None);
                                }

                                Ok(())
                            } // end of psync
                            Ok((_, RedisCommand::Wait(numreplicas, timeout))) => {
                                info!("Processing WAIT {} {}", numreplicas, timeout);

                                let replconf_getack_star: RespValue =
                                    RespValue::array_from_slice(&["REPLCONF", "GETACK", "*"]);

                                // let _ = replica_tx.send(replconf_getack_star.clone())?;

                                // get the replica count
                                // let replicas_in_sync =
                                // replication_actor_handle.get_synced_replica_count().await;

                                // info!("We have {} in sync replicas.", replicas_in_sync);

                                // let's implement the wait command
                                // https://redis.io/commands/wait/
                                //
                                // The command takes two parameters:
                                // 1. numreplicas: The number of replicas that must be connected and in sync.
                                // 2. timeout: The maximum number of milliseconds to wait for the replicas to be connected and in sync.
                                //
                                // detailed OG implementation: https://github.com/redis/redis/blob/unstable/src/replication.c#L3548
                                // if replicas_in_sync >= numreplicas {
                                //     // we can return immediately
                                //     info!(
                                //         "{} > {}, returning immediately.",
                                //         replicas_in_sync, numreplicas
                                //     );
                                //     let _ = respond_to.send(Some(vec![
                                //         (RespValue::Integer(replicas_in_sync as i64)),
                                //     ]));
                                // } else {
                                // we need to wait for the replicas to be connected and in sync
                                // but we won't wait more than timeout milliseconds.
                                // Also, we will send REPLCONF ACK * to the replicas to get their current offset.
                                // This will update the offset in the replication actor.

                                // flush the replica in sync db because we are about to ask all replicas for their offsets
                                // replication_actor_handle.reset_synced_replica_count().await;

                                // ok now we wait for everyone to reply
                                info!("Starting the waiting period of {} milliseconds.", timeout);

                                let _ = replica_tx.send(replconf_getack_star)?;

                                let start_time = Instant::now();

                                sleep(Duration::from_millis(timeout.try_into()?)).await;

                                let elapsed_time = start_time.elapsed();
                                info!("Done waiting after {:?}!", elapsed_time);

                                // get the replica count again
                                let replicas_in_sync =
                                    replication_actor_handle.get_synced_replica_count().await;

                                let _ = respond_to.send(Some(vec![
                                    (RespValue::Integer(replicas_in_sync as i64)),
                                ]));
                                // }

                                Ok(())
                            }
                            _ => {
                                debug!("Unsupported command: {:?}", request_as_encoded_string);

                                Err(anyhow!("Unsupported command."))
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

                        Ok(())
                    }
                }
            }
        }
    }
}
