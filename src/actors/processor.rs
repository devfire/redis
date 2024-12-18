use std::time::Duration;

use crate::{
    actors::messages::{HostId, ProcessorActorMessage},
    parsers::parse_command,
    protocol::{
        RedisCommand, ReplConfCommandParameter, ReplicationSectionData, ServerRole,
        SetCommandParameter,
    },
    resp::value::RespValue,
    utils::sleeping_task,
};

use anyhow::{anyhow, Context};
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

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
                wait_sleep_tx,
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

                        tracing::debug!(
                            "Simple string from master: {:?}",
                            request_as_encoded_string
                        );

                        match parse_command(&request_as_encoded_string) {
                            Ok((_remaining_bytes, RedisCommand::Fullresync(repl_id, offset))) => {
                                // we got RDB mem dump, time to load it
                                tracing::debug!(
                                    "Received FULLRESYNC repl_id: {} offset: {} from master.",
                                    repl_id,
                                    offset
                                );
                                let _ = master_tx.send(repl_id).await?;
                                let _ = respond_to.send(None);

                                Ok(())
                            }
                            _ => {
                                tracing::debug!(
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
                                debug!("Current subscriber count: {}", replica_tx.receiver_count());

                                // calculate how many bytes are in the value_as_string
                                // let request_num_bytes = request_as_encoded_string.len() as i16;

                                // // we need to update master's offset because we are sending writeable commands to replicas
                                // let mut updated_replication_data_master =
                                //     ReplicationSectionData::new();

                                // // remember, this is an INCREMENT not a total new value
                                // updated_replication_data_master.master_repl_offset =
                                //     Some(request_num_bytes);

                                // replication_actor_handle
                                //     .update_value(HostId::Myself, updated_replication_data_master)
                                //     .await;

                                let _active_client_count = replica_tx.send(request)?;

                                tracing::debug!(
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

                                tracing::debug!(
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
                                // For now, we are assuming it's a INFO REPLICATION query cmd but there may be others.
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
                                        debug!("Replica received GETACK: {}", ackvalue);

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
                                                let current_offset = current_replication_data
                                                    .master_repl_offset
                                                    .expect("Expected to find an replication entry for the replica.");

                                                let repl_conf_ack = RespValue::array_from_slice(&[
                                                    "REPLCONF",
                                                    "ACK",
                                                    &current_offset.to_string(),
                                                ]);

                                                // convert it to an encoded string purely for debugging purposes
                                                let repl_conf_ack_encoded =
                                                    repl_conf_ack.to_encoded_string()?;

                                                debug!(
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
                                        debug!("Received ACK: {} from {:?}", ack, host_id);

                                        // we got a new value, so let's reset the offset.
                                        replication_actor_handle
                                            .reset_replica_offset(host_id.clone())
                                            .await;

                                        // create a new replication data struct.
                                        let mut current_replication_data =
                                            ReplicationSectionData::new();

                                        // set the master_repl_offset to ack
                                        current_replication_data.master_repl_offset =
                                            Some(ack as i16);

                                        // update the offset value in the replication actor.
                                        replication_actor_handle
                                            .update_value(host_id, current_replication_data)
                                            .await;

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
                                        let _ = respond_to.send(Some(vec![
                                            (RespValue::SimpleString("OK".to_string())),
                                        ]));

                                        Ok(())
                                    }
                                }
                            }

                            Ok((_, RedisCommand::Psync(_replication_id, offset))) => {
                                // ignore the _replication_id for now. There are actually two of them:
                                // https://redis.io/docs/latest/operate/oss_and_stack/management/replication/#replication-id-explained

                                debug!("PSYNC: Processing replication data for {host_id}");

                                // initialize the reply of Vec<Vec<u8>>
                                //
                                let mut reply: Vec<RespValue> = Vec::new();

                                // Check if we've seen this replica before.
                                // TODO: move the common sections that always get executed out of the if let Some
                                // conditional.
                                if let Some(replication_section_data) =
                                    replication_actor_handle.get_value(host_id.clone()).await
                                {
                                    debug!("Known replica {replication_section_data}, proceeding.");
                                } else {
                                    warn!("Replica not seen before, adding.");
                                    let mut replication_data = ReplicationSectionData::new();

                                    // this is a replica we've not seen before, so let's initialize everything.
                                    replication_data.role = Some(ServerRole::Slave);
                                    replication_data.master_replid = replication_actor_handle
                                        .get_value(HostId::Myself)
                                        .await
                                        .expect("This should never fail because we always know our own replication ID.")
                                        .master_replid;
                                    replication_data.master_repl_offset = Some(0);

                                    // store the replica's values in the replication actor
                                    replication_actor_handle
                                        .update_value(host_id.clone(), replication_data)
                                        .await;

                                    // let _ = respond_to.send(None);
                                }

                                // check if the replica is asking for a full resync
                                if offset == -1 {
                                    // initial fullresync reply
                                    debug!("Full resync triggered with offset {}", offset);

                                    // Master got PSYNC ? -1
                                    // replica is expecting +FULLRESYNC <REPL_ID> 0\r\n back
                                    reply.push(RespValue::SimpleString(format!(
                                        "FULLRESYNC {} 0",
                                        replication_actor_handle
                                        .get_value(HostId::Myself)
                                        .await
                                        .expect("This should never fail because the master knows about itself")
                                        .master_replid.expect("We should know our own replid"),
                                    )));

                                    // master will then send a RDB file of its current state to the replica.
                                    // The replica is expected to load the file into memory, replacing its current state.
                                    let rdb_file_contents = config_command_actor_handle
                                        .get_rdb()
                                        .await
                                        .context("Unable to load RDB file into memory")?;

                                    tracing::debug!("For client {:?} storing offset 0", host_id);

                                    // update the offset
                                    replication_actor_handle.reset_replica_offset(host_id).await;

                                    // add the rdb file to the reply, at this point reply has 2 elements, each Vec<u8>
                                    reply.push(RespValue::Rdb(rdb_file_contents));
                                }

                                let _ = respond_to.send(Some(reply));

                                Ok(())
                            } // end of psync
                            Ok((_, RedisCommand::Wait(numreplicas, timeout))) => {
                                debug!("Processing WAIT {} {}", numreplicas, timeout);

                                let replconf_getack_star: RespValue =
                                    RespValue::array_from_slice(&["REPLCONF", "GETACK", "*"]);
                                // let _ = replica_tx.send(replconf_getack_star.clone())?;

                                let current_master_offset = replication_actor_handle
                                    .get_value(HostId::Myself)
                                    .await
                                    .expect("Expected to always have self information.")
                                    .master_repl_offset
                                    .expect("Master always has offset.");

                                // get the replica count
                                let replicas_in_sync = replication_actor_handle
                                    .get_synced_replica_count(current_master_offset) // -37 is REPLCONF GETACK *
                                    .await;

                                tracing::info!("Target number of replicas: {numreplicas} and we have {replicas_in_sync} replicas in sync.");

                                // let's implement the wait command
                                // https://redis.io/commands/wait/
                                //
                                // The command takes two parameters:
                                // 1. numreplicas: The number of replicas that must be connected and in sync.
                                // 2. timeout: The maximum number of milliseconds to wait for the replicas to be connected and in sync.
                                //
                                // detailed OG implementation: https://github.com/redis/redis/blob/unstable/src/replication.c#L3548
                                if replicas_in_sync >= numreplicas {
                                    // we can return immediately
                                    let _ = respond_to.send(Some(vec![
                                        (RespValue::Integer(replicas_in_sync as i64)),
                                    ]));
                                } else {
                                    let _ = replica_tx.send(replconf_getack_star)?;

                                    // let start_time = Instant::now();

                                    let duration = Duration::from_millis(timeout.try_into()?);

                                    let _sleeping_handle = sleeping_task(
                                        wait_sleep_tx.expect(
                                            "IF we are processing WAIT this must be present.",
                                        ),
                                        duration,
                                        current_master_offset,
                                    )
                                    .await;

                                    // yielding back to tokio

                                    // sleeping_handle.await?;

                                    // let replicas_in_sync =
                                    //     replication_actor_handle.get_synced_replica_count().await;

                                    //     debug!("After REPLCONF ACK we have {replicas_in_sync} in sync replicas.");

                                    let _ = respond_to.send(None); // no replies at this point, the sleeping_task fxn will reply
                                }

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
