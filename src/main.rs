use std::path::Path;

use crate::resp::value::RespValue;

use actors::messages::HostId;
use anyhow::{ensure, Result};

use clap::Parser;

use futures::{SinkExt, StreamExt};
use resp::codec::RespCodec;
use utils::{expire_value, generate_replication_id, handshake};
// use std::time::{SystemTime, UNIX_EPOCH};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::level_filters::LevelFilter;

use protocol::{ReplicationSectionData, ServerRole, SetCommandParameter};
use tracing::{error, info};
use tracing_subscriber::{prelude::*, EnvFilter};

use tokio::sync::{broadcast, mpsc};
// use tokio::time::{sleep, Duration};

pub mod actors;
pub mod cli;
pub mod errors;
pub mod handlers;
pub mod intervals;
pub mod parsers;
pub mod protocol;
pub mod rdb;
pub mod resp;
pub mod utils;

use crate::cli::Cli;

use crate::handlers::{
    config_command::ConfigCommandActorHandle, replication::ReplicationActorHandle,
    request_processor::RequestProcessorActorHandle, set_command::SetCommandActorHandle,
};

use crate::protocol::ConfigCommandParameter;

// use env_logger::Env;
// use log::{debug, info};
// use resp::{encode_slice, Decoder};

// use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use async_channel;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create an EnvFilter builder and set a default directive.
    // Here, LevelFilter::INFO is used as the default level.
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into()) // Set default logging level to INFO
        .from_env_lossy(); // Attempt to parse RUST_LOG, ignore invalid directives

    // Initialize a tracing subscriber suitable for async applications
    let _subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::Layer::new())
        .with(filter)
        .init();

    let cli = Cli::parse();

    // let ip_listen = "0.0.0.0".to_string();

    // cli.port comes from cli.rs; default is 6379
    let socket_address = std::net::SocketAddr::from(([0, 0, 0, 0], cli.port));

    let listener = TcpListener::bind(socket_address).await?;

    tracing::info!("Redis is running on port {}.", cli.port);

    // Get a handle to the set actor, one per redis. This starts the actor.
    let set_command_actor_handle = SetCommandActorHandle::new();

    // Get a handle to the info actor, one per redis. This starts the actor.
    let replication_actor_handle = ReplicationActorHandle::new();

    // Get a handle to the config actor, one per redis. This starts the actor.
    let config_command_actor_handle = ConfigCommandActorHandle::new();

    // this is where decoded resp values are sent for processing
    let request_processor_actor_handle = RequestProcessorActorHandle::new();

    // Create a multi-producer, single-consumer channel to send expiration messages.
    // The channel capacity is set to 9600.
    let (expire_tx, mut expire_rx) = mpsc::channel::<SetCommandParameter>(9600);

    // An async multi-producer multi-consumer channel,
    // where each message can be received by only one of all existing consumers.
    let (tcp_msgs_tx, tcp_msgs_rx) = async_channel::unbounded();

    // Create a multi-producer, single-consumer channel to recv messages from the master.
    // NOTE: these messages are replies coming back from the master, not commands to the master.
    // Used by handshake() to forward replies from the master, from replica to itself.
    // Typically, these are +OK and FULLRESYNC messages.
    let (master_tx, master_rx) = mpsc::channel::<String>(9600);

    // Setup a tokio broadcast channel to communicate all writeable updates to all the replicas.
    // This is a multi-producer, multi-consumer channel.
    // The replica_tx Sender is cloned and passed to the client handler.
    // The replica_tx is given to request_processor_actor_handle.process_request() to send writeable updates to the replica,
    // via the same initial connection that the replica used to connect to the master.
    //
    // NOTE: the master handler that got created as part of the outbound connection from the replica to the master,
    // does not handle replication messages. It only sends commands to the master and receives replies.
    // Basically, from master's POV, a replica is just a client. But from replica's POV, it acts as a client to the master,
    // receiving replies from the master via the master_rx channel.
    let (replica_tx, _replica_rx) = broadcast::channel::<RespValue>(9600);

    // Check the value provided by the arguments.
    // Store the config values if they are valid.
    // NOTE: If nothing is passed, cli.rs has the default values for clap.
    if let Some(dir) = cli.dir.as_deref() {
        // This macro is equivalent to if !$cond { return Err(anyhow!($args...)); }.
        // https://docs.rs/anyhow/latest/anyhow/macro.ensure.html
        // NOTE: we cannot use ensure! because this exits; instead we need to create the file if it
        // doesn't exist.
        ensure!(Path::new(&dir).exists(), "Directory {} not found.", dir);

        config_command_actor_handle
            .set_value(ConfigCommandParameter::Dir, dir)
            .await;
    }

    if let Some(dbfilename) = cli.dbfilename.as_deref() {
        config_command_actor_handle
            .set_value(
                ConfigCommandParameter::DbFilename,
                &dbfilename.to_string_lossy(),
            )
            .await;

        // let config_dbfilename = dbfilename.to_string_lossy().to_string();

        config_command_actor_handle
            .import_config(
                set_command_actor_handle.clone(), // need to pass this to get direct access to the redis db
                None,                             // load from disk
                expire_tx.clone(), // need to pass this to unlock expirations on config file load
            )
            .await;
    }

    // initialize to being a master, override if we are a replica.
    let replication_data: ReplicationSectionData = ReplicationSectionData {
        role: Some(ServerRole::Master),
        master_replid: Some(generate_replication_id()),
        master_repl_offset: None,
    };

    replication_actor_handle
        .update_value(HostId::Myself, replication_data)
        .await;

    info!(
        "Just set the value: {}",
        replication_actor_handle
            .get_value(HostId::Myself)
            .await
            .expect("Should have found the self value.")
    );

    // see if we need to override it
    if let Some(replica) = cli.replicaof.as_deref() {
        let master_host_port_combo = replica.replace(" ", ":");

        // We can pass a string to TcpStream::connect, so no need to create SocketAddr
        let stream = TcpStream::connect(&master_host_port_combo)
            .await
            .expect("Failed to establish connection to master."); // panic is ok here since this is not a recoverable error.

        // Must clone the actors handlers because tokio::spawn move will grab everything.
        let set_command_handler_clone = set_command_actor_handle.clone();
        let config_command_handler_clone = config_command_actor_handle.clone();
        let replication_actor_handle_clone = replication_actor_handle.clone();
        let request_processor_actor_handle_clone = request_processor_actor_handle.clone();

        let expire_tx_clone = expire_tx.clone();
        let tcp_msgs_rx_clone = tcp_msgs_rx.clone();
        let master_tx_clone = master_tx.clone();
        let replica_tx_clone = replica_tx.clone();

        tokio::spawn(async move {
            handle_connection_to_master(
                stream,
                set_command_handler_clone,
                config_command_handler_clone,
                replication_actor_handle_clone,
                request_processor_actor_handle_clone,
                expire_tx_clone,
                tcp_msgs_rx_clone,
                master_tx_clone,
                replica_tx_clone, // used to send replication messages to the replica
            )
            .await
        });

        // handshake sets the replica replid based on the value it gets from the master.
        handshake(
            tcp_msgs_tx.clone(),
            master_rx,
            cli.port,
            replication_actor_handle.clone(),
        )
        .await?;

        // // one more round of cloning
        // let replication_actor_handle_clone = replication_actor_handle.clone();
        // // kick off a once a sec offset update to master
        // tokio::spawn(async move {
        //     send_offset_to_master(tcp_msgs_tx.clone(), replication_actor_handle_clone, 1)
        //         .await
        // });
    }

    // we must clone the handler to the SetActor because the whole thing is being moved into an expiry handle loop
    let set_command_handle_expiry_clone = set_command_actor_handle.clone();

    // This will listen for messages on the expire_tx channel.
    // Once a msg comes, it'll see if it's an expiry message and if it is,
    // will move everything and spawn off a thread to expire in the future.
    let _expiry_handle_loop: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
        // Start receiving messages from the channel by calling the recv method of the Receiver endpoint.
        // This method blocks until a message is received.
        while let Some(msg) = expire_rx.recv().await {
            expire_value(msg, set_command_handle_expiry_clone.clone()).await?;
        }

        Ok(())
    });

    loop {
        // Asynchronously wait for an inbound TcpStream.
        let (stream, socket_address) = listener.accept().await?;

        info!("Received connection from {}", socket_address);

        // Must clone the actors handlers because tokio::spawn move will grab everything.
        let set_command_handler_clone = set_command_actor_handle.clone();
        let config_command_handler_clone = config_command_actor_handle.clone();
        let info_command_actor_handle_clone = replication_actor_handle.clone();
        let request_processor_actor_handle_clone = request_processor_actor_handle.clone();

        let expire_tx_clone = expire_tx.clone();
        let master_tx_clone = master_tx.clone();

        let replica_tx_clone = replica_tx.clone();
        // let replica_rx_subscriber = replica_tx.subscribe();

        // Spawn our handler to be run asynchronously.
        // A new task is spawned for each inbound socket.  The socket is moved to the new task and processed there.
        tokio::spawn(async move {
            handle_connection_from_clients(
                stream,
                set_command_handler_clone,
                config_command_handler_clone,
                info_command_actor_handle_clone,
                request_processor_actor_handle_clone,
                expire_tx_clone,
                master_tx_clone,
                replica_tx_clone,
                // replica_rx_subscriber,
            )
            .await
        });
    }
}

// This function will handle the connection from the client.
// The reason why we need two separate functions, one for clients and one for master,
// is because the replica will be acting as a client, sending commands to the master and receiving replies.
//
// But the handle_connection_from_clients() function will only be receiving commands from clients and sending replies.
// In other words, a redis instance can be both, a replica client to the master, and a server to its own clients.
// So, this is the "server" part of the redis instance.
// #[tracing::instrument]
async fn handle_connection_from_clients(
    stream: TcpStream,
    set_command_actor_handle: SetCommandActorHandle,
    config_command_actor_handle: ConfigCommandActorHandle,
    replication_actor_handle: ReplicationActorHandle,
    request_processor_actor_handle: RequestProcessorActorHandle,
    expire_tx: mpsc::Sender<SetCommandParameter>,
    master_tx: mpsc::Sender<String>, // passthrough to request_processor_actor_handle
    replica_tx: broadcast::Sender<RespValue>, // used to send replication messages to the replica
                                     // mut replica_rx: broadcast::Receiver<RespValue>, // used to receive replication messages from the master
) -> anyhow::Result<()> {
    let client_address = stream.peer_addr().map(|addr| addr)?;

    let client_ip = client_address.ip().to_string();
    let client_port = client_address.port();

    let host_id = HostId::Host {
        ip: client_ip,
        port: client_port,
    };
    info!("Handling connection from {:?}", host_id);

    let mut replica_rx = replica_tx.subscribe();

    info!("Subscribed to replica updates {:?}", replica_rx);

    // Split the TCP stream into a reader and writer.
    let (reader, writer) = stream.into_split();

    let mut reader = FramedRead::new(reader, RespCodec::new());
    let mut writer = FramedWrite::new(writer, RespCodec::new());

    // This is a channel to let the thread know whether the client is a replica or not.
    // We need to know because replication messages are only sent to replicas, not to redis-cli clients.
    let (client_or_replica_tx, mut client_or_replica_rx) = mpsc::channel::<bool>(3);

    let mut am_i_replica: bool = false;

    loop {
        tokio::select! {
            Some(msg) = reader.next() => {
                match msg {
                    Ok(request) => {
                        // send the request to the request processor actor.
                        tracing::info!("Received {:?} from client: {:?}", request.to_encoded_string()?, host_id);
                        if let Some(processed_values) = request_processor_actor_handle
                            .process_request(
                                request,
                                set_command_actor_handle.clone(),
                                config_command_actor_handle.clone(),
                                replication_actor_handle.clone(),
                                host_id.clone(),
                                expire_tx.clone(),
                                master_tx.clone(), // these are ack +OK replies from the master back to handshake()
                                replica_tx.clone(), // used to send replication messages to the replica
                                Some(client_or_replica_tx.clone()), // used to update replica status
                            )
                            .await
                        {
                            tracing::debug!("Preparing to send {} responses to client: {:?}", processed_values.len(), processed_values);

                            // iterate over processed_value and send each one to the client
                            for value in &processed_values {
                                // info!("Sending response {:?} to client: {:?}", value.to_encoded_string()?, host_id);
                                let _ = writer.send(value.clone()).await?;

                                tracing::debug!("Done sending, moving to the next value.");
                            }
                        }
                    }
                    Err(e) => {
                        error!("Unable to decode request from client: {e}");
                    }
                }
            }
         msg = replica_rx.recv() => { // from processor.rs replica_tx
            tracing::debug!("replica_rx channel received {:?} for {:?}", msg.clone()?.to_encoded_string()?, host_id);
            match msg {
                Ok(msg) => {
                    // Send replication messages only to replicas, not to other clients.
                    if am_i_replica {
                        info!("Sending message {:?} to replica: {:?}", msg.to_encoded_string()?, host_id);

                        // we need to convert the command to a RESP string to count the bytes.
                        let value_as_string = msg.to_encoded_string()?;

                        // calculate how many bytes are in the value_as_string
                        let value_as_string_num_bytes = value_as_string.len() as i16;

                        // we need to update master's offset because we are sending writeable commands to replicas
                        let mut updated_replication_data = ReplicationSectionData::new();

                        // remember, this is an INCREMENT not a total new value
                        updated_replication_data.master_repl_offset =Some(value_as_string_num_bytes);

                        replication_actor_handle.update_value(HostId::Myself,updated_replication_data).await;

                        // if let Some(mut current_replication_data) = replication_actor_handle.get_value(HostId::Myself).await {
                        //     // we need to convert the command to a RESP string to count the bytes.
                        //     let value_as_string = msg.to_encoded_string()?;

                        //     // calculate how many bytes are in the value_as_string
                        //     let value_as_string_num_bytes = value_as_string.len() as i16;

                        //     // extract the current offset value.
                        //     let current_offset = current_replication_data.master_repl_offset;

                        //     // update the offset value.
                        //     let new_offset = current_offset + value_as_string_num_bytes;

                        //     current_replication_data.master_repl_offset = new_offset;

                        //     // update the offset value in the replication actor.
                        //     replication_actor_handle.set_value(HostId::Myself,current_replication_data).await;

                        //     info!("Current master offset: {} new offset: {}",current_offset,new_offset);
                        // }
                        let _ = writer.send(msg).await?;
                        // writer.flush().await?;
                    } else {
                        tracing::debug!("Not forwarding message to non-replica client {:?}.", host_id);
                    }
                }
                Err(e) => {
                    error!("Something unexpected happened: {e}");
                }
            }
         }
         Some(msg) = client_or_replica_rx.recv() => {
            // // if let Some(client_type) = msg {
                // check to make sure this client is a replica, not a redis-cli client.
                // if it is a redis-cli client, we don't want to send replication messages to it.
                // we only want to send replication messages to replicas.
                am_i_replica  = msg;

                tracing::debug!("Updated client {:?} replica status to {}", host_id, am_i_replica);
            // // }
         }
        } // end tokio::select
    }
}

// This is the "client" part of the redis instance.
// #[tracing::instrument]
async fn handle_connection_to_master(
    stream: TcpStream,
    set_command_actor_handle: SetCommandActorHandle,
    config_command_actor_handle: ConfigCommandActorHandle,
    replication_actor_handle: ReplicationActorHandle,
    request_processor_actor_handle: RequestProcessorActorHandle,
    expire_tx: mpsc::Sender<SetCommandParameter>,
    tcp_msgs_rx: async_channel::Receiver<RespValue>,
    master_tx: mpsc::Sender<String>, // passthrough to request_processor_actor_handle
    replica_tx: broadcast::Sender<RespValue>, // used to send replication messages to the replica
) -> Result<()> {
    // Split the TCP stream into a reader and writer.
    let (reader, writer) = stream.into_split();

    let mut reader = FramedRead::new(reader, RespCodec::new());
    let mut writer = FramedWrite::new(writer, RespCodec::new());

    loop {
        tokio::select! {
            // Read data from the stream, these are commands from the master to the replica
            Some(msg) = reader.next() => {
                match msg {
                    Ok(request) => {
                        // send the request to the request processor actor
                        if let Some(processed_value) = request_processor_actor_handle
                            .process_request(
                                request.clone(),
                                set_command_actor_handle.clone(),
                                config_command_actor_handle.clone(),
                                replication_actor_handle.clone(),
                                HostId::Myself, // we are a replica, creating outbound connections, so we are Myself
                                expire_tx.clone(),
                                master_tx.clone(), // these are ack +OK replies from the master back to handshake()
                                replica_tx.clone(), // this enables daisy chaining of replicas to other replicas
                                None, // connections to master cannot update replica status
                            )
                            .await
                        {
                             // This is replica's own offset calculations.
                                                           // we need to convert the request to a RESP string to count the bytes.
                                let value_as_string = request.to_encoded_string()?;

                                // calculate how many bytes are in the value_as_string
                                let value_as_string_num_bytes = value_as_string.len() as i16;

                                info!("REPLICA: {:?} has {value_as_string_num_bytes} bytes.", value_as_string);


                                                        // we need to update master's offset because we are sending writeable commands to replicas
                        let mut updated_replication_data = ReplicationSectionData::new();
                        // remember, this is an INCREMENT not a total new value
                        updated_replication_data.master_repl_offset =Some(value_as_string_num_bytes);

                        replication_actor_handle.update_value(HostId::Myself,updated_replication_data).await;

                                                    // iterate over processed_value and send each one to the client

                                                    let strings_to_reply = "REPLCONF";
                                                    for value in processed_value.iter() {
                                                        // check to see if processed_value contains REPLCONF in the encoded string
                                                        if value.to_encoded_string()?.contains(strings_to_reply) {
                                                            // info!("Sending response to master: {:?}", value.to_encoded_string()?);
                                                            let _ = writer.send(value.clone()).await?;
                                                        }
                                                    }

                             // First, let's get our current replication data from replica's POV.
                            // if let Some(mut current_replication_data) = replication_actor_handle.get_value(HostId::Myself).await {
                            //     // we need to convert the request to a RESP string to count the bytes.
                            //     let value_as_string = request.to_encoded_string()?;

                            //     // calculate how many bytes are in the value_as_string
                            //     let value_as_string_num_bytes = value_as_string.len() as i16;

                            //     info!("REPLICA: {:?} has {value_as_string_num_bytes} bytes.", value_as_string);

                            //     // extract the current offset value.
                            //     let current_offset = current_replication_data.master_repl_offset;

                            //     // update the offset value.
                            //     let new_offset = current_offset + value_as_string_num_bytes;

                            //     current_replication_data.master_repl_offset = new_offset;

                            //     // update the offset value in the replication actor.
                            //     replication_actor_handle.update_value(HostId::Myself,current_replication_data).await;

                            //     info!("REPLICA: current offset: {current_offset} new offset: {new_offset}");

                            //     debug!("Only REPLCONF ACK commands are sent back to master: {:?}", processed_value);
                            //     // iterate over processed_value and send each one to the client

                            //     let strings_to_reply = "REPLCONF";
                            //     for value in processed_value.iter() {
                            //         // check to see if processed_value contains REPLCONF in the encoded string
                            //         if value.to_encoded_string()?.contains(strings_to_reply) {
                            //             // info!("Sending response to master: {:?}", value.to_encoded_string()?);
                            //             let _ = writer.send(value.clone()).await?;
                            //         }
                            //     }
                            // } else {
                            //     error!("Unable to locate replica replication data.");
                            // }


                        }
                    }
                    Err(e) => {
                        error!("Unable to decode request from master: {e}");
                    }
                } // end match
         } // end reader
         // see if we have any message to send to master.
         // handshake() is the only function communicating on this channel.
         // NOTE: this channel is async_channel::unbounded(), which means only 1 msg will be processed by all consumers, like AWS SQS.
         // However, we only have 1 consumer, the master, so this is fine. This is because a replica only connects to 1 master.
         msg = tcp_msgs_rx.recv() => {
            match msg {
                Ok(msg) => {
                    tracing::info!("Sending message to master: {:?}", msg.to_encoded_string()?);
                    let _ = writer.send(msg).await?;
                    // writer.flush().await?;
                }
                Err(e) => {
                    error!("Something unexpected happened: {e}");
                }
            }
         }
        } // end tokio::select
    }
}
