use crate::resp::value::RespValue;

use anyhow::Result;
use clap::Parser;
use futures::{SinkExt, StreamExt};
use resp::codec::RespCodec;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_util::codec::{FramedRead, FramedWrite};

use protocol::{ReplicationSectionData, ServerRole, SetCommandParameter};
use tracing::{debug, error, info};

use tokio::sync::{broadcast, mpsc};
use tokio::time::{sleep, Duration};

pub mod actors;
pub mod cli;
pub mod errors;
pub mod handlers;
pub mod parsers;
pub mod protocol;
pub mod rdb;
pub mod resp;

use crate::cli::Cli;

use crate::handlers::{
    config_command::ConfigCommandActorHandle,
    info_command::InfoCommandActorHandle,
    // replication::ReplicationActorHandle,
    request_processor::RequestProcessorActorHandle,
    set_command::SetCommandActorHandle,
};

use crate::protocol::{ConfigCommandParameter, InfoCommandParameter};

// use env_logger::Env;
// use log::{debug, info};
// use resp::{encode_slice, Decoder};

// use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use async_channel;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // construct a subscriber that prints formatted traces to stdout
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    // use that subscriber to process traces emitted after this point
    tracing::subscriber::set_global_default(subscriber)?;

    // Setup the logging framework
    // let env = Env::default()
    //     .filter_or("LOG_LEVEL", "info")
    //     .write_style_or("LOG_STYLE", "always");

    // env_logger::init_from_env(env);

    let cli = Cli::parse();

    // cli.port comes from cli.rs; default is 6379
    let socket_address = std::net::SocketAddr::from(([0, 0, 0, 0], cli.port));

    let listener = TcpListener::bind(socket_address).await?;

    tracing::info!("Redis is running on port {}.", cli.port);

    // Get a handle to the set actor, one per redis. This starts the actor.
    let set_command_actor_handle = SetCommandActorHandle::new();

    // Get a handle to the info actor, one per redis. This starts the actor.
    let info_command_actor_handle = InfoCommandActorHandle::new();

    // Get a handle to the config actor, one per redis. This starts the actor.
    let config_command_actor_handle = ConfigCommandActorHandle::new();

    // Get a handle to the replication actor, one per redis. This starts the actor.
    // let _replication_actor_handle = ReplicationActorHandle::new();

    // this is where decoded resp values are sent for processing
    let request_processor_actor_handle = RequestProcessorActorHandle::new();

    // let mut config_dir: String = "".to_string();

    // Create a multi-producer, single-consumer channel to send expiration messages.
    // The channel capacity is set to 9600.
    let (expire_tx, mut expire_rx) = mpsc::channel::<SetCommandParameter>(9600);

    // An async multi-producer multi-consumer channel,
    // where each message can be received by only one of all existing consumers.
    let (tcp_msgs_tx, tcp_msgs_rx) = async_channel::unbounded();

    // Create a multi-producer, single-consumer channel to recv messages from the master.
    // NOTE: these messages are replies coming back from the master, not commands to the master.
    // Used by handshake() to receive ack +OK replies from the master.
    let (master_tx, master_rx) = mpsc::channel::<String>(9600);

    // Setup a tokio broadcast channel to communicate all writeable updates to all the replicas.
    // This is a multi-producer, multi-consumer channel.
    // Both the replica_tx Sender and replica_rx Receiver are cloned and passed to the client handler.
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
        config_command_actor_handle
            .set_value(ConfigCommandParameter::Dir, dir)
            .await;
        // tracing::debug!("Config directory: {dir}");
        // config_dir = dir.to_string();
    }

    if let Some(dbfilename) = cli.dbfilename.as_deref() {
        config_command_actor_handle
            .set_value(
                ConfigCommandParameter::DbFilename,
                &dbfilename.to_string_lossy(),
            )
            .await;
        // debug!("Config db filename: {}", dbfilename.display());
        // let config_dbfilename = dbfilename.to_string_lossy().to_string();

        config_command_actor_handle
            .import_config(
                set_command_actor_handle.clone(), // need to pass this to get direct access to the redis db
                None,                             // load from disk
                expire_tx.clone(), // need to pass this to unlock expirations on config file load
            )
            .await;

        // debug!(
        //     "Config db dir: {} filename: {}",
        //     config_dir, config_dbfilename
        // );
    }

    // initialize to being a master, override if we are a replica
    let mut info_data: ReplicationSectionData = ReplicationSectionData::new(ServerRole::Master);

    // see if we need to override it
    if let Some(replica) = cli.replicaof.as_deref() {
        let master_host_port_combo = replica.replace(" ", ":");

        // We can pass a string to TcpStream::connect, so no need to create SocketAddr
        // replication_actor_handle
        //     .connect_to_master(master_host_port_combo)
        //     .await;

        let stream = TcpStream::connect(&master_host_port_combo)
            .await
            .expect("Failed to establish connection to master.");

        // Must clone the actors handlers because tokio::spawn move will grab everything.
        let set_command_handler_clone = set_command_actor_handle.clone();
        let config_command_handler_clone = config_command_actor_handle.clone();
        let info_command_actor_handle_clone = info_command_actor_handle.clone();
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
                info_command_actor_handle_clone,
                request_processor_actor_handle_clone,
                expire_tx_clone,
                tcp_msgs_rx_clone,
                master_tx_clone,
                replica_tx_clone, // used to send replication messages to the replica
            )
            .await
        });

        handshake(tcp_msgs_tx.clone(), master_rx, cli.port.to_string()).await?;

        debug!(
            "Handshake completed, connected to master at {}.",
            master_host_port_combo
        );
        // set the role to slave
        info_data = ReplicationSectionData::new(ServerRole::Slave);
        // use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    }

    info_command_actor_handle
        .set_value(InfoCommandParameter::Replication, info_data)
        .await;

    // we must clone the handler to the SetActor because the whole thing is being moved into an expiry handle loop
    let set_command_handle_clone = set_command_actor_handle.clone();

    // This will listen for messages on the expire_tx channel.
    // Once a msg comes, it'll see if it's an expiry message and if it is,
    // will move everything and spawn off a thread to expire in the future.
    let _expiry_handle_loop = tokio::spawn(async move {
        // Start receiving messages from the channel by calling the recv method of the Receiver endpoint.
        // This method blocks until a message is received.
        while let Some(msg) = expire_rx.recv().await {
            // We may or may not need to expire a value. If not, no big deal, just wait again.
            if let Some(duration) = msg.expire {
                match duration {
                    // reminder: seconds are Unix timestamps
                    protocol::SetCommandExpireOption::EX(seconds) => {
                        // Must clone again because we're about to move this into a dedicated sleep thread.
                        let expire_command_handler_clone = set_command_handle_clone.clone();
                        let _expiry_handle = tokio::spawn(async move {
                            // get the current system time
                            let now = SystemTime::now();

                            // how many seconds have elapsed since beginning of time
                            let duration_since_epoch = now
                                .duration_since(UNIX_EPOCH)
                                // .ok()
                                .expect("Failed to calculate duration since epoch"); // Handle potential error

                            // i64 since it is possible for this to be negative, i.e. past time expiration
                            let expiry_time =
                                seconds as i64 - duration_since_epoch.as_secs() as i64;

                            // we sleep if this is NON negative
                            if !expiry_time < 0 {
                                debug!("Sleeping for {} seconds.", expiry_time);
                                sleep(Duration::from_secs(expiry_time as u64)).await;
                            }

                            // Fire off a command to the handler to remove the value immediately.
                            expire_command_handler_clone.delete_value(&msg.key).await;
                        });
                    }
                    protocol::SetCommandExpireOption::PX(milliseconds) => {
                        // Must clone again because we're about to move this into a dedicated sleep thread.
                        let command_handler_expire_clone = set_command_handle_clone.clone();
                        let _expiry_handle = tokio::spawn(async move {
                            // get the current system time
                            let now = SystemTime::now();

                            // how many milliseconds have elapsed since beginning of time
                            let duration_since_epoch = now
                                .duration_since(UNIX_EPOCH)
                                // .ok()
                                .expect("Failed to calculate duration since epoch"); // Handle potential error

                            // i64 since it is possible for this to be negative, i.e. past time expiration
                            let expiry_time =
                                milliseconds as i64 - duration_since_epoch.as_millis() as i64;

                            // we sleep if this is NON negative
                            if !expiry_time < 0 {
                                debug!("Sleeping for {} milliseconds.", expiry_time);
                                sleep(Duration::from_millis(expiry_time as u64)).await;
                            }

                            debug!("Expiring {:?}", msg);

                            // Fire off a command to the handler to remove the value immediately.
                            command_handler_expire_clone.delete_value(&msg.key).await;
                        });
                    }
                    protocol::SetCommandExpireOption::EXAT(_) => todo!(),
                    protocol::SetCommandExpireOption::PXAT(_) => todo!(),
                    protocol::SetCommandExpireOption::KEEPTTL => todo!(),
                }
            }
        }
    });

    loop {
        // Asynchronously wait for an inbound TcpStream.
        let (stream, _) = listener.accept().await?;

        // Must clone the actors handlers because tokio::spawn move will grab everything.
        let set_command_handler_clone = set_command_actor_handle.clone();
        let config_command_handler_clone = config_command_actor_handle.clone();
        let info_command_actor_handle_clone = info_command_actor_handle.clone();
        let request_processor_actor_handle_clone = request_processor_actor_handle.clone();

        let expire_tx_clone = expire_tx.clone();
        // let tcp_msgs_rx_clone = tcp_msgs_rx.clone();
        let master_tx_clone = master_tx.clone();

        let replica_tx_clone = replica_tx.clone();
        let replica_rx_subscriber = replica_tx.subscribe();
        //tcp_msgs_rx_clone.close(); // close the channel since redis as a server will never read it

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
                replica_rx_subscriber,
            )
            .await
        });
    }
}

// #[tracing::instrument]
async fn handshake(
    tcp_msgs_tx: async_channel::Sender<RespValue>,
    mut master_rx: mpsc::Receiver<String>,
    port: String,
) -> anyhow::Result<()> {
    // begin the replication handshake
    // STEP 1: PING
    let ping = RespValue::array_from_slice(&["PING"]);

    // STEP 2: REPLCONF listening-port <PORT>
    // initialize the empty array
    let repl_conf_listening_port =
        RespValue::array_from_slice(&["REPLCONF", "listening-port", &port]);

    // STEP 3: REPLCONF capa psync2
    // initialize the empty array
    let repl_conf_capa = RespValue::array_from_slice(&["REPLCONF", "capa", "psync2"]);

    // STEP 4: send the PSYNC ? -1
    let psync = RespValue::array_from_slice(&["PSYNC", "?", "-1"]);

    // let handshake_commands = vec![repl_conf_listening_port, repl_conf_capa, psync];

    // send the ping
    tcp_msgs_tx.send(ping).await?;
    // wait for a reply from the master before proceeding
    let reply = master_rx.recv().await;
    debug!("HANDSHAKE: master replied to ping {:?}", reply);

    // send the REPLCONF listening-port <PORT>
    tcp_msgs_tx.send(repl_conf_listening_port).await?;
    // wait for a reply from the master before proceeding
    let reply = master_rx.recv().await;
    debug!("HANDSHAKE: master replied {:?}", reply);

    // send the REPLCONF capa psync2
    tcp_msgs_tx.send(repl_conf_capa).await?;
    // wait for a reply from the master before proceeding
    let reply = master_rx.recv().await;
    debug!("HANDSHAKE: master replied {:?}", reply);

    // send the PSYNC ? -1
    tcp_msgs_tx.send(psync).await?;
    // no waiting any more, we are done with the handshake

    // for command in handshake_commands.into_iter() {
    //     // Send the value.
    //     // Encodes a slice of string to RESP binary buffer.
    //     // It is used to create a request command on redis client.
    //     // https://docs.rs/resp/latest/resp/fn.encode_slice.html
    //     tcp_msgs_tx.send(command).await?;

    //     // wait for the +OK reply from the master before proceeding
    //     let reply = master_rx.recv().await;
    //     debug!("HANDSHAKE: master replied {:?}", reply);
    // }

    debug!("Handshake completed.");

    Ok(())
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
    info_command_actor_handle: InfoCommandActorHandle,
    request_processor_actor_handle: RequestProcessorActorHandle,
    expire_tx: mpsc::Sender<SetCommandParameter>,
    master_tx: mpsc::Sender<String>, // passthrough to request_processor_actor_handle
    replica_tx: broadcast::Sender<RespValue>, // used to send replication messages to the replica
    mut replica_rx: broadcast::Receiver<RespValue>, // used to receive replication messages from the master
) -> anyhow::Result<()> {
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
                        tracing::info!("Client reader returned RESP: {:?}", request);
                        if let Some(processed_values) = request_processor_actor_handle
                            .process_request(
                                request,
                                set_command_actor_handle.clone(),
                                config_command_actor_handle.clone(),
                                info_command_actor_handle.clone(),
                                expire_tx.clone(),
                                master_tx.clone(), // these are ack +OK replies from the master back to handshake()
                                Some(replica_tx.clone()), // used to send replication messages to the replica
                                Some(client_or_replica_tx.clone()), // used to update replica status
                            )
                            .await
                        {
                            tracing::info!("Sending replies to client: {:?}", processed_values);
                            // iterate over processed_value and send each one to the client
                            for value in processed_values.iter() {
                                debug!("Sending response to client: {:?}", value);
                                let _ = writer.send(value.clone()).await?;
                            }
                        }
                    }
                    Err(e) => {
                        error!("Unable to decode request from client: {e}");
                    }
                }
            }
         msg = replica_rx.recv() => {
            match msg {
                Ok(msg) => {
                    // Send replication messages only to replicas, not to other clients.
                    if am_i_replica {
                        tracing::info!("Sending message to replica: {:?}", msg);
                        let _ = writer.send(msg).await?;
                        // writer.flush().await?;
                    } else {
                        debug!("Not sending replication message to non-replica client.");
                    }
                }
                Err(e) => {
                    error!("Something unexpected happened: {e}");
                }
            }
         }
         Some(msg) = client_or_replica_rx.recv() => {
            // // if let Some(client_type) = msg {
            //     // check to make sure this client is a replica, not a redis-cli client.
            //     // if it is a redis-cli client, we don't want to send replication messages to it.
            //     // we only want to send replication messages to replicas.
                am_i_replica  = msg;

                debug!("Updated client replica status to {:?}", am_i_replica);
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
    info_command_actor_handle: InfoCommandActorHandle,
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
            // Read data from the stream, n is the number of bytes read
            Some(msg) = reader.next() => {
                match msg {
                    Ok(request) => {
                        tracing::info!("Master reader returned RESP: {:?}", request);
                        // send the request to the request processor actor
                        if let Some(processed_value) = request_processor_actor_handle
                            .process_request(
                                request.clone(),
                                set_command_actor_handle.clone(),
                                config_command_actor_handle.clone(),
                                info_command_actor_handle.clone(),
                                expire_tx.clone(),
                                master_tx.clone(), // these are ack +OK replies from the master back to handshake()
                                Some(replica_tx.clone()), // this enables daisy chaining of replicas to other replicas
                                None, // connections to master cannot update replica status
                            )
                            .await
                        {
                             // get the current replication data.
                             let mut current_replication_data =
                             info_command_actor_handle
                                 .get_value(InfoCommandParameter::Replication)
                                 .await
                                 .expect(
                                     "Unable to get current replication data.",
                                 );

                             // we need to convert the request to a RESP string to count the bytes.
                             let value_as_string = request.to_encoded_string().expect("Failed to encode request as a string.");

                             // calculate how many bytes are in the value_as_string
                             let value_as_string_bytes = value_as_string.len() as i16;

                             // extract the current offset value.
                             let current_offset = current_replication_data.master_repl_offset;

                             // update the offset value.
                             current_replication_data.master_repl_offset = current_offset + value_as_string_bytes;

                             // update the offset value in the info actor.
                             info_command_actor_handle
                                 .set_value(
                                     InfoCommandParameter::Replication,
                                     current_replication_data,
                                 )
                                 .await;

                            debug!("Only REPLCONF ACK commands are sent back to master: {:?}", processed_value);
                            // iterate over processed_value and send each one to the client
                            for value in processed_value.iter() {
                                info!("Sending response to master: {:?}", value);
                                let _ = writer.send(value.clone()).await?;
                            }
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
                    tracing::info!("Sending message to master: {:?}", msg);
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
