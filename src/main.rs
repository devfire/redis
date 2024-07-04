use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use clap::Parser;

use protocol::{InfoSectionData, RedisCommand, ServerRole, SetCommandParameter};

use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

pub mod actors;
pub mod cli;
pub mod errors;
pub mod handlers;
pub mod parsers;
pub mod protocol;
pub mod rdb;

use crate::cli::Cli;

use crate::handlers::{
    config_command::ConfigCommandActorHandle, info_command::InfoCommandActorHandle,
    replication::ReplicationActorHandle, request_processor::RequestProcessorActorHandle,
    set_command::SetCommandActorHandle,
};

use crate::protocol::{ConfigCommandParameter, InfoCommandParameter};

use env_logger::Env;
use log::info;
use resp::Decoder;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Setup the logging framework
    let env = Env::default()
        .filter_or("LOG_LEVEL", "info")
        .write_style_or("LOG_STYLE", "always");

    env_logger::init_from_env(env);

    let cli = Cli::parse();

    // Get a handle to the set actor, one per redis. This starts the actor.
    let set_command_actor_handle = SetCommandActorHandle::new();

    // Get a handle to the info actor, one per redis. This starts the actor.
    let info_command_actor_handle = InfoCommandActorHandle::new();

    // Get a handle to the config actor, one per redis. This starts the actor.
    let config_command_actor_handle = ConfigCommandActorHandle::new();

    // Get a handle to the replication actor, one per redis. This starts the actor.
    let replication_actor_handle = ReplicationActorHandle::new();

    // this is where decoded resp values are sent for processing
    let request_processor_actor_handle = RequestProcessorActorHandle::new();

    let mut config_dir: String = "".to_string();

    // Create a multi-producer, single-consumer channel to send expiration messages.
    // The channel capacity is set to 9600.
    let (expire_tx, mut expire_rx) = mpsc::channel::<SetCommandParameter>(9600);

    // Check the value provided by the arguments.
    // Store the config values if they are valid.
    // NOTE: If nothing is passed, cli.rs has the default values for clap.
    if let Some(dir) = cli.dir.as_deref() {
        config_command_actor_handle
            .set_value(ConfigCommandParameter::Dir, dir)
            .await;
        info!("Config directory: {dir}");
        config_dir = dir.to_string();
    }

    if let Some(dbfilename) = cli.dbfilename.as_deref() {
        config_command_actor_handle
            .set_value(
                ConfigCommandParameter::DbFilename,
                &dbfilename.to_string_lossy(),
            )
            .await;
        info!("Config db filename: {}", dbfilename.display());
        let config_dbfilename = dbfilename.to_string_lossy().to_string();

        config_command_actor_handle
            .load_config(
                &config_dir,
                &config_dbfilename,
                set_command_actor_handle.clone(), // need to pass this to get direct access to the redis db
                expire_tx.clone(), // need to pass this to unlock expirations on config file load
            )
            .await;

        info!(
            "Config db dir: {} filename: {}",
            config_dir, config_dbfilename
        );
    }

    // initialize to being a master, override if we are a replica
    let mut info_data: InfoSectionData = InfoSectionData::new(ServerRole::Master);

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
        tokio::spawn(async move {
            process(
                stream,
                set_command_handler_clone,
                config_command_handler_clone,
                info_command_actor_handle_clone,
                request_processor_actor_handle_clone,
                expire_tx_clone,
            )
            .await
        });

        // set the role to slave
        info_data = InfoSectionData::new(ServerRole::Slave);
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
                                info!("Sleeping for {} seconds.", expiry_time);
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
                                info!("Sleeping for {} milliseconds.", expiry_time);
                                sleep(Duration::from_millis(expiry_time as u64)).await;
                            }

                            info!("Expiring {:?}", msg);

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

    // cli.port comes from cli.rs; default is 6379
    let socket_address = std::net::SocketAddr::from(([0, 0, 0, 0], cli.port));

    let listener = TcpListener::bind(socket_address).await?;

    info!("Redis is running on port {}.", cli.port);

    loop {
        // Asynchronously wait for an inbound TcpStream.
        let (stream, _) = listener.accept().await?;

        // Must clone the actors handlers because tokio::spawn move will grab everything.
        let set_command_handler_clone = set_command_actor_handle.clone();
        let config_command_handler_clone = config_command_actor_handle.clone();
        let info_command_actor_handle_clone = info_command_actor_handle.clone();
        let request_processor_actor_handle_clone = request_processor_actor_handle.clone();

        let expire_tx_clone = expire_tx.clone();

        // Spawn our handler to be run asynchronously.
        // A new task is spawned for each inbound socket.  The socket is moved to the new task and processed there.
        tokio::spawn(async move {
            process(
                stream,
                set_command_handler_clone,
                config_command_handler_clone,
                info_command_actor_handle_clone,
                request_processor_actor_handle_clone,
                expire_tx_clone,
            )
            .await
        });
    }
}

async fn process(
    stream: TcpStream,
    set_command_actor_handle: SetCommandActorHandle,
    config_command_actor_handle: ConfigCommandActorHandle,
    info_command_actor_handle: InfoCommandActorHandle,
    request_processor_actor_handle: RequestProcessorActorHandle,
    expire_tx: mpsc::Sender<SetCommandParameter>,
) -> Result<()> {
    // Split the TCP stream into a reader and writer.
    let (mut reader, mut writer) = stream.into_split();

    loop {
        // Buffer to store the data
        let mut buf = vec![0; 1024];

        // Read data from the stream, n is the number of bytes read
        let n = reader
            .read(&mut buf)
            .await
            .expect("Unable to read from buffer");

        if n == 0 {
            info!("Empty buffer.");
            return Ok(()); // we don't want to return an error since an empty buffer is not a problem.
                           // return Err(RedisError::ParseFailure.into());
        }

        // info!("Read {} bytes", n);

        // https://docs.rs/resp/latest/resp/struct.Decoder.html
        let mut decoder = Decoder::new(std::io::BufReader::new(buf.as_slice()));

        let request: resp::Value = decoder.decode().expect("Unable to decode request");

        // send the request to the request processor actor
        if let Some(processed_value) = request_processor_actor_handle
            .process_request(
                request,
                set_command_actor_handle.clone(),
                config_command_actor_handle.clone(),
                info_command_actor_handle.clone(),
                expire_tx.clone(),
            )
            .await
        {
            // encode the Value as a binary Vec
            let encoded_value = resp::encode(&processed_value);
            let _ = writer.write_all(&encoded_value).await?;
            writer.flush().await?;
        }
    } // end of loop
}
