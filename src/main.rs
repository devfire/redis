use anyhow::Result;
use clap::Parser;
use protocol::SetCommandParameters;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

pub mod actors;
pub mod cli;
pub mod errors;
pub mod handlers;
pub mod messages;
pub mod parsers;
pub mod protocol;
pub mod rdb;

use crate::cli::Cli;
use crate::errors::RedisError;

// Handlers for all the actors defined
use crate::{
    handlers::{config_command::ConfigCommandActorHandle, set_command::SetCommandActorHandle},
    parsers::parse_command,
};

use crate::protocol::{ConfigCommandParameters, RedisCommand};

use env_logger::Env;
use log::info;
use resp::{Decoder, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Setup the logging framework
    let env = Env::default()
        .filter_or("LOG_LEVEL", "info")
        .write_style_or("LOG_STYLE", "always");

    env_logger::init_from_env(env);

    let cli = Cli::parse();

    // Get a handle to the set actor, one per redis. This starts the actor.
    let set_command_actor_handle = SetCommandActorHandle::new();

    // Get a handle to the config actor, one per redis. This starts the actor.
    let config_command_actor_handle = ConfigCommandActorHandle::new();
    let mut config_dbfilename: String = "".to_string();
    let mut config_dir: String = "".to_string();

    // Check the value provided by the arguments.
    // Store the config values if they are valid.
    // NOTE: If nothing is passed, cli.rs has the default values for clap.
    if let Some(dir) = cli.dir.as_deref() {
        config_command_actor_handle
            .set_value(ConfigCommandParameters::Dir, dir)
            .await;
        info!("Config directory: {dir}");
        config_dir = dir.to_string();
    }

    if let Some(dbfilename) = cli.dbfilename.as_deref() {
        config_command_actor_handle
            .set_value(
                ConfigCommandParameters::DbFilename,
                &dbfilename.to_string_lossy(),
            )
            .await;
        info!("Config db filename: {}", dbfilename.display());
        config_dbfilename = dbfilename.to_string_lossy().to_string();
    }

    info!(
        "Config db dir: {} filename: {}",
        config_dir, config_dbfilename
    );

    let listener = TcpListener::bind("0.0.0.0:6379").await?;

    info!("Redis is running.");

    config_command_actor_handle
        .load_config(
            &config_dir,
            &config_dbfilename,
            set_command_actor_handle.clone(), // need to pass this to get direct access to the redis db
        )
        .await;

    loop {
        // Asynchronously wait for an inbound TcpStream.
        let (stream, _) = listener.accept().await?;

        // Must clone the actors handlers because tokio::spawn move will grab everything.
        let set_command_handler_clone = set_command_actor_handle.clone();
        let config_command_handler_clone = config_command_actor_handle.clone();

        // Spawn our handler to be run asynchronously.
        // A new task is spawned for each inbound socket.  The socket is moved to the new task and processed there.
        tokio::spawn(async {
            process(
                stream,
                set_command_handler_clone,
                config_command_handler_clone,
            )
            .await
        });
    }
}

async fn process(
    stream: TcpStream,
    set_command_actor_handle: SetCommandActorHandle,
    config_command_actor_handle: ConfigCommandActorHandle,
) -> Result<()> {
    // Split the TCP stream into a reader and writer.
    // Create a multi-producer, single-consumer channel to send expiration messages.
    // The channel capacity is set to 9600.
    let (mut reader, mut writer) = stream.into_split();

    let (expire_tx, mut expire_rx) = mpsc::channel::<SetCommandParameters>(9600);

    // we must clone the handler to the SetActor because the whole thing is being moved into an expiry handle loop
    let expire_command_handler_clone = set_command_actor_handle.clone();

    // this will listen for messages on the expire_tx channel.
    // Once a msg comes, it'll see if it's an expiry message and if it is, will move everything and spawn off a thread to expire in the future.
    let _expiry_handle_loop = tokio::spawn(async move {
        // Start receiving messages from the channel by calling the recv method of the Receiver endpoint.
        // This method blocks until a message is received.
        while let Some(msg) = expire_rx.recv().await {
            // We may or may not need to expire a value. If not, no big deal, just wait again.
            if let Some(duration) = msg.expire {
                match duration {
                    protocol::SetCommandExpireOption::EX(seconds) => {
                        // Must clone again because we're about to move this into a dedicated sleep thread.
                        let expire_command_handler_clone = expire_command_handler_clone.clone();
                        let _expiry_handle = tokio::spawn(async move {
                            sleep(Duration::from_secs(seconds as u64)).await;
                            info!("Expiring {:?}", msg);

                            // Fire off a command to the handler to remove the value immediately.
                            expire_command_handler_clone.delete_value(&msg.key).await;
                        });
                    }
                    protocol::SetCommandExpireOption::PX(milliseconds) => {
                        // Must clone again because we're about to move this into a dedicated sleep thread.
                        let command_handler_expire_clone = expire_command_handler_clone.clone();
                        let _expiry_handle = tokio::spawn(async move {
                            sleep(Duration::from_millis(milliseconds as u64)).await;
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
        // info!("Received {:?}", request);

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
                // reason is, nom parser operates on str not Vec<Value>. so sending request as an encoded string,
                // we can avoid recreating the original RESP array and just encode the request.
                let request_as_encoded_string = request.to_encoded_string()?;

                info!("RESP request: {:?}", request_as_encoded_string);

                // let return_value =
                //     processor_handle.process_value(parse_command(&request_as_encoded_string));
                // let _ = writer.write_all(&return_value).await?;

                // OK, what we get back from the parser is a command with all of its parameters.
                // Now we get to do stuff with the command.
                // If it's something simple like PING, we handle it immediately and return.
                // If not, we get an actor handle and send it to the actor to process.
                match parse_command(&request_as_encoded_string) {
                    Ok((_remaining_bytes, RedisCommand::Ping)) => {
                        // Encode the value to RESP binary buffer.
                        let response = Value::String("PONG".to_string()).encode();
                        let _ = writer.write_all(&response).await?;
                        writer.flush().await?;
                    }
                    // return Err(RedisError::ParseFailure.into()) closes the connection so let's not do that
                    Err(_) => {
                        let err_response =
                            Value::Error(RedisError::ParseFailure.to_string()).encode();

                        let _ = writer.write_all(&err_response).await?;
                        writer.flush().await?;
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

                        // don't even bother checking whether there is an expiry field or not.
                        // Reason is, this will always send to the channel and the while loop will figure out if there's an expiry field or not.
                        // if not, this is a noop.
                        // expire_tx
                        //     .send(set_parameters.clone())
                        //     .await
                        //     .expect("Unable to start the expiry thread.");
                        // Encode the value to RESP binary buffer.
                        let response = Value::String("OK".to_string()).encode();
                        let _ = writer.write_all(&response).await?;
                        writer.flush().await?;
                    }
                    Ok((_, RedisCommand::Get(key))) => {
                        // we may or may not get a value for the supplied key.
                        // if we do, we return it. If not, we encode Null and send that back.
                        if let Some(value) = set_command_actor_handle.get_value(&key).await {
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
                        writer.flush().await?;
                    }
                    Ok((_, RedisCommand::Strlen(key))) => {
                        // we may or may not get a value for the supplied key.
                        // if we do, we return the length. If not, we encode 0 and send that back.
                        // https://redis.io/commands/strlen/
                        if let Some(value) = set_command_actor_handle.get_value(&key).await {
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
                        if let Some(value) = config_command_actor_handle.get_value(config_key).await
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
                        if let Some(keys) = set_command_actor_handle.get_keys(&pattern).await {
                            for key in keys {
                                let response = Value::Bulk(key);
                                keys_collection.push(response);
                            }
                        } else {
                            let response = Value::Null; // key does not exist, return nil
                            keys_collection.push(response);
                            // let _ = writer.write_all(&response).await?;
                        }

                        info!("Returning keys: {:?}", keys_collection);
                        let response = Value::Array(keys_collection).encode();

                        let _ = writer.write_all(&response).await?;
                        writer.flush().await?;
                    }
                }
            }
        }
    }
}
