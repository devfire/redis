use anyhow::Result;
use protocol::SetCommandParameters;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

pub mod actors;
pub mod errors;
mod handlers;
pub mod messages;
pub mod parsers;
pub mod protocol;

// use std::string::ToString;

use crate::errors::RedisError;
use crate::protocol::RedisCommand;
use crate::{handlers::set_command::SetCommandActorHandle, parsers::parse_command};

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

    let listener = TcpListener::bind("0.0.0.0:6379").await?;

    // Get a handle to the set actor, one per redis. This starts the actor.
    let set_command_actor_handle = SetCommandActorHandle::new();

    info!("Redis is running.");

    loop {
        // Asynchronously wait for an inbound TcpStream.
        let (stream, _) = listener.accept().await?;


        // Must clone the handler because tokio::spawn move will grab everything.
        let set_command_handler_clone = set_command_actor_handle.clone();

        // Spawn our handler to be run asynchronously.
        // A new task is spawned for each inbound socket.  The socket is moved to the new task and processed there.
        tokio::spawn(async move {
            process(stream, set_command_handler_clone)
                .await
                .expect("Failed to spawn process thread");
        });
    }
}

async fn process(stream: TcpStream, set_command_actor_handle: SetCommandActorHandle) -> Result<()> {
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
                    protocol::SetCommandExpireOption::EX(_) => todo!(),
                    protocol::SetCommandExpireOption::PX(milliseconds) => {
                        // Must clone again because we're about to move this into a dedicated sleep thread.
                        let expire_command_handler_clone = expire_command_handler_clone.clone();
                        let _expiry_handle = tokio::spawn(async move {
                            sleep(Duration::from_millis(milliseconds as u64)).await;
                            info!("Expiring {:?}", msg);

                            // Fire off a command to the handler to remove the value immediately.
                            expire_command_handler_clone.expire_value(msg.clone()).await;
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

                info!("Encoded: {:?}", request_as_encoded_string);

                // OK, what we get back from the parser is a command with all of its parameters.
                // Now we get to do stuff with the command.
                // If it's something simple like PING, we handle it immediately and return.
                // If not, we get an actor handle and send it to the actor to process.
                match parse_command(&request_as_encoded_string) {
                    Ok((_remaining_bytes, RedisCommand::Ping)) => {
                        // Encode the value to RESP binary buffer.
                        let response = Value::String("PONG".to_string()).encode();
                        let _ = writer.write_all(&response).await?;
                    }
                    // return Err(RedisError::ParseFailure.into()) closes the connection so let's not do that
                    Err(_) => {
                        let err_response =
                            Value::Error(RedisError::ParseFailure.to_string()).encode();

                        let _ = writer.write_all(&err_response).await?;
                    }
                    Ok((_, RedisCommand::Echo(message))) => {
                        // Encode the value to RESP binary buffer.
                        let response = Value::String(message).encode();

                        let _ = writer.write_all(&response).await?;
                    }
                    Ok((_, RedisCommand::Command)) => {
                        // Encode the value to RESP binary buffer.
                        let response = Value::String("+OK".to_string()).encode();
                        let _ = writer.write_all(&response).await?;
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
                        let _ = writer.write_all(&response).await?;
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
                }
            }
        }
    }
}
