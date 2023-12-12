use anyhow::Result;

pub mod actors;
pub mod errors;
mod handlers;
pub mod messages;
pub mod protocol;

// use std::string::ToString;

use crate::errors::RedisError;
use crate::handlers::resp_array::parse_command;
use crate::handlers::set_command::SetCommandActorHandle;
use crate::protocol::RedisCommand;

use env_logger::Env;
use log::{info, warn};
use resp::{encode, encode_slice, Decoder, Value};
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

    info!("Redis is running.");

    loop {
        // Asynchronously wait for an inbound TcpStream.
        let (stream, _) = listener.accept().await?;

        // Spawn our handler to be run asynchronously.
        // A new task is spawned for each inbound socket.  The socket is
        // moved to the new task and processed there.
        tokio::spawn(async move {
            process(stream).await;
        });
    }
}

async fn process(stream: TcpStream) -> Result<()> {
    let (mut reader, mut writer) = stream.into_split();

    // Get a handle to the set actor, one per process. This starts the actor.
    let set_command_actor_handle = SetCommandActorHandle::new();

    loop {
        // Buffer to store the data
        let mut buf = vec![0; 1024];

        // Read data from the stream, n is the number of bytes read
        let n = reader
            .read(&mut buf)
            .await
            .expect("Unable to read from buffer");

        if n == 0 {
            warn!("Error: buffer empty");
            return Err(RedisError::ParseFailure.into());
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
                // info!("Array received {:?}", array);
                // need to recast it as &mut because handler() manipulates the array of Values
                // let array_encoded_string = request.encode(); // to_encoded_string()?;

                // info!("Received: {:?}", request.to_encoded_string());
                // Let's figure out what command we got.
                //
                // let (_,parsed_command) = parse_command(&request.clone().encode())?;

                // info!("Parsed command: {}", parsed_command);

                let request_copy = request.clone().to_encoded_string()?.to_uppercase();

                info!("Encoded: {:?}", request_copy);

                // OK, what we get back from the parser is a command with all of its parameters.
                // Now we get to do stuff with the command.
                // If it's something simple like PING, we handle it immediately and return.
                // If not, we get an actor handle and send it to the actor to process.
                match parse_command(&request_copy) {
                    Ok((_remaining_bytes, RedisCommand::Ping)) => {
                        // Encode the value to RESP binary buffer.
                        let response = Value::String("PONG".to_string()).encode();
                        let _ = writer
                            .write_all(&response)
                            .await
                            .expect("Unable to write TCP");
                    }
                    Err(_) => todo!(),
                    Ok((_, RedisCommand::Echo(message))) => {
                        // if let Some(msg) = message {
                        // Encode the value to RESP binary buffer.
                        let response = Value::String(message).encode();

                        let _ = writer
                            .write_all(&response)
                            .await
                            .expect("Unable to write TCP");
                        // }
                    }
                    Ok((_, RedisCommand::Command)) => {
                        // Encode the value to RESP binary buffer.
                        let response = Value::String("+OK".to_string()).encode();
                        let _ = writer
                            .write_all(&response)
                            .await
                            .expect("Unable to write TCP.");
                    } // RedisCommand::Set(key_value_pair) => {
                      //     // key_value_pair is an Option, so we have to Some() it.
                      //     if let Some(kvp) = key_value_pair {
                      //         set_command_actor_handle.set_value(kvp).await
                      //     }

                      //     // Encode the value to RESP binary buffer.
                      //     let response = Value::String("OK".to_string()).encode();
                      //     let _ = writer
                      //         .write_all(&response)
                      //         .await
                      //         .expect("Unable to write TCP");
                      // }

                      // RedisCommand::Get(key) => {
                      //     if let Some(input_key) = key {
                      //         let value = set_command_actor_handle.get_value(&input_key).await;

                      //         // Encode the value to RESP binary buffer.
                      //         let response = value.encode();
                      //         let _ = writer
                      //             .write_all(&response)
                      //             .await
                      //             .expect("Unable to write TCP");
                      //     }
                      // }
                }
            }
        }
    }
}
