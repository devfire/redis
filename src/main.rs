// use anyhow;

pub mod errors;
pub mod handlers;
pub mod protocol;

// use std::string::ToString;

use crate::handlers::handler;
use crate::protocol::RedisCommand;

use env_logger::Env;
use log::{info, warn};
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

async fn process(stream: TcpStream) {
    let (mut reader, mut writer) = stream.into_split();

    //let mut reader = BufReader::new(reader);

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
            break;
        }

        info!("Read {} bytes", n);

        // https://docs.rs/resp/latest/resp/struct.Decoder.html
        let mut decoder = Decoder::new(std::io::BufReader::new(buf.as_slice()));

        let request: resp::Value = decoder.decode().expect("Unable to decode request");
        info!("Received {:?}", request);

        match request {
            Value::Null => todo!(),
            Value::NullArray => todo!(),
            Value::String(_) => todo!(),
            Value::Error(_) => todo!(),
            Value::Integer(_) => todo!(),
            Value::Bulk(_) => todo!(),
            Value::BufBulk(_) => todo!(),
            Value::Array(mut array) => {
                // info!("Array received {:?}", array);
                // need to recast it as &mut because handler() manipulates the array of Values
                let array = &mut array;
                while !array.is_empty() {
                    // we are popping off the first value, going through the whole array one by one.
                    // TODO: Nested arrays are not supported yet.
                    let top_value = array.remove(0);

                    if let Some(parsed_command) =
                        handler(top_value, array).expect("Unable to identify command.")
                    {
                        info!("Parsed command: {}", parsed_command);
                        match parsed_command {
                            RedisCommand::Ping => {
                                // Encode the value to RESP binary buffer.
                                let response = Value::String("PONG".to_string()).encode();
                                let _ = writer
                                    .write_all(&response)
                                    .await
                                    .expect("Unable to write TCP");
                            }
                            RedisCommand::Echo(message) => {
                                if let Some(msg) = message {
                                    // Encode the value to RESP binary buffer.
                                    let response = msg.encode();

                                    let _ = writer
                                        .write_all(&response)
                                        .await
                                        .expect("Unable to write TCP");
                                }
                            }
                            RedisCommand::Command(_) => {
                                // Encode the value to RESP binary buffer.
                                let response = Value::String("+OK".to_string()).encode();
                                let _ = writer
                                    .write_all(&response)
                                    .await
                                    .expect("Unable to write TCP");
                            }
                            RedisCommand::Set(_) => todo!(),
                        }
                    }
                }
            }
        }
    }
}
