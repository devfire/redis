use codec::RespCodec;
use env_logger::Env;

use errors::RedisError;
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use protocol::RespFrame;

use tokio::net::{TcpListener, TcpStream};

use tokio_util::codec::{FramedRead, FramedWrite};

use crate::{handlers::handle_array, protocol::RespDataType};

mod codec;
mod errors;
mod handlers;
mod parser;
mod protocol;

#[tokio::main]
async fn main() -> anyhow::Result<(), RedisError> {
    // Setup the logging framework
    let env = Env::default()
        .filter_or("LOG_LEVEL", "info")
        .write_style_or("LOG_STYLE", "always");

    env_logger::init_from_env(env);

    let listener = TcpListener::bind("0.0.0.0:6379").await?;

    info!("Redis is running.");

    loop {
        // Asynchronously wait for an inbound TcpStream.
        let (stream, _address) = listener.accept().await?;

        // Spawn our handler to be run asynchronously.
        // A new task is spawned for each inbound socket.
        // The socket is moved to the new task and processed there.
        tokio::spawn(async move {
            process(stream).await.expect("Failed spawning process()");
        });
    }
}

async fn process(stream: TcpStream) -> anyhow::Result<()> {
    let (reader, writer) = stream.into_split();

    let mut reader = FramedRead::new(reader, RespCodec::new());
    let mut writer = FramedWrite::new(writer, RespCodec::new());

    // Redis generally uses RESP as a request-response protocol in the following way:
    //  Clients send commands to a Redis server as an array of bulk strings.
    //  The first (and sometimes also the second) bulk string in the array is the command's name.
    //  Subsequent elements of the array are the arguments for the command.
    // The server replies with a RESP type.
    while let Some(message) = reader.next().await {
        match message {
            Ok(RespFrame::Array(value)) => {
                if let Some(resp_frames) = value {
                    info!("Got an array: {:?}", resp_frames);
                    for message in resp_frames {
                        match message {
                            RespFrame::SimpleString(_) => todo!(),
                            RespFrame::Integer(_) => todo!(),
                            RespFrame::Error(_) => todo!(),
                            RespFrame::BulkString(ref bulk_string) => {
                                if let Some(bulk_str) = bulk_string {
                                    info!("Parsed to: {:?}", String::from_utf8_lossy(bulk_str));
                                    let reply = RespDataType::SimpleString(String::from("pong"));
                                    writer.send(reply).await?;
                                }
                            }
                            RespFrame::Array(_) => todo!(),
                        }
                    }

                    // handle_array(msg, &mut writer).await?;
                }
            }

            Ok(_) => {
                warn!("This is a valid RESP message but not handled by the server")
            }
            Err(_) => todo!(),
        }
    }

    Ok(())
}
