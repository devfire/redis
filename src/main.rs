use codec::RespCodec;
use env_logger::Env;

use errors::RedisError;
use futures_util::{StreamExt};
use log::{info, warn};
// use protocol::RespFrame;

use tokio::net::{TcpListener, TcpStream};

use tokio_util::codec::{FramedRead, FramedWrite};

use crate::protocol::Command;

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
    let _writer = FramedWrite::new(writer, RespCodec::new());

    // Redis generally uses RESP as a request-response protocol in the following way:
    //  Clients send commands to a Redis server as an array of bulk strings.
    //  The first (and sometimes also the second) bulk string in the array is the command's name.
    //  Subsequent elements of the array are the arguments for the command.
    // The server replies with a RESP type.
    while let Some(command) = reader.next().await {
        info!("Received command: {:?}", command);
        match command {
            Ok(Command::Ping) => {
                
            },
            Ok(_) => {
                warn!("Unknown command {:?}", command);
            }
            Err(_) => todo!(),
        }
    }

    Ok(())
}
