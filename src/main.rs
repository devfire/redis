use anyhow;

use env_logger::Env;
use log::{info, warn};

use crate::codec::RespCodec;
use futures::sink::SinkExt;
use futures::StreamExt;
use protocol::RespFrame;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{FramedRead, FramedWrite};

mod codec;
mod errors;
mod parser;
mod protocol;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
            process(stream).await;
        });
    }
}

async fn process(stream: TcpStream) {
    let (mut reader, mut writer) = stream.into_split();

    let mut reader = FramedRead::new(reader, RespCodec::new());
    let mut writer = FramedWrite::new(writer, RespCodec::new());

    while let Some(message) = reader.next().await {
        match message {
            Ok(RespFrame::Integer(value)) => info!("Got an integer {}", value),
            Ok(RespFrame::SimpleString(value)) => info!("Got a simple string {:?}", value),
            Ok(RespFrame::Array(value)) => {
                if let Some(msg) = value {
                    info!("Got an array: {:?}", msg)
                }
            }

            Ok(_) => {
                warn!("This is a valid RESP message but not handled by the server")
            }
            Err(_) => todo!(),
        }
    }
}
