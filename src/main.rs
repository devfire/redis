// use anyhow;

pub mod protocol;

use std::str::FromStr;

use crate::protocol::Command;

use env_logger::Env;
use log::{info, warn};
use resp::{Decoder, Value};
use tokio::io::AsyncReadExt;
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

fn handler(value: Value) -> Option<Command> {
    match value {
        Value::Bulk(raw_string) => {
            // https://docs.rs/strum_macros/0.25.3/strum_macros/derive.EnumString.html
            let input_variant = Command::from_str(&raw_string).expect("Command::from_str failed");
            match input_variant {
                Command::Ping => Some(Command::Ping),
                Command::Command => Some(Command::Command),
                Command::Echo(_) => Some(Command::Echo(None)),
                _ => None,
            }
        }
        Value::BufBulk(_) => todo!(),
        // Value::Array(array) => {
        //     // if let Some(element) = array.remove(0) {
        //     //     handler(element)
        //     // }
        // },
        _ => return None,
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
                info!("Array received {:?}", array);
                while !array.is_empty() {
                    if let Some(parsed_command) = handler(array.remove(0)) {
                        info!("Detected command {}", parsed_command);
                    }
                }
                // for command in array {
                //     if let Some(parsed_command) = handler(command) {
                //         info!("Detected command {}", parsed_command);
                //     }
                // }
            }
        }
    }
}
