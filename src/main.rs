use anyhow;

use env_logger::Env;
use log::{info, warn};

use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{FramedRead, FramedWrite};


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

    // let mut client_reader = FramedRead::new(reader, RespCodec::new());
    // let mut client_writer = FramedWrite::new(writer, RespCodec::new());


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

    }
}
