use log::{error, info};
use resp::{encode_slice, Decoder};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
};

use crate::errors::RedisError;

use super::messages::ReplicationActorMessage;

pub struct ReplicatorActor {
    // The receiver for incoming messages
    receiver: mpsc::Receiver<ReplicationActorMessage>,
    // The section-key-value hash map for storing data.
    // There are multiple sections, each has multiple keys, each key with one value.
    // kv_hash: HashMap<InfoCommandParameter, InfoSectionData>,
}

impl ReplicatorActor {
    // Constructor for the actor
    pub fn new(receiver: mpsc::Receiver<ReplicationActorMessage>) -> Self {
        // Initialize the key-value hash map. The key is an enum of two types, dir and dbfilename.
        // let kv_hash = HashMap::new();

        // Return a new actor with the given receiver and an empty key-value hash map
        Self { receiver }
    }

    // Run the actor
    pub async fn run(&mut self) {
        // Continuously receive messages and handle them
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }

    // Handle a message.
    pub async fn handle_message(&mut self, msg: ReplicationActorMessage) {
        // Match on the type of the message

        match msg {
            ReplicationActorMessage::ConnectToMaster { connection_string } => {
                info!("Connecting to master: {}", connection_string);

                // Establish a TCP connection to the master with the connection_string
                let stream = TcpStream::connect(&connection_string)
                    .await
                    .expect("Failed to establish connection to master.");

                // Split the TCP stream into a reader and writer.
                let (mut reader, mut writer) = stream.into_split();

                // Send a PING to the master
                let ping = encode_slice(&["PING"]);

                writer
                    .write_all(&ping)
                    .await
                    .expect("Failed to write to stream");

                // split the string by : and take the second half, the port,
                // then parse it to a u16
                let connection_port = connection_string.split(':').collect::<Vec<&str>>()[1];

                // Now let's create the replconf REPLCONF listening-port <PORT>
                let replconf = encode_slice(&["REPLCONF", "listening-port", &connection_port]);

                // send to the master
                writer
                    .write_all(&replconf)
                    .await
                    .expect("Failed to write replconf to stream");

                // Read the response from the master
                // Buffer to store the data
                let mut buf = vec![0; 1024];

                // Read data from the stream, n is the number of bytes read
                let n = reader
                    .read(&mut buf)
                    .await
                    .expect("Unable to read from buffer");

                if n == 0 {
                    error!("Empty buffer.");
                    // return Ok(()); // we don't want to return an error since an empty buffer is not a problem.
                    // return Err(RedisError::ParseFailure.into());
                }

                // info!("Read {} bytes", n);

                // https://docs.rs/resp/latest/resp/struct.Decoder.html
                let mut decoder = Decoder::new(std::io::BufReader::new(buf.as_slice()));

                let request: resp::Value = decoder.decode().expect("Unable to decode request");

                match request {
                    resp::Value::Null => todo!(),
                    resp::Value::NullArray => todo!(),
                    resp::Value::String(s) => {
                        info!("Received {}", s);
                    }
                    resp::Value::Error(_) => todo!(),
                    resp::Value::Integer(_) => todo!(),
                    resp::Value::Bulk(_) => todo!(),
                    resp::Value::BufBulk(_) => todo!(),
                    resp::Value::Array(_) => todo!(),
                }

                // Now let's create the replconf REPLCONF capa psync2
                let replconf2 = encode_slice(&["REPLCONF", "capa", "psync2"]);

                // send to the master
                writer
                    .write_all(&replconf2)
                    .await
                    .expect("Failed to write replconf to stream");

            }
        }
    }
}
