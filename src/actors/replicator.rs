use log::info;
use resp::encode_slice;
use tokio::{io::AsyncWriteExt, net::TcpStream, sync::mpsc};

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
                let mut stream = TcpStream::connect(connection_string)
                    .await
                    .expect("Failed to establish connection to matser.");

                // Send a PING to the master
                let ping = encode_slice(&["PING"]);

                stream
                    .write_all(&ping)
                    .await
                    .expect("Failed to write to stream");
            }
        }
    }
}
