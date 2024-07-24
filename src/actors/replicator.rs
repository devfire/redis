use log::info;
use tokio::{io::AsyncWriteExt, net::TcpStream, sync::mpsc};

use super::messages::ReplicationActorMessage;

pub struct ReplicatorActor {
    // The receiver for incoming messages
    receiver: mpsc::Receiver<ReplicationActorMessage>,
    // The stores the stream to the master redis instance.
    // Has to be an Option because during the initialization we don't have the TCP connection details.
    master_stream: Option<TcpStream>,
}

impl ReplicatorActor {
    // Constructor for the actor
    pub fn new(receiver: mpsc::Receiver<ReplicationActorMessage>) -> Self {
        let master_stream = None;
        // Return a new actor with the given receiver and an empty tcp stream
        Self {
            receiver,
            master_stream,
        }
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
                // Establish a TCP connection to the master with the connection_string
                let stream = TcpStream::connect(&connection_string)
                    .await
                    .expect("Failed to establish connection to master.");

                // Add the connection to the hash map
                self.master_stream = Some(stream);
                debug!("Connected to master: {}.", connection_string);
            }
            ReplicationActorMessage::SendCommand { command } => {
                // Send the command to the master
                let encoded_command = command.encode();

                if let Some(ref mut stream) = self.master_stream {
                    stream
                        .write_all(&encoded_command)
                        .await
                        .expect("Failed to write replication command");
                }
            }
        }
    }
}
