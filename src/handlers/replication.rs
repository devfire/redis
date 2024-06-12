use log::info;
use tokio::{
    net::unix::SocketAddr,
    sync::{mpsc, oneshot},
};

use crate::actors::{messages::ReplicationActorMessage, replicator::ReplicatorActor};

#[derive(Clone)]
pub struct ReplicationActorHandle {
    sender: mpsc::Sender<ReplicationActorMessage>,
}

// Gives you access to the underlying actor.
impl ReplicationActorHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = ReplicatorActor::new(receiver);

        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    /// Stores sections for redis INFO command, taking a key, value pair as input. Returns nothing.
    /// https://redis.io/commands/info/
    pub async fn connect_master(&self, connection_string: SocketAddr) {
        let msg = ReplicationActorMessage::ConnectToMaster { connection_string };

        // Ignore send errors.
        let _ = self.sender.send(msg).await.expect("Failed to set value.");
    }
}
