use crate::actors::{messages::ProcessorActorMessage, processor::ProcessorActor};
use anyhow::Result;
use log::info;
use tokio::sync::{mpsc, oneshot};

#[derive(Clone)]
pub struct ProcessorActorHandle {
    sender: mpsc::Sender<ProcessorActorMessage>,
}

// Gives you access to the underlying actor.
impl ProcessorActorHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = ProcessorActor::new(receiver);

        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    /// Gets sections from INFO command, taking a key as input and returning a value.
    /// https://redis.io/commands/config-get/
    pub fn process_request(&self, request: resp::Value) -> Result<()> {
        info!("Processing request: {:?}", request);
        // let (send, recv) = oneshot::channel();
        let msg = ProcessorActorMessage::Process { request };

        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check the
        // failure twice.
        let _ = self.sender.send(msg);

        Ok(())
    }
}
