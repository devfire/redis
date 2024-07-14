use crate::{
    actors::{messages::ProcessorActorMessage, processor::ProcessorActor},
    handlers::set_command::SetCommandActorHandle,
    protocol::SetCommandParameter,
};

use log::info;
// use resp::Value;
use tokio::sync::mpsc;

use super::{config_command::ConfigCommandActorHandle, info_command::InfoCommandActorHandle};

#[derive(Clone)]
pub struct RequestProcessorActorHandle {
    sender: mpsc::Sender<ProcessorActorMessage>,
}

// Gives you access to the underlying actor.
impl RequestProcessorActorHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = ProcessorActor::new(receiver);

        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    /// Processes RESP commands.
    /// https://redis.io/commands/
    pub async fn process_request(
        &self,
        request: resp::Value,
        set_command_actor_handle: SetCommandActorHandle,
        config_command_actor_handle: ConfigCommandActorHandle,
        info_command_actor_handle: InfoCommandActorHandle,
        expire_tx: mpsc::Sender<SetCommandParameter>,
        master_tx: mpsc::Sender<String>,
    ) -> Option<Vec<u8>> {
        info!("Processing request: {:?}", request);
        // create a multiple producer, single consumer channel
        let (send, mut recv) = mpsc::channel(10);
        let msg = ProcessorActorMessage::Process {
            request,
            set_command_actor_handle,
            config_command_actor_handle,
            info_command_actor_handle,
            expire_tx,
            master_tx,
            respond_to: send,
        };

        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check the
        // failure twice.
        let _ = self
            .sender
            .send(msg)
            .await
            .expect("Failed to send value for processing.");

        // Loop until the channel is closed.
        while let Some(value) = recv.recv().await {
            info!("Received value: {:?}", value);
            if let Some(value) = value {
                return Some(value);
            } else {
                return None;
            }
        }
        None

        // while let message = recv.recv().await.expect("msg") {
        //     if let Some(value) = message {
        //         Some(value)
        //     } else {
        //         None
        //     }
        // }
    }
}
