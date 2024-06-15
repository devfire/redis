use crate::{
    actors::{messages::ProcessorActorMessage, processor::ProcessorActor},
    handlers::set_command::SetCommandActorHandle, protocol::SetCommandParameter,
};
use anyhow::Result;
use log::info;
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

    /// Gets sections from INFO command, taking a key as input and returning a value.
    /// https://redis.io/commands/config-get/
    pub fn process_request(
        &self,
        request: resp::Value,
        set_command_actor_handle: SetCommandActorHandle,
        config_command_actor_handle: ConfigCommandActorHandle,
        info_command_actor_handle: InfoCommandActorHandle,
        expire_tx: mpsc::Sender<SetCommandParameter>
    ) -> Result<()> {
        info!("Processing request: {:?}", request);
        // let (send, recv) = oneshot::channel();
        let msg = ProcessorActorMessage::Process {
            request,
            set_command_actor_handle,
            config_command_actor_handle,
            info_command_actor_handle,
            expire_tx
        };

        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check the
        // failure twice.
        let _ = self.sender.send(msg);

        Ok(())
    }
}
