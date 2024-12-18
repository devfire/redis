use crate::{
    actors::{
        messages::{HostId, ProcessorActorMessage},
        processor::ProcessorActor,
    },
    handlers::set_command::SetCommandActorHandle,
    protocol::SetCommandParameter,
    resp::value::RespValue,
};

// use anyhow::{Context, Result, anyhow};

// use tracing::debug;
// use resp::Value;
use tokio::sync::{broadcast, mpsc, oneshot};

use super::{config_command::ConfigCommandActorHandle, replication::ReplicationActorHandle};

#[derive(Clone, Debug)]
pub struct RequestProcessorActorHandle {
    sender: mpsc::Sender<ProcessorActorMessage>,
}

// Gives you access to the underlying actor.
impl RequestProcessorActorHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = ProcessorActor::new(receiver);

        // This isn't going to fail, so we are ignoring errors here.
        let _handle = tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    /// Takes RESP frames, parses them into Redis commands and returns proper replies back to the requestor.
    /// https://redis.io/commands/
    pub async fn process_request(
        &self,
        request: RespValue,
        set_command_actor_handle: SetCommandActorHandle,
        config_command_actor_handle: ConfigCommandActorHandle,
        replication_actor_handle: ReplicationActorHandle,
        host_id: HostId,
        expire_tx: mpsc::Sender<SetCommandParameter>,
        master_tx: mpsc::Sender<String>,
        replica_tx: broadcast::Sender<RespValue>, // we get this from master handler only
        client_or_replica_tx: Option<mpsc::Sender<bool>>,
        wait_sleep_tx: Option<mpsc::Sender<i16>>,
    ) -> Option<Vec<RespValue>> {
        tracing::debug!("Processing request: {:?}", request);
        // create a multiple producer, single consumer channel
        let (send, recv) = oneshot::channel();

        let msg = ProcessorActorMessage::Process {
            request,
            set_command_actor_handle,
            config_command_actor_handle,
            replication_actor_handle,
            host_id,
            expire_tx,
            master_tx,
            replica_tx,
            client_or_replica_tx,
            respond_to: send,
            wait_sleep_tx,
        };

        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check the
        // failure twice.
        let _ = self.sender.send(msg).await;

        if let Some(value) = recv
            .await
            .expect("Request processor actor task has been killed")
        {
            tracing::info!("Processor actor returns {:?}", value);
            Some(value)
        } else {
            None
        }
    }
}
