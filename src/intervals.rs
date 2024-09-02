// Module for handling repetitive tasks, like sending REPLCONF

use tokio::time::{interval, Duration};
use tracing::info;

use crate::{
    actors::messages::HostId, handlers::replication::ReplicationActorHandle, resp::value::RespValue,
};

pub async fn send_offset_to_master(
    tcp_msgs_tx: async_channel::Sender<RespValue>,
    replication_actor_handle: ReplicationActorHandle,
    delay: u64,
) -> anyhow::Result<()> {
    let mut interval = interval(Duration::from_secs(delay));

    loop {
        interval.tick().await;
        info!("Sending REPLCONF ACK to master");
        // First, let's get our current replication data from replica's POV.
        if let Some(current_replication_data) =
            replication_actor_handle.get_value(HostId::Myself).await
        {
            // extract the current offset value.
            let current_offset = current_replication_data.master_repl_offset;

            let replconf_ack_offset =
                RespValue::array_from_slice(&["REPLCONF", "ACK", &current_offset.to_string()]);

            tcp_msgs_tx.send(replconf_ack_offset).await?;
        }
    }
}

pub async fn send_ack_to_replicas(
    tcp_msgs_tx: async_channel::Sender<RespValue>,
    replication_actor_handle: ReplicationActorHandle,
    delay: u64,
) -> anyhow::Result<()> {
    let mut interval = interval(Duration::from_secs(delay));

    loop {
        interval.tick().await;
        info!("Sending REPLCONF ACK to master");
        // First, let's get our current replication data from replica's POV.
        if let Some(current_replication_data) =
            replication_actor_handle.get_value(HostId::Myself).await
        {
            // extract the current offset value.
            let current_offset = current_replication_data.master_repl_offset;

            let replconf_ack_offset =
                RespValue::array_from_slice(&["REPLCONF", "ACK", &current_offset.to_string()]);

            tcp_msgs_tx.send(replconf_ack_offset).await?;
        }
    }
}
