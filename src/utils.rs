// This code implements several utility functions.

// Key functions and their purposes:

// expire_value: Handles delayed expiration of values based on specified EX or PX options.
// It schedules a task to delete the value after the specified duration.
//
// handshake: Manages the replication handshake process between a master and slave node.
// It sends and receives necessary commands to establish the connection and synchronize replication data.
//
// generate_replication_id: Generates a random 40-character alphanumeric string to be used as a replication ID.

// Additional details:

// The code uses tokio for asynchronous operations and anyhow for error handling.

// It leverages tracing for logging and debugging.
// The code includes functions to handle different expiration options (EX, PX, EXAT, PXAT, KEEPTTL) but currently only implements the EX and PX options.
// The handshake function sends commands to establish a replication connection, including PING, REPLCONF, and PSYNC.
// The generate_replication_id function uses rand to generate a random string for the replication ID.

use crate::{
    actors::messages::HostId,
    handlers::{replication::ReplicationActorHandle, set_command::SetCommandActorHandle},
    protocol::{self, ReplicationSectionData, ServerRole, SetCommandParameter},
    resp::value::RespValue,
};
use anyhow::{Context, Result};

use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tracing::{debug, error};

// for master repl id generation
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::iter;
// ----------

pub async fn sleeping_task(wait_sleep_tx: mpsc::Sender<i16>, duration: Duration, target_offset: i16) -> JoinHandle<()> {
    let handle = tokio::spawn(async move {
        tracing::info!("Sleeping thread started.");
        sleep(duration).await;
        tracing::info!("Sleeping thread finished: {:?}.", duration);
        wait_sleep_tx
            .send(target_offset) // we are passing this around to avoid advancing the offset prematurely
            .await
            .expect("This should have succeeded.");
    });
    handle
}

pub async fn update_master_offset(
    replica_tx: broadcast::Sender<RespValue>,
    replication_actor_handle: ReplicationActorHandle,
) {
    let mut replica_rx = replica_tx.subscribe();
    // Start receiving messages from the channel by calling the recv method of the Receiver endpoint.
    // This method blocks until a message is received.
    loop {
        let msg = replica_rx.recv().await;
        match msg {
            Ok(payload) => {
                // we need to convert the command to a RESP string to count the bytes.
                let value_as_string = payload
                    .to_encoded_string()
                    .expect("Expected to easily convert RESP to string");

                // calculate how many bytes are in the value_as_string
                let value_as_string_num_bytes = value_as_string.len() as i16;

                // these should never fail, so expect is ok.
                debug!(
                    "MASTER: current offset: {} bytes",
                    replication_actor_handle
                        .get_value(HostId::Myself)
                        .await
                        .expect("Expected to get master replication info.")
                        .master_repl_offset
                        .expect("Expected to get master offset.")
                );

                // we need to update master's offset because we are sending writeable commands to replicas
                let mut updated_replication_data_master = ReplicationSectionData::new();

                // remember, this is an INCREMENT not a total new value
                updated_replication_data_master.master_repl_offset =
                    Some(value_as_string_num_bytes);

                // updating master offset as a master
                replication_actor_handle
                    .update_value(HostId::Myself, updated_replication_data_master)
                    .await;

                debug!(
                    "MASTER: updated offset: {}",
                    replication_actor_handle
                        .get_value(HostId::Myself)
                        .await
                        .expect("Expected to get master replication info.")
                        .master_repl_offset
                        .expect("Expected to get master offset.")
                );
            }
            Err(e) => {
                error!("Something horrible happened while trying to update master offset: {e}")
            }
        }
    }
}
pub async fn expire_value(
    msg: SetCommandParameter,
    set_command_actor_handle: SetCommandActorHandle,
) -> anyhow::Result<()> {
    // We may or may not need to expire a value. If not, no big deal, just wait again.
    if let Some(duration) = msg.expire {
        match duration {
            // reminder: seconds are Unix timestamps
            protocol::SetCommandExpireOption::EX(seconds) => {
                // Must clone again because we're about to move this into a dedicated sleep thread.
                let expire_command_handler_clone = set_command_actor_handle.clone();

                // NOTE: type annotations are needed here
                let _expiry_handle: tokio::task::JoinHandle<Result<()>> =
                    tokio::spawn(async move {
                        // get the current system time
                        let now = SystemTime::now();

                        // how many seconds have elapsed since beginning of time
                        let duration_since_epoch = now.duration_since(UNIX_EPOCH)?;

                        // i64 since it is possible for this to be negative, i.e. past time expiration
                        let expiry_time = seconds as i64 - duration_since_epoch.as_secs() as i64;

                        // we sleep if this is NON negative
                        if !expiry_time < 0 {
                            debug!("Sleeping for {} seconds.", expiry_time);
                            sleep(Duration::from_secs(expiry_time as u64)).await;
                        }

                        // Fire off a command to the handler to remove the value immediately.
                        expire_command_handler_clone.delete_value(&msg.key).await;

                        Ok(())
                    });
            }
            protocol::SetCommandExpireOption::PX(milliseconds) => {
                // Must clone again because we're about to move this into a dedicated sleep thread.
                let command_handler_expire_clone = set_command_actor_handle.clone();
                let _expiry_handle: tokio::task::JoinHandle<Result<()>> =
                    tokio::spawn(async move {
                        // get the current system time
                        let now = SystemTime::now();

                        // how many milliseconds have elapsed since beginning of time
                        let duration_since_epoch = now.duration_since(UNIX_EPOCH)?;

                        // i64 since it is possible for this to be negative, i.e. past time expiration
                        let expiry_time =
                            milliseconds as i64 - duration_since_epoch.as_millis() as i64;

                        // we sleep if this is NON negative
                        if !expiry_time < 0 {
                            debug!("Sleeping for {} milliseconds.", expiry_time);
                            sleep(Duration::from_millis(expiry_time as u64)).await;
                        }

                        // Fire off a command to the handler to remove the value immediately.
                        command_handler_expire_clone.delete_value(&msg.key).await;

                        Ok(())
                    });
            }
            protocol::SetCommandExpireOption::EXAT(_) => todo!(),
            protocol::SetCommandExpireOption::PXAT(_) => todo!(),
            protocol::SetCommandExpireOption::KEEPTTL => todo!(),
        }
    }

    Ok(())
}

pub async fn handshake(
    tcp_msgs_tx: async_channel::Sender<RespValue>,
    mut master_rx: mpsc::Receiver<String>,
    port: u16,
    replication_actor_handle: ReplicationActorHandle,
) -> anyhow::Result<()> {
    // begin the replication handshake
    // STEP 1: PING
    let ping = RespValue::array_from_slice(&["PING"]);

    // STEP 2: REPLCONF listening-port <PORT>
    // initialize the empty array
    let replconf_listening_port =
        RespValue::array_from_slice(&["REPLCONF", "listening-port", &port.to_string()]);

    // STEP 3: REPLCONF capa psync2
    // initialize the empty array
    let repl_conf_capa = RespValue::array_from_slice(&["REPLCONF", "capa", "psync2"]);

    // STEP 4: send the PSYNC ? -1
    let psync = RespValue::array_from_slice(&["PSYNC", "?", "-1"]);

    // // let handshake_commands = vec![repl_conf_listening_port, repl_conf_capa, psync];

    // send the ping
    tcp_msgs_tx.send(ping).await?;
    // wait for a reply from the master before proceeding
    let reply = master_rx
        .recv()
        .await
        .context("Failed to receive a reply from master after sending PING.")?;
    debug!("HANDSHAKE PING: master replied to ping {:?}", reply);

    // send the REPLCONF listening-port <PORT>
    tcp_msgs_tx.send(replconf_listening_port).await?;
    // wait for a reply from the master before proceeding
    let reply = master_rx.recv().await.context(
        "Failed to receive a reply from master after sending REPLCONF listening-port <PORT>.",
    )?;
    debug!(
        "HANDSHAKE REPLCONF listening-port <PORT>: master replied {:?}",
        reply
    );

    // send the REPLCONF capa psync2
    tcp_msgs_tx.send(repl_conf_capa).await?;
    // wait for a reply from the master before proceeding
    let reply = master_rx
        .recv()
        .await
        .context("Failed to receive a reply from master after sending REPLCONF capa psync2.")?;
    debug!("HANDSHAKE REPLCONF capa psync2: master replied {:?}", reply);

    // send the PSYNC ? -1
    /*
        When a replica connects to a master for the first time, it sends a PSYNC ? -1 command.
        This is the replica's way of telling the master that it doesn't have any data yet, and needs to be fully resynchronized.

        The master acknowledges this by sending a FULLRESYNC response to the replica.
        After sending the FULLRESYNC response, the master will then send a RDB file of its current state to the replica.
        The replica is expected to load the file into memory, replacing its current state.
    */
    tcp_msgs_tx.send(psync).await?;

    // NOTE: offset is set to None which is OK we are not going to update it.
    // Reason is, some other thread may have updated the offset already, so we need to preserve it.
    let replication_data: ReplicationSectionData = ReplicationSectionData {
        role: Some(ServerRole::Slave),
        master_replid: Some(
            master_rx
                .recv()
                .await
                .context("Failed to receive a reply from master after sending PSYNC ? -1.")?,
        ), // master will reply with its repl id
        master_repl_offset: None,
    };

    // my own replication data, i.e. slave's own replication data
    replication_actor_handle
        .update_value(HostId::Myself, replication_data)
        .await;

    // We are done with the handshake!
    debug!("Handshake completed.");

    Ok(())

    // Ok(replication_id)
}

pub fn generate_replication_id() -> String {
    // Initialize a random number generator based on the current thread.
    let mut rng = thread_rng();

    // Create a sequence of 40 random alphanumeric characters.
    let repl_id: String = iter::repeat(())
        // Map each iteration to a randomly chosen alphanumeric character.
        .map(|()| rng.sample(Alphanumeric))
        // Convert the sampled character into its char representation.
        .map(char::from)
        .take(40) // Take only the first 40 characters.
        .collect(); // Collect the characters into a String.

    repl_id
}
