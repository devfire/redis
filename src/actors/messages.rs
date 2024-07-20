use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::{
    handlers::{
        config_command::ConfigCommandActorHandle, info_command::InfoCommandActorHandle,
        set_command::SetCommandActorHandle,
    },
    protocol::{
        ConfigCommandParameter, InfoCommandParameter, InfoSectionData, SetCommandParameter,
    },
};

/// The ActorMessage enum defines the kind of messages we can send to the actor.
/// By using an enum, we can have many different message types,
/// and each message type can have its own set of arguments.
/// We return a value to the sender by using an oneshot channel,
/// which is a message passing channel that allows sending exactly one message.
#[derive(Debug)]
pub enum SetActorMessage {
    // the idea here is that values are stored in a String->Value HashMap.
    // So, to get a Value back the client must supply a String key.
    GetValue {
        key: String,
        respond_to: oneshot::Sender<Option<String>>,
    },
    SetValue {
        // SetCommandParameters is defined in protocol.rs
        input: SetCommandParameter,
    },
    DeleteValue {
        // Deletes the value at a given interval
        value: String,
    },
    // returns a vector of all the keys in the HashMap
    GetKeys {
        pattern: String,
        respond_to: oneshot::Sender<Option<Vec<String>>>,
    },
}

#[derive(Debug)]
pub enum ConfigActorMessage {
    // the idea here is that values are stored in a HashMap.
    // So, to get a CONFIG Value back the client must supply a String key.
    // NOTE: Only dir and dbfilename keys are supported.
    GetConfigValue {
        config_key: ConfigCommandParameter,
        respond_to: oneshot::Sender<Option<String>>,
    },
    SetConfigValue {
        // should be either dir or dbfilename
        config_key: ConfigCommandParameter,
        config_value: String,
    },
    LoadConfig {
        // should be either dir or dbfilename
        dir: String,
        dbfilename: String,
        set_command_actor_handle: crate::handlers::set_command::SetCommandActorHandle,
        expire_tx: mpsc::Sender<SetCommandParameter>,
    },
    GetConfig {
        // should be either dir or dbfilename
        dir: String,
        dbfilename: String,
        respond_to: oneshot::Sender<Option<Vec<u8>>>,
    },
}

#[derive(Debug)]
pub enum InfoActorMessage {
    // the idea here is that values are stored in a HashMap.
    // So, to get a INFO value back the client must supply a String key.
    // NOTE: https://redis.io/docs/latest/commands/info/ has a ton of parameters,
    // only some are currently supported.
    //
    // Info values are 2 dimensional:
    // Example: Replication -> role -> master.
    GetInfoValue {
        info_key: InfoCommandParameter, // defined in protocol.rs
        respond_to: oneshot::Sender<Option<InfoSectionData>>,
    },

    SetInfoValue {
        info_key: InfoCommandParameter, // defined in protocol.rs
        info_value: InfoSectionData,
    },
}

// #[derive(Debug)]
// pub enum ReplicationActorMessage {
//     // connection string to connect to master
//     ConnectToMaster { connection_string: String },
//     SendCommand { command: resp::Value },
// }

// #[derive(Debug)]
pub enum ProcessorActorMessage {
    // connection string to connect to master
    Process {
        request: resp::Value,
        set_command_actor_handle: SetCommandActorHandle,
        config_command_actor_handle: ConfigCommandActorHandle,
        info_command_actor_handle: InfoCommandActorHandle,
        expire_tx: mpsc::Sender<SetCommandParameter>,
        master_tx: mpsc::Sender<String>,
        replica_tx: Option<broadcast::Sender<Vec<u8>>>,
        client_or_replica_tx: Option<mpsc::Sender<bool>>,
        // NOTE: a single request like PSYNC can return multiple responses.
        // So, where a Vec<u8> is a single reponse, a Vec<Vec<u8>> is multiple responses.
        respond_to: oneshot::Sender<Option<Vec<Vec<u8>>>>,
    },
}

// implement the debug trait for the ProcessorActorMessage enum
impl std::fmt::Debug for ProcessorActorMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcessorActorMessage::Process {
                request,
                set_command_actor_handle: _,
                config_command_actor_handle: _,
                info_command_actor_handle: _,
                expire_tx: _,
                master_tx: _,
                replica_tx,
                client_or_replica_tx: _,
                respond_to: _,
            } => {
                write!(f, "ProcessorActorMessage::Process request: {:?}, replica sender: {:?}", request, replica_tx)
            }
        }
    }
}
