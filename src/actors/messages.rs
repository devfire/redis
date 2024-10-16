use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

// use crate::protocol::WaitCommandParameter;
use crate::resp::value::RespValue;
use crate::{
    handlers::{
        config_command::ConfigCommandActorHandle, replication::ReplicationActorHandle,
        set_command::SetCommandActorHandle,
    },
    protocol::{ConfigCommandParameter, ReplicationSectionData, SetCommandParameter},
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
    ImportRdb {
        set_command_actor_handle: crate::handlers::set_command::SetCommandActorHandle,
        import_from_memory: Option<Vec<u8>>,
        expire_tx: mpsc::Sender<SetCommandParameter>,
    },
    GetRdb {
        respond_to: oneshot::Sender<Option<Vec<u8>>>,
    },
}

#[derive(Debug)]
pub enum ReplicatorActorMessage {
    // the idea here is that values are stored in a HashMap.
    // So, to get a INFO value back the client must supply a String key.
    // NOTE: https://redis.io/docs/latest/commands/info/ has a ton of parameters,
    // only some are currently supported.
    //
    // Info values are 2 dimensional:
    // Example: Replication -> role -> master.
    GetReplicationValue {
        // info_key: InfoCommandParameter, // defined in protocol.rs
        host_id: HostId, // this is HOSTIP:PORT format
        respond_to: oneshot::Sender<Option<ReplicationSectionData>>,
    },

    UpdateReplicationValue {
        // info_key: InfoCommandParameter, // defined in protocol.rs
        host_id: HostId,
        replication_value: ReplicationSectionData,
    },
    GetReplicaCount {
        respond_to: oneshot::Sender<usize>, // total number of connected, synced up replicas
    },

    ResetReplicaOffset {
        host_id: HostId,
    },

}

#[derive(Clone, Hash, Eq, PartialEq)]
pub enum HostId {
    Host { ip: String, port: u16 },
    Myself, // this is used to store this redis' instance own metadata, like its offset, etc.
}

impl std::fmt::Debug for HostId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HostId::Host { ip, port } => {
                write!(f, "{}:{}", ip, port)
            }
            HostId::Myself => write!(f, "self"),
        }
    }
}
impl std::fmt::Display for HostId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HostId::Host { ip, port } => {
                write!(f, "{}:{}", ip, port)
            }
            HostId::Myself => write!(f, "HostId::Myself"),
        }
    }
}

pub enum ProcessorActorMessage {
    // connection string to connect to master
    Process {
        request: RespValue,
        set_command_actor_handle: SetCommandActorHandle,
        config_command_actor_handle: ConfigCommandActorHandle,
        replication_actor_handle: ReplicationActorHandle,
        host_id: HostId,
        expire_tx: mpsc::Sender<SetCommandParameter>,
        master_tx: mpsc::Sender<String>,
        replica_tx: broadcast::Sender<RespValue>, // typically this is either +OK or offset
        client_or_replica_tx: Option<mpsc::Sender<bool>>,
        // NOTE: a single request like PSYNC can return multiple responses.
        // So, where a Vec<u8> is a single reponse, a Vec<Vec<u8>> is multiple responses.
        respond_to: oneshot::Sender<Option<Vec<RespValue>>>,
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
                replication_actor_handle: _,
                host_id: _,
                expire_tx: _,
                master_tx: _,
                replica_tx,
                client_or_replica_tx: _,
                respond_to: _,
            } => {
                write!(
                    f,
                    "ProcessorActorMessage::Process request: {:?}, replica sender: {:?}",
                    request, replica_tx
                )
            }
        }
    }
}
