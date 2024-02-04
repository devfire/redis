use tokio::sync::oneshot;

use crate::protocol::{ConfigCommandParameters, SetCommandParameters};

/// The ActorMessage enum defines the kind of messages we can send to the actor.
/// By using an enum, we can have many different message types, and each message type can have its own set of arguments.
/// We return a value to the sender by using an oneshot channel, which is a message passing channel that allows
/// sending exactly one message.
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
        input: SetCommandParameters,
    },
    ExpireValue {
        // Expires the value at a given interval
        expiry: String,
    },
}

#[derive(Debug)]
pub enum ConfigActorMessage {
    // the idea here is that values are stored in a HashMap.
    // So, to get a CONFIG Value back the client must supply a String key.
    // NOTE: Only dir and dbfilename keys are supported.
    GetValue {
        config_key: ConfigCommandParameters,
        respond_to: oneshot::Sender<Option<String>>,
    },
    SetValue {
        // should be either dir or dbfilename
        config_key: ConfigCommandParameters,
        config_value: String,
    },
}

#[derive(Debug)]
pub enum ProcessActorMessage {
    // the idea here is that values are stored in a HashMap.
    // So, to get a CONFIG Value back the client must supply a String key.
    // NOTE: Only dir and dbfilename keys are supported.
    LoadConfig {
        config_key: String,
        respond_to: oneshot::Sender<Option<String>>,
    },
    SetValue {
        // should be either dir or dbfilename
        config_key: String,
        config_value: String,
    },
}

