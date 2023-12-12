use resp::Value;
use tokio::sync::oneshot;

use crate::protocol::SetCommandParameters;

pub enum SetActorMessage {
    // the idea here is that values are stored in a String->Value HashMap.
    // so, to get a Value back the client must supply a String key.
    GetValue {
        key: String,
        respond_to: oneshot::Sender<Value>,
    },
    SetValue {
        // SetCommandType is defined in protocol.rs
        input: SetCommandParameters,
    },
}
