use tokio::sync::oneshot;

use crate::protocol::SetCommandParameters;

pub enum SetActorMessage {
    // the idea here is that values are stored in a String->Value HashMap.
    // So, to get a Value back the client must supply a String key.
    GetValue {
        key: String,
        respond_to: oneshot::Sender<String>,
    },
    SetValue {
        // SetCommandParameters is defined in protocol.rs
        input: SetCommandParameters,
    },
}
