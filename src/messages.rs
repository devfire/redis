use tokio::sync::oneshot;

pub enum ActorMessage {
    GetValue {
        key: String,
        respond_to: oneshot::Sender<String>,
    },
    SetValue {
        // a tuple works better than a hash since we can enforce a single pair always
        input_kv: (String, String),
    }, 
}
