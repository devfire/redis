use tokio::sync::{mpsc, oneshot};

use crate::{actors::SetCommandActor, messages::ActorMessage};

#[derive(Clone)]
pub struct SetCommandActorHandle {
    sender: mpsc::Sender<ActorMessage>,
}

impl SetCommandActorHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = SetCommandActor::new(receiver);
        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    pub async fn get_value(&self, key: String) -> String {
        let (send, recv) = oneshot::channel();
        let msg = ActorMessage::GetValue {
            key: key,
            respond_to: send,
        };

        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check the
        // failure twice.
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}
