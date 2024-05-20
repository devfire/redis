use tokio::sync::{mpsc, oneshot};
// pub mod actors;

use crate::{
    actors::set::SetCommandActor, messages::SetActorMessage, protocol::SetCommandParameter,
};

#[derive(Clone, Debug)]
pub struct SetCommandActorHandle {
    sender: mpsc::Sender<SetActorMessage>,
}

// Gives you access to the underlying actor.
impl SetCommandActorHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = SetCommandActor::new(receiver);
        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    /// implements the redis GET command, taking a key as input and returning a value.
    /// https://redis.io/commands/get/
    pub async fn get_value(&self, key: &str) -> Option<String> {
        let (send, recv) = oneshot::channel();
        let msg = SetActorMessage::GetValue {
            key: key.to_string(),
            respond_to: send,
        };

        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check the
        // failure twice.
        let _ = self.sender.send(msg).await;

        // this is going back once the msg comes back from the actor.
        // NOTE: we might get None back, i.e. no value for the given key.
        if let Some(value) = recv.await.expect("Actor task has been killed") {
            Some(value)
        } else {
            None
        }
    }

    /// implements the redis KEYS command, taking a pattern as input and returning a list of keys.
    /// https://redis.io/commands/keys/
    pub async fn get_keys(&self, pattern: &str) -> Option<Vec<String>> {
        let (send, recv) = oneshot::channel();
        let msg = SetActorMessage::GetKeys {
            pattern: pattern.to_string(),
            respond_to: send,
        };

        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check the
        // failure twice.
        let _ = self.sender.send(msg).await;

        if let Some(keys) = recv.await.expect("Actor task has been killed") {
            Some(keys)
        } else {
            None
        }
    }
    /// implements the redis SET command, taking a key, value pair as input. Returns nothing.
    pub async fn set_value(
        &self,
        expire_tx: mpsc::Sender<SetCommandParameter>,
        set_parameters: SetCommandParameter,
    ) {
        let msg = SetActorMessage::SetValue {
            input: set_parameters.clone(),
        };

        // Ignore send errors.
        let _ = self.sender.send(msg).await.expect("Failed to set value.");

        // let parameters = set_parameters.clone();

        expire_tx
            .send(set_parameters)
            .await
            .expect("Unable to start the expiry thread.");

        // let parameters_clone = parameters.clone();
        // let _expiry_handle = tokio::spawn(async move {
        //     tokio::time::sleep(std::time::Duration::from_secs(2 as u64)).await;
        //     // log::info!("Expiring {:?}", msg);

        //     // Fire off a command to the handler to remove the value immediately.
        //     let msg = SetActorMessage::DeleteValue {
        //         value: parameters_clone.key.to_string(),
        //     };

        //     // Ignore send errors.
        //     let _ = self
        //         .sender
        //         .send(msg)
        //         .await
        //         .expect("Failed to expire value.");
        // });
    }

    /// implements immediate removal of keys. This is triggered by a tokio::spawn sleep thread in main.rs
    pub async fn delete_value(&self, key: &String) {
        let msg = SetActorMessage::DeleteValue {
            value: key.to_string(),
        };

        // Ignore send errors.
        let _ = self
            .sender
            .send(msg)
            .await
            .expect("Failed to expire value.");
    }
}
