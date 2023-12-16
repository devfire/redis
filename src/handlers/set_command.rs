use log::info;
use tokio::sync::{mpsc, oneshot};

use crate::{
    actors::SetCommandActor,
    messages::SetActorMessage,
    protocol::{SetCommandExpireOption, SetCommandParameters},
};

#[derive(Clone)]
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

    /// implements the redis SET command, taking a key, value pair as input. Returns nothing.
    pub async fn set_value(&self, parameters: SetCommandParameters) {
        let msg = SetActorMessage::SetValue {
            input: parameters.clone(),
        };

        if let Some(expiration_parameters) = parameters.expire {
            match expiration_parameters {
                SetCommandExpireOption::EX(_) => todo!(),
                SetCommandExpireOption::PX(_milliseconds) => {
                    let msg = SetActorMessage::ExpireValue {
                        expiry: parameters,
                    };

                    let _ = self.sender.send(msg).await; 
                }
                SetCommandExpireOption::EXAT(_) => todo!(),
                SetCommandExpireOption::PXAT(_) => todo!(),
                SetCommandExpireOption::KEEPTTL => todo!(),
            }
        }
        // match parameters.expire {
        //     Some(SetCommandExpireOption::EX(seconds)) => info!("Expire in {} seconds", seconds),
        //     Some(SetCommandExpireOption::PX(milliseconds)) => {
        //         info!("Expire in {} milliseconds", milliseconds);
        //         let msg = SetActorMessage::ExpireValue {
        //             expiry: parameters.expire.map(SetCommandExpireOption::PX),
        //         };
        //         let _ = self.sender.send(msg).await;
        //     }
        //     Some(SetCommandExpireOption::EXAT(timestamp)) => info!("Expire at {}", timestamp),
        //     Some(SetCommandExpireOption::PXAT(timestamp)) => info!("Expire at {}", timestamp),
        //     Some(SetCommandExpireOption::KEEPTTL) => info!("Keep TTL"),
        //     None => info!("No expire option"),
        // }

        // Ignore send errors.
        let _ = self.sender.send(msg).await.expect("Failed to set value.");
    }
}
