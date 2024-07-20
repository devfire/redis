use tracing::{debug, error, info};
use tokio::sync::{mpsc, oneshot};

use crate::{
    actors::{config::ConfigCommandActor, messages::ConfigActorMessage},
    errors::RedisError,
    protocol::ConfigCommandParameter,
};

#[derive(Clone, Debug)]
pub struct ConfigCommandActorHandle {
    sender: mpsc::Sender<ConfigActorMessage>,
}

// Gives you access to the underlying actor.
impl ConfigCommandActorHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = ConfigCommandActor::new(receiver);

        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    /// implements the redis CONFIG GET command, taking a key as input and returning a value.
    /// https://redis.io/commands/config-get/
    pub async fn get_value(&self, config_key: ConfigCommandParameter) -> Option<String> {
        debug!("Getting value for key: {:?}", config_key);
        let (send, recv) = oneshot::channel();
        let msg = ConfigActorMessage::GetConfigValue {
            config_key,
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

    /// implements the redis CONFIG SET command, taking a key, value pair as input. Returns nothing.
    /// https://redis.io/commands/config-set/
    pub async fn set_value(&self, config_key: ConfigCommandParameter, config_value: &str) {
        let msg = ConfigActorMessage::SetConfigValue {
            config_key,
            config_value: config_value.to_string(),
        };

        info!(
            "Setting value for key: {:?}, value: {}",
            config_key, config_value
        );
        // Ignore send errors.
        let _ = self.sender.send(msg).await.expect("Failed to set value.");
    }

    /// Loads the config file on startup
    pub async fn load_config(
        &self,
        dir: &str,
        dbfilename: &str,
        set_command_actor_handle: super::set_command::SetCommandActorHandle,
        expire_tx: mpsc::Sender<crate::protocol::SetCommandParameter>,
    ) {
        let msg = ConfigActorMessage::LoadConfig {
            dir: dir.to_string(),
            dbfilename: dbfilename.to_string(),
            set_command_actor_handle,
            expire_tx, // this is a channel back to main.rs expiry loop
        };

        // Ignore send errors.
        let _ = self.sender.send(msg).await.expect("Failed to set value.");
    }

    /// Loads the config file into memory, returning it as a Vec<u8>
    pub async fn get_config(
        &self,
        // dir: &str,
        // dbfilename: &str,
    ) -> anyhow::Result<Vec<u8>, RedisError> {
        // no need to pass these two, we should already know the values
        let dir;
        let dbfilename;

        // Checks if the specified config file exists. If it does, attempts to load its contents into memory.
        //
        // Upon successful reading, the file's contents are sent through the `respond_to` channel.
        // If the file does not exist, logs an error message and sends a `None` value through the `respond_to` channel.
        //
        // This process involves opening the file asynchronously, reading its entire content into a byte vector (`buffer`),
        // and then sending this buffer through a communication channel designed for responding to configuration requests.
        // Error handling is performed at each step to ensure robustness against file access issues.
        if let Some(config_directory) = self.get_value(ConfigCommandParameter::Dir).await {
            info!("Found the dir setting: {}", config_directory);
            dir = config_directory;
        } else {
            error!("Failed to get the config file dir!");
            return Err(RedisError::IOError(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failure trying to load config into memory.",
            )));
        };

        if let Some(config_filename) = self.get_value(ConfigCommandParameter::DbFilename).await {
            info!("Found the dbfilename setting: {}", config_filename);
            dbfilename = config_filename;
        } else {
            error!("Failed to get the config filename!");
            return Err(RedisError::IOError(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failure trying to load config into memory.",
            )));
        }

        let (send, recv) = oneshot::channel();

        let msg = ConfigActorMessage::GetConfig {
            dir,
            dbfilename,
            respond_to: send,
        };

        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check the
        // failure twice.
        let _ = self.sender.send(msg).await;

        // this is going back once the msg comes back from the actor.
        // NOTE: we might get None back, i.e. something bad happened trying to load config into memory.
        if let Some(config_file) = recv.await.expect("Actor task has been killed") {
            Ok(config_file)
        } else {
            Err(RedisError::IOError(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failure trying to load config into memory.",
            )))
        }
    }
}
