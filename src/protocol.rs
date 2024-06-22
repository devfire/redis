// This file stores the various commands and their options currently supported.
use core::fmt;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::iter;
// use tokio_util::codec::{Decoder, Encoder};

// use crate::errors::RedisError;

#[derive(Debug)]
pub enum RedisCommand {
    Ping,
    Echo(String),
    Command,
    Set(SetCommandParameter),
    Get(String),
    Del(Vec<String>),
    Strlen(String),                 // https://redis.io/commands/strlen
    Mget(Vec<String>),              // https://redis.io/commands/mget
    Append(String, String),         // https://redis.io/commands/append/
    Config(ConfigCommandParameter), // CONFIG GET
    Keys(String),
    Info(Option<InfoCommandParameter>),
}

// implement Encoder for RedisCommand
// impl Encoder<RedisCommand> for RedisCommand {
//     type Error = RedisError;

//     fn encode(&mut self, item: RedisCommand, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
//         todo!()
//     }
// }

// INFO [section [section ...]]
// The optional parameter can be used to select a specific section of information:
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum InfoCommandParameter {
    All,
    Default,
    Replication,
}

/// Replication section https://redis.io/docs/latest/commands/info/
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct InfoSectionData {
    // role: Value is "master" if the instance is replica of no one,
    // or "slave" if the instance is a replica of some master instance.
    // Note that a replica can be master of another replica (chained replication).
    pub role: ServerRole,
    pub master_replid: String,
    pub master_repl_offset: u16,
}

// return InfoSectionData as a string
impl fmt::Display for InfoSectionData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "role:{}:", &self.role)?;
        write!(f, "master_replid:{}:", &self.master_replid)?;
        write!(f, "master_repl_offset:{}:", &self.master_repl_offset)
    }
}

// implement new for InfoSectionData
impl InfoSectionData {
    // Generates a random alphanumeric string of 40 characters to serve as a replication ID.
    // This method is useful for creating unique identifiers for replication purposes in Redis setups.
    pub fn generate_replication_id() -> String {
        // Initialize a random number generator based on the current thread.
        let mut rng = thread_rng();

        // Create a sequence of 40 random alphanumeric characters.
        let repl_id: String = iter::repeat(())
            // Map each iteration to a randomly chosen alphanumeric character.
            .map(|()| rng.sample(Alphanumeric))
            // Convert the sampled character into its char representation.
            .map(char::from)
            .take(40) // Take only the first 40 characters.
            .collect(); // Collect the characters into a String.

        repl_id
    }

    pub fn new(role: ServerRole) -> Self {
        Self {
            // role: Value is "master" if the instance is replica of no one
            role,
            // master_replid: The ID of the master instance
            master_replid: Self::generate_replication_id(),
            // Each master also maintains a "replication offset" corresponding to how many bytes of commands
            // have been added to the replication stream
            master_repl_offset: 0,
        }
    }
}

/// Master or slave.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum ServerRole {
    // role: Value is "master" if the instance is replica of no one,
    // or "slave" if the instance is a replica of some master instance.
    // Note that a replica can be master of another replica (chained replication).
    Master,
    Slave,
}

// implement display for ServerRole enum
impl fmt::Display for ServerRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServerRole::Master => write!(f, "master"),
            ServerRole::Slave => write!(f, "slave"),
        }
    }
}

// SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]
#[derive(Clone, Debug)]
pub struct SetCommandParameter {
    pub key: String,
    pub value: String,
    pub option: Option<SetCommandSetOption>,
    // GET: Return the old string stored at key, or nil if key did not exist.
    // An error is returned and SET aborted if the value stored at key is not a string.
    pub get: Option<bool>,
    pub expire: Option<SetCommandExpireOption>,
}

#[derive(Debug, Clone, Copy)]
pub enum SetCommandSetOption {
    NX,
    XX,
}

#[derive(Debug, Clone, Copy)]
pub enum ExpiryOption {
    Seconds(u32),
    Milliseconds(u64),
}

#[derive(Debug, Clone, Copy)]
pub enum SetCommandExpireOption {
    EX(u32), // unix timestamp seconds
    PX(u64), // unix timestamp milliseconds
    EXAT(usize),
    PXAT(usize),
    KEEPTTL,
}

// these are passed from the command line
#[derive(Debug, Clone, PartialEq, Copy, Eq, Hash)]
pub enum ConfigCommandParameter {
    Dir,
    DbFilename,
}

// this is needed to convert the enum variants to strings
impl fmt::Display for ConfigCommandParameter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigCommandParameter::Dir => write!(f, "dir"),
            ConfigCommandParameter::DbFilename => write!(f, "dbfilename"),
        }
    }
}
