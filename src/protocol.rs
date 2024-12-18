// This file stores the various commands and their options currently supported.
use core::fmt;

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
    ReplConf(ReplConfCommandParameter),
    Psync(String, i16),      // client (master_replid, master_repl_offset)
    Fullresync(String, i16), // master's (master_replid, master_repl_offset)
    Rdb(Vec<u8>),            // RDB file in memory representation
    Wait(usize, usize),
}

// REPLCONF parameters
// https://redis.io/commands/replconf
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum ReplConfCommandParameter {
    Getack(String),
    Ack(usize),
    Capa,
    ListeningPort(u16),
}

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
pub struct ReplicationSectionData {
    // role: Value is "master" if the instance is replica of no one,
    // or "slave" if the instance is a replica of some master instance.
    // Note that a replica can be master of another replica (chained replication).
    // NOTE: since Redis 4.0 replica writes are only local,
    // and are not propagated to sub-replicas attached to the instance.
    // Sub-replicas instead will always receive the replication stream identical
    // to the one sent by the top-level master to the intermediate replicas.
    //
    // NOTE: these are all Option to enable updates to individual fields. Because actors serialize updates from multiple
    // simultaenous threads, we need a way to enable updates to individual struct members without get-update-push cycle,
    // which risks race conditions in cases of multiple threads trying to update the same value at the same time.
    pub role: Option<ServerRole>,
    pub master_replid: Option<String>,
    pub master_repl_offset: Option<i16>, // cannot be u16 because initial offset is -1
}

impl fmt::Display for ReplicationSectionData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(my_role) = &self.role {
            write!(f, "role:{}:", *my_role)?;
        } else {
            write!(f, "Role: Not set")?;
        }

        if let Some(replid) = &self.master_replid {
            write!(f, "master_replid:{}:", *replid)?;
        } else {
            write!(f, "Master Replication ID: Not set")?;
        }

        if let Some(my_offset) = &self.master_repl_offset {
            write!(f, "master_repl_offset:{}:", *my_offset)?;
        } else {
            write!(f, "Master Replication Offset: Not set")?;
        }

        Ok(())
    }
}

impl ReplicationSectionData {
    pub fn new() -> Self {
        ReplicationSectionData {
            role: None,          // Default role is Master
            master_replid: None, // Empty string by default
            master_repl_offset: Some(0),
        }
    }

    /// This is used when the master receives a REPLCONF ACK N from a replica.
    /// When that happens, the old value must be erased and the new value added.
    pub fn reset_replica_offset(&mut self) {
        self.master_repl_offset = Some(0);
    }
}

// implement new for InfoSectionData
// impl ReplicationSectionData {
//     // Generates a random alphanumeric string of 40 characters to serve as a replication ID.
//     // This method is useful for creating unique identifiers for replication purposes in Redis setups.
//     pub fn generate_replication_id() -> String {
//         // Initialize a random number generator based on the current thread.
//         let mut rng = thread_rng();

//         // Create a sequence of 40 random alphanumeric characters.
//         let repl_id: String = iter::repeat(())
//             // Map each iteration to a randomly chosen alphanumeric character.
//             .map(|()| rng.sample(Alphanumeric))
//             // Convert the sampled character into its char representation.
//             .map(char::from)
//             .take(40) // Take only the first 40 characters.
//             .collect(); // Collect the characters into a String.

//         repl_id
//     }

//     // pub fn new() -> Self {
//     //     Self {
//     //         // role: Value is "master" if the instance is replica of no one
//     //         role,
//     //         // master_replid: The ID of the master instance
//     //         master_replid: "".to_string(),
//     //         // Each master also maintains a "replication offset" corresponding to how many bytes of commands
//     //         // have been added to the replication stream.
//     //         // This is tracked in replicator actor Hash PER REPLICA.
//     //         master_repl_offset: 0,
//     //     }
//     // }
// }

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
pub struct WaitCommandParameter {
    pub numreplicas: u16,
    pub timeout: u16,
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
