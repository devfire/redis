use core::fmt;

#[derive(Debug)]
pub enum RedisCommand {
    Ping,
    Echo(String),
    Command,
    Set(SetCommandParameters),
    Get(String),
    Del(Vec<String>),
    Strlen(String),                  // https://redis.io/commands/strlen
    Mget(Vec<String>),               // https://redis.io/commands/mget
    Append(String, String),          // https://redis.io/commands/append/
    Config(ConfigCommandParameters), // CONFIG GET
    Keys(String),
}

// SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]
#[derive(Clone, Debug)]
pub struct SetCommandParameters {
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
pub enum SetCommandExpireOption {
    EX(u32), // unix timestamp seconds
    PX(u64), // unix timestamp milliseconds
    EXAT(usize),
    PXAT(usize),
    KEEPTTL,
}

// these are passed from the command line
#[derive(Debug, Clone, PartialEq, Copy, Eq, Hash)]
pub enum ConfigCommandParameters {
    Dir,
    DbFilename,
}

// this is needed to convert the enum variants to strings
impl fmt::Display for ConfigCommandParameters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigCommandParameters::Dir => write!(f, "dir"),
            ConfigCommandParameters::DbFilename => write!(f, "dbfilename"),
        }
    }
}
