#[derive(Debug)]
pub enum RedisCommand {
    Ping,
    Echo(String),
    Command,
    Set(SetCommandParameters),
    Get(String),
    Del(Vec<String>),
    Strlen(String),         // https://redis.io/commands/strlen
    Mget(Vec<String>),      // https://redis.io/commands/mget
    Append(String, String), // https://redis.io/commands/append/
    Config(ConfigCommandParameters),         // CONFIG GET
}

// SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]
#[derive(Clone, Debug)]
pub struct SetCommandParameters {
    pub key: String,
    pub value: String,
    pub option: Option<SetCommandSetOption>,
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
    EX(usize),
    PX(usize),
    EXAT(usize),
    PXAT(usize),
    KEEPTTL,
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConfigCommandParameters {
    Dir,
    DbFilename,
}

