#[derive(Debug)]
pub enum RedisCommand {
    Ping,
    Echo(String),
    Command,
    Set(SetCommandParameters), // key, value tuple for the Set command
    Get(String),
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
    KEEPTTL
}
