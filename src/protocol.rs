#[derive(Debug)]
pub enum RedisCommand {
    Ping,
    Echo(String),
    Command,
    Set(SetCommandParameters), // key, value tuple for the Set command
    Get(String),
}

// SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]
#[derive(Debug)]
pub struct SetCommandParameters {
    pub key: String,
    pub value: String,
    pub option: Option<String>,
    pub get: Option<String>,
    pub expire: Option<String>,
}

// #[derive(Debug)]
// pub enum SetCommandSetOption {
//     NX,
//     XX,
// }