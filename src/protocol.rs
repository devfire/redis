use bytes::Bytes;

pub enum Request {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    Null,
    Array(Vec<Request >),
}

#[derive(Debug, Clone)]
pub enum Command {
    Ping,
    Command,
    Docs,
    Echo(String),
    Null,
}

#[derive(Debug,Clone, Copy)]
pub enum Response {
    Pong,
    Null,
}