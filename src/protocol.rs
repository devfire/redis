use strum_macros::{EnumString, Display};

#[derive(EnumString, Display)]
pub enum Request {
    Ping,
    Command,
    Docs,
}

pub enum Response {
    Pong,
}