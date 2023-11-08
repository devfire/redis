use strum_macros::{EnumString, Display};

#[derive(EnumString, Display)]
#[strum(serialize_all = "lowercase")]
pub enum Request {
    Ping,
    Command,
    Docs,
}

pub enum Response {
    Pong,
}