use strum_macros::EnumString;

#[derive(EnumString)]
pub enum Request {
    Ping,
    Command,
    Docs,
}

pub enum Response {
    Pong,
}