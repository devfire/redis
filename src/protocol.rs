use resp::Value;
use strum_macros::{Display, EnumString};

#[derive(Debug, PartialEq, EnumString, Display)]
#[strum(ascii_case_insensitive)]
pub enum RedisCommand {
    Ping,
    Echo(Option<Value>),
    Command(Option<Value>),
}
