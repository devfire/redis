use resp::Value;
use strum_macros::{Display, EnumString};
// pub type SetCommandType = (String, Vec<Value>);
pub type SetCommandType = (String, Value);

#[derive(Debug, PartialEq, EnumString, Display)]
#[strum(ascii_case_insensitive)]
pub enum RedisCommand {
    Ping,
    Echo(Option<Value>),
    Command(Option<Value>),
    Set(Option<SetCommandType>), // key, value tuple for the Set command
    Get(Option<String>),
}
