use strum_macros::{Display, EnumString};

#[derive(Debug, PartialEq, EnumString, Display)]
#[strum(ascii_case_insensitive)]
pub enum RedisCommand {
    Ping,
    Echo(Option<String>),
    Command(Option<String>),
}
<<<<<<< HEAD


#[derive(Debug, Clone)]
pub enum RespDataType {
    // first byte is +
    SimpleString(String),
}
=======
>>>>>>> resp-command-parser
