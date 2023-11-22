use strum_macros::{Display, EnumString};

#[derive(Debug, PartialEq, EnumString, Display)]
#[strum(ascii_case_insensitive)]
pub enum Command {
    Ping,
    Echo(Option<String>),
    Command,
}
