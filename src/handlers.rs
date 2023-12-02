use std::str::FromStr;

use log::info;
use resp::Value;

use crate::protocol::RedisCommand;


pub fn handler(value: Value, array: &mut Vec<Value>) -> Option<RedisCommand> {
    match value {
        Value::Bulk(raw_string) => {
            // https://docs.rs/strum_macros/0.25.3/strum_macros/derive.EnumString.html
            // this is crafting a possible enum variant from the string.
            // So, if we are passed "PING" in raw_string, then this will construct a Command::Ping enum variant
            let input_variant =
                RedisCommand::from_str(&raw_string).expect("Command::from_str failed");

            // now we try to match our variant against the arms of known command matches
            match input_variant {
                RedisCommand::Ping => Some(RedisCommand::Ping), // got a ping!
                RedisCommand::Command(_) => {
                    // Command is tricky because the RESP format is COMMAND DOCS so we need to grab this COMMAND command
                    // and then take the one immediately following COMMAND.
                    info!("Assembling COMMAND");

                    // we are popping off one more element to grab the message that followed ECHO
                    // let's make sure the array is not empty first, ECHO can be by itself with no message
                    if !array.is_empty() {
                        let command_message = array.remove(0); // 0th element, i.e. first one

                        // https://docs.rs/resp/latest/resp/enum.Value.html
                        // Remember, in COMMAND DOCS, DOCS is optional, so it needs a Some().
                        // Then this function returns an Option<> so we need one more Some().
                        return Some(RedisCommand::Command(Some(command_message)));
                    } else {
                        None
                    }
                }
                RedisCommand::Echo(_) => {
                    // Echo is tricky because the RESP format is ECHO "MESSAGE" so we need to grab this ECHO command
                    // and then take the one immediately following ECHO.
                    //
                    // Then, we are popping off one more element to grab the message that followed ECHO
                    // let's make sure the array is not empty first, ECHO can be by itself with no message
                    let mut echo_message = Value::String("".to_string());

                    if !array.is_empty() {
                        echo_message = array.remove(0); // 0th element, i.e. first one

                        info!("Assembling ECHO + {:?}", echo_message);
                        //https://docs.rs/resp/latest/resp/enum.Value.html
                        // Remember, Echo "MESSAGE", message is optional, so it needs a Some().
                        // Then this function returns an Option<> so we need one more Some().
                    }

                    // Looks like we got ECHO followed by nothing so simply reply with an empty string.
                    return Some(RedisCommand::Echo(Some(echo_message)));
                } // _ => None,
            }
        }
        Value::BufBulk(_) => todo!(),
        // TODO: Need to handle nested arrays.
        // Value::Array(array) => {
        //     // if let Some(element) = array.remove(0) {
        //     //     handler(element)
        //     // }
        // },
        _ => return None,
    }
}
