use log::info;

use crate::protocol::RespFrame;

pub fn handle_array(array: Vec<RespFrame>) {
    for command in array {
        match command {
            RespFrame::SimpleString(_) => todo!(),
            RespFrame::Integer(_) => todo!(),
            RespFrame::Error(_) => todo!(),
            RespFrame::BulkString(bulk_string) => {
                // remember, BulkString(Option<Vec<u8>>)
                // we may have gotten an empty string
                if let Some(command_vec) = bulk_string {
                    let command_string =
                        String::from_utf8(command_vec).expect("Conversion to utf8 failed");
                    info!("Bulk string: {}", command_string);
                    match command_string.to_lowercase().as_str() {
                        "ping" => {
                            info!("Got ping, sending pong");
                        }
                        _ => {
                            info!("{} is not handled at the moment", command_string)
                        }
                    }
                }
            }
            RespFrame::Array(_) => todo!(),
        }
    }
}
