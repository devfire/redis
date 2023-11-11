use log::info;
use tokio::net::tcp::OwnedWriteHalf;
use tokio_util::codec::FramedWrite;


use crate::{
    codec::RespCodec,
    protocol::{RespDataType, RespFrame},
};

use futures::sink::SinkExt;
// use futures::StreamExt;


pub async fn handle_array(array: Vec<RespFrame>, writer: &mut FramedWrite<OwnedWriteHalf, RespCodec>) -> anyhow::Result<()> {
    for command in array {
        match command {
            RespFrame::SimpleString(_) => todo!(),
            RespFrame::Integer(_) => todo!(),
            RespFrame::Error(_) => todo!(),
            RespFrame::BulkString(bulk_string) => {
                // remember, BulkString(Option<Vec<u8>>)
                // we may have gotten an empty string
                if let Some(command_vec) = bulk_string {
                    // from bulk to Rust string returns a Result, we need to handle it
                    let command_string =
                        String::from_utf8(command_vec).expect("Conversion to utf8 failed");

                    info!("Bulk string: {}", command_string);
                    match command_string.to_lowercase().as_str() {
                        "ping" => {
                            info!("Got ping, sending pong");
                            let reply = RespDataType::SimpleString(String::from("pong"));
                            writer.send(reply).await?;
                            
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

    Ok(())
}
