// use resp::Value;

use std::str::FromStr;

use log::info;
use redis_starter_rust::protocol::Request;
use resp::{encode, Value};
use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf};

pub async fn handle_array(array: Vec<Value>, writer: &mut OwnedWriteHalf) {
    // Handle the array of requests.
    // https://redis.io/docs/reference/protocol-spec/#arrays
    // NOTE: arrays can contain mixed data types. See link above for details.
    for req in array {
        // info!("Processing array value: {:?}", req);
        match req {
            Value::Bulk(bulk_string) => {
                info!("Processing value: {}", bulk_string);

                let command = Request::from_str(&bulk_string)
                    .expect("Unable to convert bulk string to protocol command");
                match command {
                    Request::Ping => {
                        // Encode a "PONG" response
                        let pong = encode(&Value::Bulk("PONG".into()));

                        // Write the response to the client
                        writer
                            .write_all(&pong)
                            .await
                            .expect("Unable to write back.");
                    }
                    Request::Command => {
                        info!("{} received, ignoring.", command)
                    }
                    Request::Docs => {
                        info!("{} received, ignoring.", command)
                    }
                    //_ => error!("Unknown command supplied"),
                }
            }
            Value::Null => todo!(),
            Value::NullArray => todo!(),
            Value::String(_) => todo!(),
            Value::Error(_) => todo!(),
            Value::Integer(_) => todo!(),
            Value::BufBulk(_) => todo!(),
            Value::Array(_) => todo!(),
        }
    }

    // Check if the command is "PING"
    // if let Value::Array(array) = decoded {
    //     if let Value::BulkString(ping) = &array[0] {
    //         if ping.as_str() == "PING" {
    //             // Encode a "PONG" response
    //             let pong = encode(&Value::BulkString("PONG".into())).unwrap();

    //             // Write the response to the client
    //             writer.write_all(&pong).await.unwrap();
    //         }
    //     }
    // }
}
