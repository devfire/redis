// use resp::Value;

use log::info;
use resp::{Value, encode};
use tokio::{net::tcp::OwnedWriteHalf, io::AsyncWriteExt};

pub async fn handle_array(array: Vec<Value>, writer: &mut OwnedWriteHalf) {
    // Handle the array of requests.
    // https://redis.io/docs/reference/protocol-spec/#arrays
    // NOTE: arrays can contain mixed data types. See link above for details.
    for req in array {
        info!("Processing array: {:?}", req);
        match req {
            Value::Bulk(bulk_string) => {
                info!("Processing {}", bulk_string);
                if bulk_string == "PING" {
                    // Encode a "PONG" response
                    let pong = encode(&Value::Bulk("PONG".into()));
                    // Write the response to the client
                    writer.write_all(&pong).await.unwrap();

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
