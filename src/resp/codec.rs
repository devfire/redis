use tokio_util::codec::{Decoder, Encoder};
// use tracing::info;

use bytes::{Buf, BytesMut};
use nom::{Err, Needed};
use tracing::error;

use crate::errors::RedisError;

use super::{parsers::parse_resp, value::RespValue};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RespCodec {}

impl RespCodec {
    /// Creates a new [`RespCodec`].
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for RespCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for RespCodec {
    type Error = RedisError;

    type Item = RespValue;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        // convert decimal ascii to string
        tracing::info!("Decoding: {:?}", src);

        match parse_resp(src) {
            Ok((remaining_bytes, parsed_message)) => {
                // advance the cursor by the difference between what we read
                // and what we parsed
                src.advance(src.len() - remaining_bytes.len());

                // return the parsed message
                Ok(Some(parsed_message))
            }
            Err(Err::Incomplete(Needed::Size(_))) => Ok(None),

            Err(e) => {
                error!("Error {} parsing RESP message: {:?}", e, src);
                Err(RedisError::ParseFailure)
            }
        }
    }
} // end of impl Decoder for RespCodec

// now let's implement the Encoder
impl Encoder<RespValue> for RespCodec {
    type Error = RedisError;

    fn encode(&mut self, item: RespValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            RespValue::SimpleString(s) => {
                dst.extend_from_slice(b"+");
                dst.extend_from_slice(s.as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            RespValue::Error(s) => {
                dst.extend_from_slice(b"-");
                dst.extend_from_slice(s.as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            RespValue::Integer(i) => {
                dst.extend_from_slice(b":");
                dst.extend_from_slice(i.to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            RespValue::BulkString(Some(data)) => {
                dst.extend_from_slice(b"$");
                dst.extend_from_slice(data.len().to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                dst.extend_from_slice(&data);
                dst.extend_from_slice(b"\r\n");
            }
            RespValue::BulkString(None) => {
                dst.extend_from_slice(b"$-1\r\n");
            }
            RespValue::Array(arr) => {
                dst.extend_from_slice(b"*");
                dst.extend_from_slice(arr.len().to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                for item in arr {
                    self.encode(item, dst)?;
                }
            }
            RespValue::Null => {
                dst.extend_from_slice(b"_\r\n");
            }
            RespValue::NullArray => todo!(),

            // Not strictly speaking a RESP type, but we use it to send RDB files to replicas.
            // The file is sent using the following format:
            // $<length_of_file>\r\n<contents_of_file>
            // (This is similar to how Bulk Strings are encoded, but without the trailing \r\n)
            RespValue::Rdb(rdb) => {
                dst.extend_from_slice(b"$");
                dst.extend_from_slice(rdb.len().to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                dst.extend_from_slice(&rdb);
            }
        }
        tracing::info!("Encoded: {:?}", dst);
        Ok(())
    } // end of fn encode
} // end of impl Encoder for RespCodec
