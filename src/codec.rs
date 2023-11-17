use bytes::BufMut;
use bytes::{Buf, BytesMut};

use log::info;
use nom::Err;
use nom::Needed;
use std::str;

use tokio_util::codec::{Decoder, Encoder};

use crate::errors;
use crate::errors::RedisError;
use crate::parser::parse_commands;
use crate::protocol::{RespDataType, Command};


#[derive(Clone, Debug)]
pub struct RespCodec {}

impl RespCodec {
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
    //NOTE: #[from] std::io::Error is required in the error definition
    type Error = RedisError;
    type Item = Vec<Command>;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        info!("Decoding a resp message {:?}", src);

        if src.is_empty() {
            return Ok(None);
        }
        match parse_commands(str::from_utf8(src).expect("BytesMut to str conversion failed")) {
            Ok((remaining_bytes, parsed_message)) => {
                // advance the cursor by the difference between what we read
                // and what we parsed
                src.advance(src.len() - remaining_bytes.len());

                // return the parsed message
                Ok(Some(parsed_message))
            }
            Err(Err::Incomplete(Needed::Size(_))) => Ok(None),
            Err(_) => Err(RedisError::ParseFailure),
        }
    }
}

impl Encoder<RespDataType> for RespCodec {
    type Error = RedisError;

    fn encode(&mut self, item: RespDataType, dst: &mut BytesMut) -> Result<(), errors::RedisError> {
        let crlf = "\r\n";

        match item {
            RespDataType::SimpleString(simple_string) => {
                dst.reserve(1);
                dst.put_u8(b'+');

                let simple_string = simple_string.to_uppercase();

                dst.put(simple_string.as_bytes());

                dst.put(crlf.as_bytes());
            }
        }
        Ok(())
    }
}
