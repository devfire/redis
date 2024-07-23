use tracing::info;
use tokio_util::codec::{Decoder, Encoder};

use bytes::{Buf, BufMut, BytesMut};
use nom::{Err, Needed};

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

        match parse_resp(src) {
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