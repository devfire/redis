use nom::{Err, Needed};
use tokio_util::codec::Decoder;

use bytes::{Buf, BytesMut};

use crate::errors::RedisError;

use super::{format::Rdb, parsers::parse_rdb_file};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RdbCodec {}

impl RdbCodec {
    /// Creates a new [`MessageCodec`].
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for RdbCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for RdbCodec {
    //NOTE: #[from] std::io::Error is required in the error definition
    type Error = RedisError;

    type Item = Rdb;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        match parse_rdb_file(src) {
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
