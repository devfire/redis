use tracing::info;
use tokio_util::codec::{Decoder, Encoder};

use bytes::{Buf, BufMut, BytesMut};
use nom::{Err, Needed};

use crate::errors::RedisError;

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