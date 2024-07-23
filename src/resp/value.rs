use bytes::{BufMut, BytesMut};
use tokio_util::codec::Encoder;

use super::codec::RespCodec;

/// Represents a RESP value, see [Redis Protocol specification](http://redis.io/topics/protocol).
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum RespValue {
    /// Null bulk reply, `$-1\r\n`
    Null,
    /// Null array reply, `*-1\r\n`
    NullArray,
    /// For Simple Strings the first byte of the reply is "+".
    SimpleString(String),
    /// For Errors the first byte of the reply is "-".
    Error(String),
    /// For Integers the first byte of the reply is ":".
    Integer(i64),
    /// For Bulk Strings the first byte of the reply is "$".
    BulkString(Option<Vec<u8>>),
    /// For Bulk <binary> Strings the first byte of the reply is "$".
    // BufBulk(Vec<u8>),
    /// For Arrays the first byte of the reply is "*".
    Array(Vec<RespValue>),
}

impl RespValue {
    pub fn array_from_slice(slice: &[&str]) -> Self {
        RespValue::Array(
            slice
                .iter()
                .map(|&s| RespValue::BulkString(Some(s.as_bytes().to_vec())))
                .collect(),
        )
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buffer = BytesMut::new();
        let mut codec = RespCodec::new();
        codec
            .encode(self.clone(), &mut buffer)
            .expect("Encoding should not fail");
        buffer.to_vec()
    }
}
