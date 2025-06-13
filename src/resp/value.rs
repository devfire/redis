use bytes::BytesMut;
use std::io::{Error, ErrorKind};
use tracing::debug;

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
    /// $<length_of_file>\r\n<contents_of_file>
    /// This is similar to how Bulk Strings are encoded, but without the trailing \r\n
    Rdb(Vec<u8>),
}

impl RespValue {
    /// Used to create client requests.
    pub fn array_from_slice(slice: &[&str]) -> Self {
        RespValue::Array(
            slice
                .iter()
                .map(|&s| RespValue::BulkString(Some(s.as_bytes().to_vec())))
                .collect(),
        )
    }

    /// Encodes a RespValue into RESP protocol format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buffer = BytesMut::new();
        self.encode_to_buffer(&mut buffer);
        buffer.to_vec()
    }

    /// Encodes a RespValue into the provided buffer.
    pub fn encode_to_buffer(&self, dst: &mut BytesMut) {
        match self {
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
                dst.extend_from_slice(data);
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
                    item.encode_to_buffer(dst);
                }
            }
            RespValue::Null => {
                dst.extend_from_slice(b"$-1\r\n");
            }
            RespValue::NullArray => {
                dst.extend_from_slice(b"*-1\r\n");
            }
            RespValue::Rdb(rdb) => {
                dst.extend_from_slice(b"$");
                dst.extend_from_slice(rdb.len().to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                dst.extend_from_slice(rdb);
            }
        }
    }

    pub fn to_encoded_string(&self) -> anyhow::Result<String> {
        let bytes = self.encode();
        let encoded_string = String::from_utf8(bytes);
        match encoded_string {
            Ok(s) => {
                debug!("Encoded string: {}", s);
                Ok(s)
            }
            Err(e) => Err(Error::new(ErrorKind::InvalidData, e).into()),
        }
    }
}
