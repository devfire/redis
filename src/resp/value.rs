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
    BulkString(Option<String>),
    /// For Bulk <binary> Strings the first byte of the reply is "$".
    // BufBulk(Vec<u8>),
    /// For Arrays the first byte of the reply is "*".
    Array(Vec<RespValue>),
}