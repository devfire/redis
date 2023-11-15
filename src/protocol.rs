/// Enum for types defined in RESP specification.
/// Its variants contain Vec<u8> or Option<Vec<u8>> for optional types (i.e. Bulk Strings and Arrays).
/// https://redis.io/docs/reference/protocol-spec/

#[derive(Debug, Clone, PartialEq)]
pub enum RespFrame {
    /// Simple string in RESP.
    ///
    /// # Examples
    /// ```
    /// let simple_string = parser::resp(&b"+OK\r\n"[..]);
    /// if let (_, protocol::Resp::SimpleString(value)) = simple_string.unwrap() {
    ///   assert_eq!(value, b"OK".to_vec());
    /// }
    /// ```
    SimpleString(Vec<u8>),
    /// Integer in RESP. Contains i64 value.
    ///
    /// # Examples
    /// ```
    /// let integer = parser::resp(&b":8\r\n"[..]);
    /// if let (_, protocol::Resp::Integer(value)) = integer.unwrap() {
    ///   assert_eq!(value, 8);
    /// }
    /// ```
    Integer(i64),
    /// Error in RESP.
    ///
    /// # Examples
    /// ```
    /// let error = parser::resp(&b"-ERROR\r\n"[..]);
    /// if let (_, protocol::Resp::Error(value)) = error.unwrap() {
    ///   assert_eq!(value, b"ERROR".to_vec());
    /// }
    /// ```
    Error(Vec<u8>),
    /// Bulk String in RESP, contains None if encounters empty string.
    ///
    /// # Examples
    /// ```
    /// /// Bulk String
    /// let bulk_string = parser::resp(&b"$3\r\nstr\r\n"[..]);
    /// if let (_, protocol::Resp::BulkString(Some(value))) = bulk_string.unwrap() {
    ///   assert_eq!(value, b"str".to_vec());
    /// }
    ///
    /// use std::matches;
    /// /// Empty Bulk String
    /// let empty_bulk_string = parser::resp(&b"$0\r\n"[..]);
    /// assert!(matches!(parser::Resp::BulkString(None), empty_bulk_string));
    /// ```
    BulkString(Option<Vec<u8>>),
    /// Array in RESP, contains None if encounters empty array.
    ///
    /// # Examples
    /// ```
    /// /// Bulk String
    /// let bulk_string = parser::resp(&b"$3\r\nstr\r\n"[..]);
    /// if let (_, protocol::Resp::BulkString(Some(value))) = bulk_string.unwrap() {
    ///   assert_eq!(value, b"str".to_vec());
    /// }
    ///
    /// use std::matches;
    /// /// Empty Bulk String
    /// let empty_bulk_string = parser::resp(&b"$0\r\n"[..]);
    /// assert!(matches!(protocol::Resp::BulkString(None), empty_bulk_string));
    /// ```
    Array(Option<Vec<RespFrame>>),
}



#[derive(Debug, Clone)]
pub enum RespDataType {
    // first byte is +
    SimpleString(String),
}

#[derive (Debug,PartialEq)]
pub enum Command {
    Ping,
    Echo(String),
}