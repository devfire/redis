pub enum Rdb {
    MagicString,
    Version(String),
    OpCode(OpCode),
    Type(String),
    ExpiryTime(String),
    // Each key value pair has 4 parts:
    //
    // Key Expiry Timestamp. This is optional.
    // 1 byte flag indicating the value type.
    // The key, encoded as a Redis String. See String Encoding.
    // The value, encoded according to the value type. See Value Encoding.
    KeyValuePair {
        key_expiry_time: Option<u16>,
        value_type: u8,
        key: String,
        value: String,
    },
    End,
}

pub enum OpCode {
    Eof,
    Selectdb(u16),
    Expiretime(u32),
    ExpiretimeMs(u64),
    ResizeDb(u32),
    Aux,
}
