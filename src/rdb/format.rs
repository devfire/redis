// use std::{fs, path::Path};

// use bytes::Bytes;

// use clap::builder::Str;

use crate::protocol::SetCommandExpireOption;

#[derive(Debug)]
pub enum Rdb {
    RdbHeader {
        magic: String,
        version: String,
    },
    OpCode {
        opcode: RdbOpCode,
    },
    //   Type(String),
    // ExpiryTime(String),
    // Each key value pair has 4 parts:
    //
    // Key Expiry Timestamp. This is optional.
    // 1 byte flag indicating the value type.
    // The key, encoded as a Redis String. See String Encoding.
    // The value, encoded according to the value type. See Value Encoding.
    KeyValuePair {
        key_expiry_time: Option<SetCommandExpireOption>,
        value_type: ValueType,
        key: String,
        value: String,
    },
    //    End,
}

#[derive(Debug, PartialEq, Clone)]
pub enum ValueType {
    LengthEncoding { length: u32, special: bool },
    StringEncoding,
    ListEncoding,
    // SetEncoding,
}

impl ValueType {
    // this only applies to LengthEncodings.
    // if this gets called on some other enum variant simply return 0,
    // this makes it easier to deal with, without if let Some()
    pub fn get_length(&self) -> u32 {
        match self {
            ValueType::LengthEncoding { length, .. } => *length,
            _ => 0,
        }
    }

    pub fn is_special(&self) -> bool {
        match self {
            ValueType::LengthEncoding { special, .. } => *special,
            _ => false, // Default value
        }
    }
}

#[derive(Debug)]
pub enum RdbOpCode {
    Eof(u64), //checksum
    Selectdb,
    // Expiretime(u32),
    // ExpiretimeMs(u64),
    ResizeDb {
        db_hash_table_length: u32,
        expiry_hash_table_length: u32,
    },
    Aux,
}

// impl RdbOpCode {
//     fn from_u8(value: u8) -> Option<Self> {
//         match value {
//             0xFF => Some(RdbOpCode::Eof),
//             0xFE => Some(RdbOpCode::Selectdb),
//             // Add other opcodes and their corresponding values here
//             _ => None,
//         }
//     }
// }
