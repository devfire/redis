// use std::{fs, path::Path};

// use bytes::Bytes;

// use clap::builder::Str;

#[derive(Debug)]
pub enum Rdb {
    RdbHeader {
        magic: String,
        version: String,
    },
    OpCode {
        opcode: RdbOpCode,
    },
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

// We need this because of rdb length encoding:
// Length encoding is used to store the length of the next object in the stream. 
// Length encoding is a variable byte encoding designed to use as few bytes as possible.
// 
// This is how length encoding works : Read one byte from the stream, compare the two most significant bits:
//
// Bits	How to parse
// 00	The next 6 bits represent the length
// 01	Read one additional byte. The combined 14 bits represent the length
// 10	Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
// 11	The next object is encoded in a special format. The remaining 6 bits indicate the format.
// So, in case of 0b11 the next 6 bits are NOT the length, it's the a special format.
//
#[derive(Debug)]
pub enum LengthEncoding {
    StringLength(u32),
    // Format(u32),
    Compressed
}

#[derive(Debug)]
pub enum RdbOpCode {
    Eof,
    Selectdb,
    // Expiretime(u32),
    // ExpiretimeMs(u64),
    // ResizeDb(u32),
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
