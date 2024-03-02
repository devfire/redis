// use std::{fs, path::Path};

// use bytes::Bytes;

// use clap::builder::Str;

#[derive(Debug)]
pub enum Rdb {
    RdbHeader {magic: String, version: String},
    OpCode {opcode: RdbOpCode},
    Type(String),
    // ExpiryTime(String),
    // Each key value pair has 4 parts:
    //
    // Key Expiry Timestamp. This is optional.
    // 1 byte flag indicating the value type.
    // The key, encoded as a Redis String. See String Encoding.
    // The value, encoded according to the value type. See Value Encoding.
    KeyValuePair {
        key_expiry_time: Option<u32>,
        value_type: ValueType,
        key: String,
        value: String,
    },
    End,
}

#[derive(Debug, PartialEq)]
pub enum ValueType {
    StringEncoding,
    ListEncoding,
    SetEncoding,
}

// impl Rdb {
//     pub fn load(fullpath: &str) -> String {
//         // TODO: load from file
//         let contents = fs::read_to_string(fullpath).expect("Unable to read the RDB file");

//         // let db = std::fs::File::open(&Path::new(&fullpath)).expect("Failed to load config file.");

//         // let reader = std::io::BufReader::new(db);

//         // This is just a test parse to display the rdb contents.
//         // Otherwise, it is not doing anything else.
//         // rdb::parse(
//         //     reader,
//         //     rdb::formatter::JSON::new(),
//         //     rdb::filter::Simple::new(),
//         // )
//         // .expect("Unable to parse config file.");

//         contents
//     }


// }

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