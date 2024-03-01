use log::{error, info};
use nom::{
    branch::alt,
    bytes::{complete::tag, streaming::take},
    combinator::map,
    number::streaming::{be_u8, le_u8},
    HexDisplay, IResult,
};
use rdb::types::RdbError;

use crate::errors::RedisError;

use super::format::{Rdb, RdbOpCode};

fn parse_rdb_header(input: &[u8]) -> IResult<&[u8], Rdb> {
    let (input, _magic) = tag("REDIS")(input)?;
    let (input, version) = take(4usize)(input)?;
    let version = String::from_utf8_lossy(version).to_string();

    Ok((
        input,
        Rdb::RdbHeader {
            magic: "REDIS".to_string(),
            version,
        },
    ))
}

// https://rdb.fnordig.de/file_format.html#op-codes
fn parse_op_code_eof(input: &[u8]) -> IResult<&[u8], Rdb> {
    let (input, _eof_marker) = tag([0xFF])(input)?;
    info!("EOF detected.");
    Ok((
        input,
        Rdb::OpCode {
            opcode: RdbOpCode::Eof,
        },
    ))
}

// A Redis instance can have multiple databases.
// A single byte 0xFE flags the start of the database selector.
// After this byte, a variable length field indicates the database number.
fn parse_op_code_selectdb(input: &[u8]) -> IResult<&[u8], Rdb> {
    let (input, _dbselector) = tag([0xFE])(input)?;
    let (input, _length) = (parse_rdb_length)(input)?;

    info!("SELECTDB OpCode detected.");
    Ok((
        input,
        Rdb::OpCode {
            opcode: RdbOpCode::Selectdb,
        },
    ))
}

fn parse_rdb_length(input: &[u8]) -> IResult<&[u8], u32> {
    let (input, first_byte) = nom::number::streaming::be_u8(input)?;
    let (input, length) = match first_byte & 0b1100_0000 {
        0b0000_0000 => {
            // 00: The next 6 bits represent the length
            let length = (first_byte & 0b0011_1111) as u32;
            (input, length)
        }
        0b0100_0000 => {
            // 01: Read one additional byte. The combined 14 bits represent the length
            let (input, next_byte) = nom::number::streaming::be_u8(input)?;
            let length = ((first_byte as u32 & 0b0011_1111) << 8) | next_byte as u32;
            (input, length)
        }
        0b1000_0000 => {
            // 10: Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
            let (input, length) = nom::number::streaming::be_u32(input)?;
            (input, length)
        }
        0b1100_0000 => {
            info!("11: special format detected!");
            // 11: The next object is encoded in a special format. The remaining 6 bits indicate the format.
            // let (input, length) = nom::number::streaming::be_u32(input)?;
            let format = (first_byte & 0b0011_1111) as u32;
            match format {
                0b0000_0000 => {
                    info!("8 bit integer follows!");
                }
                0b0100_0000 => {
                    info!("16 bit integer follows!");
                }
                0b1000_0000 => {
                    info!("32 bit integer follows!");
                }
                0b1100_0000 => {
                    info!("Compressed string follows!");
                }
                _ => {
                    error!("Unknown length encoding.");
                }
            }
            (input, format)
        }
        _ => {
            error!("No suitable length encoding bit match");
            // Something really bad happened.
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Fail,
            )));
        }
    };

    info!("Calculated length: {}", length);
    Ok((input, length))
}

// fn parse_rdb_length(input: &[u8]) -> IResult<&[u8], usize> {
//     be_u8(input).map(|(remaining, byte)| {
//         let mut len = (byte & 0b00111111) as usize; // Extract lower 7 bits
//         let mut shift = 0;

//         if (byte & 0b11000000) == 0 {
//             // One byte encoding (00)
//             return (remaining, len);
//         }
//         if (byte & 0b10000000) == 0b10000000 {
//             // Two byte encoding (01)
//             shift = 7;
//         } else if (byte & 0b11000000) == 0b11000000 {
//             // Five byte encoding (10)
//             shift = 14;
//             len = 0; // Discard the first 6 bits
//         }

//         let mut next_byte = be_u8(remaining)?;
//         len |= ((next_byte & 0b00111111) as usize) << shift;

//         (next_byte.1, len)
//     })
// }

/// Auxiliary field. May contain arbitrary metadata such as
/// Redis version, creation time, used memory.
/// first comes the key, then the value. Both are strings.
//
fn parse_rdb_aux(input: &[u8]) -> IResult<&[u8], Rdb> {
    let (input, _aux_opcode) = tag([0xFA])(input)?;

    // taking the key first
    let (input, key_length) = (parse_rdb_length)(input)?;
    let (input, key) = take(key_length)(input)?;

    info!("Key: {:?}", std::str::from_utf8(key));
    // taking the value next
    let (input, value_length) = (parse_rdb_length)(input)?;
    let (input, value) = take(value_length)(input)?;

    info!(
        "AUX key: {:?} value {:?}",
        std::str::from_utf8(key),
        std::str::from_utf8(value)
    );

    Ok((
        input,
        Rdb::OpCode {
            opcode: RdbOpCode::Aux,
        },
    ))
}

pub fn parse_rdb_file(input: &[u8]) -> IResult<&[u8], Rdb> {
    info!("Parsing: {:?}", input.to_ascii_lowercase());
    alt((
        // map(tag_no_case("*1\r\n$4\r\nPING\r\n"), |_| RedisCommand::Ping),
        parse_rdb_header,
        parse_op_code_eof,
        parse_op_code_selectdb,
        parse_rdb_aux,
    ))(input)
}
