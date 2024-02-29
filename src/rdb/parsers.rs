use log::info;
use nom::{
    branch::alt,
    bytes::{complete::tag, streaming::take},
    combinator::map,
    number::streaming::le_u8,
    IResult,
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
    let (input, first_byte) = nom::number::complete::le_u8(input)?;
    let (input, length) = match first_byte & 0b1100_0000 {
        0b0000_0000 => {
            // 00: The next 6 bits represent the length
            let length = (first_byte & 0b0011_1111) as u32;
            (input, length)
        }
        0b0100_0000 => {
            // 01: Read one additional byte. The combined 14 bits represent the length
            let (input, next_byte) = nom::number::complete::le_u8(input)?;
            let length = ((first_byte as u32 & 0b0011_1111) << 8) | next_byte as u32;
            (input, length)
        }
        0b1000_0000 => {
            // 10: Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
            let (input, length) = nom::number::complete::le_u32(input)?;
            (input, length)
        }
        _ => {
            // 11: The next object is encoded in a special format. The remaining 6 bits indicate the format.
            // This is a placeholder for handling special formats, which is beyond the scope of this example.
            // return Err(nom::Err::Failure((input, nom::error::ErrorKind::Fail)));
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Fail,
            )));
        }
    };

    Ok((input, length))
}

/// Auxiliary field. May contain arbitrary metadata such as
/// Redis version, creation time, used memory.
/// first comes the key, then the value. Both are strings.
//
fn parse_rdb_aux(input: &[u8]) -> IResult<&[u8], Rdb> {
    let (input, _aux_opcode) = tag([0xFA])(input)?;

    // taking the key first
    let (input, key_length) = (parse_rdb_length)(input)?;
    let (input, key) = take(key_length)(input)?;

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
    alt((
        // map(tag_no_case("*1\r\n$4\r\nPING\r\n"), |_| RedisCommand::Ping),
        parse_rdb_header,
        parse_op_code_eof,
        parse_op_code_selectdb,
        parse_rdb_aux,
    ))(input)
}
