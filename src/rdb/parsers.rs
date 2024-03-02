use clap::error;
use log::{error, info};
use nom::{
    branch::alt,
    bytes::{complete::tag, streaming::take},
    //    combinator::map,
    //  number::streaming::{be_u8, le_u8},
    IResult,
};

use super::format::{LengthEncoding, Rdb, RdbOpCode};

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

fn parse_rdb_length(input: &[u8]) -> IResult<&[u8], LengthEncoding> {
    let (input, first_byte) = nom::number::streaming::be_u8(input)?;

    // This is either length, or format number of bits or compressed type
    // ::Compressed is purely for initialization purposes.
    let mut length_encoding = (input, LengthEncoding::Compressed);
    match first_byte & 0b1100_0000 {
        0b0000_0000 => {
            // 00: The next 6 bits represent the length
            let length = (first_byte & 0b0011_1111) as u32;
            // Ok((input, LengthEncoding::StringLength(length)))
            length_encoding = (input, LengthEncoding::StringLength(length));
        }
        0b0100_0000 => {
            // 01: Read one additional byte. The combined 14 bits represent the length
            let (input, next_byte) = nom::number::streaming::be_u8(input)?;
            let length = ((first_byte as u32 & 0b0011_1111) << 8) | next_byte as u32;
            length_encoding = (input, LengthEncoding::StringLength(length));
            // Ok((input, LengthEncoding::StringLength(length)))
        }
        0b1000_0000 => {
            // 10: Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
            let (input, length) = nom::number::streaming::be_u32(input)?;
            length_encoding = (input, LengthEncoding::StringLength(length));
            // Ok((input, LengthEncoding::StringLength(length)))
        }
        0b1100_0000 => {
            info!("11: special format detected!");
            // 11: The next object is encoded in a special format. The remaining 6 bits indicate the format.
            // let (input, length) = nom::number::streaming::be_u32(input)?;
            let format = (first_byte & 0b0011_1111) as u32;
            match format {
                0b0000_0000 => {
                    info!("8 bit integer follows!");
                    // (input, LengthEncoding::Format(8))
                    length_encoding = (input, LengthEncoding::StringLength(8));
                }
                0b0100_0000 => {
                    info!("16 bit integer follows!");
                    length_encoding = (input, LengthEncoding::StringLength(16));
                    // Ok((input, LengthEncoding::Format(16)))
                }
                0b1000_0000 => {
                    info!("32 bit integer follows!");
                    // Ok((input, LengthEncoding::Format(32)))
                    length_encoding = (input, LengthEncoding::StringLength(32));
                }
                0b1100_0000 => {
                    info!("Compressed string follows!");
                    // return Ok((input, LengthEncoding::Compressed));
                    length_encoding = (input, LengthEncoding::Compressed);
                }
                _ => {
                    error!("Unknown length encoding.");
                    // return Err(nom::Err::Failure(nom::error::Error::new(
                    //     input,
                    //     nom::error::ErrorKind::Fail,
                    // )));
                }
            }
            // Ok((input, length_encoding))
        }
        _ => {
            error!("No suitable length encoding bit match");
            // Something really bad happened.
            // return Err(nom::Err::Failure(nom::error::Error::new(
            //     input,
            //     nom::error::ErrorKind::Fail,
            // )));
        }
    };
    Ok(length_encoding)
    // info!("Calculated length: {}", length);
    // Ok((input, length))
}

/// Auxiliary field. May contain arbitrary metadata such as
/// Redis version, creation time, used memory.
/// first comes the key, then the value. Both are strings.
//
fn parse_rdb_aux(input: &[u8]) -> IResult<&[u8], Rdb> {
    let (input, _aux_opcode) = tag([0xFA])(input)?;

    // taking the key first
    let (input, key_length_encoding) = (parse_rdb_length)(input)?;

    let mut key_length: u32 = 0;
    let mut value_length: u32 = 0;

    match key_length_encoding {
        // the key is always a valid string, so we can ignore non strings
        LengthEncoding::StringLength(len) => {
            key_length = len;
            info!("Key length: {}", len);
        }
        _ => {
            error!("Something terrible has happened.")
        }
    }
    let (input, key) = take(key_length)(input)?;

    // let (input, key_length) = (parse_rdb_length)(input)?;

    // taking the value next
    // Now, this value length can be length, or format, or compressed.
    let (input, value_length_encoding) = (parse_rdb_length)(input)?;
    match value_length_encoding {
        LengthEncoding::StringLength(len) => {
            value_length = len;
            info!("Value length: {}", len);
        }
        LengthEncoding::Compressed => todo!(),
    }

    // let (input, value_length) = (parse_rdb_length)(input)?;
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
