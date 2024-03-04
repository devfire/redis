use log::{error, info};
use nom::{
    branch::alt,
    bytes::{complete::tag, streaming::take},
    number::streaming::{le_u16, le_u32, le_u8},
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
    info!("SELECTDB OpCode detected.");
    let (input, _dbselector) = tag([0xFE])(input)?;
    let (input, length_type) = (parse_rdb_length)(input)?;

    let length: u32 = 0;
    match length_type {
        LengthEncoding::StringLength(length) => {
            info!("SELECTDB length: {}", length);
        }
        _ => todo!(),
    }

    let (input, db_number) = (take(length))(input)?;

    info!(
        "SELECTDB number: {:?} length: {}",
        std::str::from_utf8(db_number),
        length
    );

    Ok((
        input,
        Rdb::OpCode {
            opcode: RdbOpCode::Selectdb,
        },
    ))
}

fn parse_rdb_length(input: &[u8]) -> IResult<&[u8], LengthEncoding> {
    let (input, first_byte) = le_u8(input)?;
    let two_most_significant_bits = (first_byte & 0b11000000) >> 6;

    info!(
        "First byte: {:08b} two most significant bits: {:08b}",
        first_byte, two_most_significant_bits
    );

    // This is either length, or format number of bits or compressed type
    // ::Compressed is purely for initialization purposes.
    let mut length_encoding = (input, LengthEncoding::Compressed);
    match first_byte & 0b1100_0000 {
        0 => {
            // 00: The next 6 bits represent the length
            let length = (first_byte & 0b0011_1111) as u32;
            // Ok((input, LengthEncoding::StringLength(length)))
            length_encoding = (input, LengthEncoding::StringLength(length));
        }
        1 => {
            // 01: Read one additional byte. The combined 14 bits represent the length
            let (input, next_byte) = le_u8(input)?;
            let length = ((first_byte as u32 & 0b0011_1111) << 8) | next_byte as u32;
            length_encoding = (input, LengthEncoding::StringLength(length));
            // Ok((input, LengthEncoding::StringLength(length)))
        }
        2 => {
            // 10: Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
            let (input, length) = le_u32(input)?;
            length_encoding = (input, LengthEncoding::StringLength(length));
            // Ok((input, LengthEncoding::StringLength(length)))
        }
        0b1100_0000 => {
            info!("11: special format detected!");
            // 11: The next object is encoded in a special format. The remaining 6 bits indicate the format.
            // let (input, length) = nom::number::streaming::be_u32(input)?;
            let format = (first_byte & 0b0011_1111) as u32;
            match format {
                0 => {
                    info!("8 bit integer follows!");
                    length_encoding = (input, LengthEncoding::IntegerBits(8));
                }
                1 => {
                    info!("16 bit integer follows!");
                    length_encoding = (input, LengthEncoding::IntegerBits(16));
                }
                2 => {
                    info!("32 bit integer follows!");
                    length_encoding = (input, LengthEncoding::IntegerBits(32));
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
    info!("Calculated length: {:?}", length_encoding.1);
    Ok(length_encoding)

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
    // let mut value_length: u32 = 0;
    // let mut integer_type: u8 = 0;

    match key_length_encoding {
        // the key is always a valid string, so we can ignore non strings
        LengthEncoding::StringLength(kl) => {
            key_length = kl;
        }
        _ => {
            error!("In parse_rdb_aux something terrible has happened.")
        }
    }

    info!("Key length: {}", key_length);
    let (input, key) = take(key_length)(input)?;

    // taking the value next
    // Now, this value length can be length, or format, or compressed.
    let (input, value_length_encoding) = (parse_rdb_length)(input)?;

    let (input, value) = match value_length_encoding {
        LengthEncoding::StringLength(value_length) => {
            info!("Value length: {}", value_length);
            take(value_length)(input)?
        }
        LengthEncoding::IntegerBits(integer_bits) => match integer_bits {
            8 => {
                let (i, integer) = le_u8(input)?;
                (i, integer.to_le_bytes())
            }
            16 => {
                let (i, integer) = le_u16(input)?;
                (i, integer.to_be_bytes())
            }
            32 => {
                let (i, integer) = le_u32(input)?;
                (i, integer as u32)
            }
            _ => {
                error!("Unknown integer bit sizes");
                return Err(nom::Err::Failure(nom::error::Error::new(
                    input,
                    nom::error::ErrorKind::Fail,
                )));
            }
        },
        _ => todo!(),
    };

    // let (input, value_length) = (parse_rdb_length)(input)?;

    // info!(
    //     "AUX key: {:?} value {:?}",
    //     std::str::from_utf8(key),
    //     std::str::from_utf8(value)
    // );

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
