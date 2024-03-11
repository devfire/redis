use log::{error, info};
use nom::{
    branch::alt,
    bytes::{complete::tag, streaming::take},
    character::streaming::u8,
    combinator::{map, opt, value},
    number::streaming::{le_u32, le_u8},
    sequence::{preceded, tuple},
    IResult,
};

use super::format::{Rdb, RdbOpCode, ValueType};

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
    let (input, length) = (parse_rdb_length)(input)?;

    let (input, db_number) = (take(length))(input)?;

    info!("Db number: {:?}", std::str::from_utf8(db_number));

    info!("SELECTDB OpCode detected.");
    Ok((
        input,
        Rdb::OpCode {
            opcode: RdbOpCode::Selectdb,
        },
    ))
}

fn parse_rdb_length(input: &[u8]) -> IResult<&[u8], u32> {
    let (input, first_byte) = le_u8(input)?;
    let two_most_significant_bits = (first_byte & 0b11000000) >> 6;
    info!(
        "First byte: {:08b} two most significant bits: {:08b}",
        first_byte, two_most_significant_bits
    );

    let (input, length) = match two_most_significant_bits {
        0 => {
            // 00: The next 6 bits represent the length
            let length = (first_byte & 0b0011_1111) as u32;
            (input, length)
        }
        1 => {
            // 01: Read one additional byte. The combined 14 bits represent the length
            let (input, next_byte) = le_u8(input)?;
            let length = (((first_byte & 0b0011_1111) as u32) << 8) | next_byte as u32;
            (input, length)
        }
        2 => {
            // 10: Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
            let (input, length) = le_u32(input)?;
            (input, length)
        }
        3 => {
            info!("11: special format detected!");
            // 11: The next object is encoded in a special format. The remaining 6 bits indicate the format.
            // let (input, length) = nom::number::streaming::be_u32(input)?;
            let format = (first_byte & 0b0011_1111) as u32;
            info!("Format: {:b}", format);
            let mut length = 0;
            match format {
                0 => {
                    info!("8 bit integer follows!");
                    length = 1 // 8;
                }
                1 => {
                    info!("16 bit integer follows!");
                    length = 2 // 16;
                }
                2 => {
                    info!("32 bit integer follows!");
                    length = 4;
                }
                0b11 => {
                    info!("Compressed string follows!");
                }
                _ => {
                    error!("Unknown length encoding.");
                }
            }
            (input, length)
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
    info!("Value: {:?}", std::str::from_utf8(value));

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

// fn parse_value_type(input: &[u8]) -> IResult<&[u8], u8> {
//     // let's grab 1 byte
//     let (input, value_type) = nom::number::streaming::le_u8(input)?;

//     Ok((input, value_type))
// }

fn parse_value_type(input: &[u8]) -> IResult<&[u8], ValueType> {
    preceded(
        u8, // This consumes the first byte, which is the flag indicating the encoding type
        alt((
            map(tag(&[0u8]), |_| ValueType::StringEncoding),
            map(tag(&[1u8]), |_| ValueType::ListEncoding),
            map(tag(&[2u8]), |_| ValueType::SetEncoding),
        )),
    )(input)
}

fn parse_string(input: &[u8]) -> IResult<&[u8], String> {
    let (input, key_length) = (parse_rdb_length)(input)?;
    let (input, key) = take(key_length)(input)?;
    info!("Value: {:?}", std::str::from_utf8(key));
    Ok((
        input,
        std::str::from_utf8(key)
            .expect("Key [u8] to str conversion failed")
            .to_string(),
    ))
}

fn parse_rdb_value_with_expiry(input: &[u8]) -> IResult<&[u8], Rdb> {
    let (input, (expiry_time, value_type, key, value)) = tuple((
        // opt: The opt combinator is used to make the parsing of the optional.
        // If these options are not present in the input string, opt will return None.
        // alt: The alt combinator is used to try multiple parsers in order until one succeeds.
        //
        opt(alt((
            // value: The value combinator is used to map the result of a parser to a specific value.
            //
            value(4usize, tag([0xFD])),
            value(8usize, tag([0xFC])),
        ))),
        parse_value_type,
        parse_string,
        parse_string,
    ))(input)?;

    Ok((
        input,
        Rdb::KeyValuePair {
            key_expiry_time: expiry_time,
            value_type,
            key,
            value,
        },
    ))
}

fn parse_resize_db(input: &[u8]) -> IResult<&[u8], Rdb> {
    // 0xFB means resize db
    // It encodes two values to speed up RDB loading by avoiding additional resizes and rehashing.
    // The op code is followed by two length-encoded integers indicating:
    //
    // Database hash table size
    // Expiry hash table size
    // length first
    let (input, _aux_opcode) = tag([0xFB])(input)?;
    let (input, db_hash_table_length) = (parse_rdb_length)(input)?;

    // value next
    // let (input, _db_hash_table_size) = take(db_hash_table_length)(input)?;

    let (input, expiry_hash_table_length) = (parse_rdb_length)(input)?;

    // value next
    // let (input, _expiry_hash_table_size) = take(expiry_hash_table_length)(input)?;

    Ok((
        input,
        Rdb::OpCode {
            opcode: RdbOpCode::ResizeDb {
                db_hash_table_length,
                expiry_hash_table_length,
            },
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
        parse_rdb_value_with_expiry,
        parse_resize_db,
    ))(input)
}
