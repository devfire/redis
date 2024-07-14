use log::{debug, error};
use nom::{
    branch::alt,
    bytes::{complete::tag, streaming::take},
    combinator::value,
    number::streaming::{le_u16, le_u32, le_u64, le_u8},
    sequence::tuple,
    IResult,
};

use crate::protocol::SetCommandExpireOption;

use super::format::{Rdb, RdbOpCode, ValueType};

fn parse_rdb_header(input: &[u8]) -> IResult<&[u8], Rdb> {
    let (input, _magic) = tag("REDIS")(input)?;
    let (input, version) = take(4usize)(input)?;
    let version = String::from_utf8_lossy(version).to_string();

    debug!("RDB header version {} detected.", version);
    Ok((
        input,
        Rdb::RdbHeader {
            magic: "REDIS".to_string(),
            version,
        },
    ))
}

// https://rdb.fnordig.de/file_format.html#op-codes
fn parse_eof(input: &[u8]) -> IResult<&[u8], Rdb> {
    let (input, _eof_marker) = tag([0xFF])(input)?;
    let (input, checksum) = le_u64(input)?;
    // let (input, checksum) = map_opt(take(8usize), |bytes: &[u8]| {
    //     bytes.try_into().ok().map(u32::from_le_bytes)
    // })(input)?;
    // let checksum = String::from_utf8_lossy(checksum).to_string();

    debug!("EOF detected.");

    Ok((
        input,
        Rdb::OpCode {
            opcode: RdbOpCode::Eof(checksum),
        },
    ))
}

// A Redis instance can have multiple databases.
// A single byte 0xFE flags the start of the database selector.
// After this byte, a variable length field indicates the database number.
fn parse_selectdb(input: &[u8]) -> IResult<&[u8], Rdb> {
    let (input, _dbselector) = tag([0xFE])(input)?;
    let (input, value_type) = (parse_string_length)(input)?;

    let (input, _db_number) = (take(value_type.get_length()))(input)?;

    debug!("SELECTDB OpCode detected.");
    Ok((
        input,
        Rdb::OpCode {
            opcode: RdbOpCode::Selectdb,
        },
    ))
}

fn parse_string_length(input: &[u8]) -> IResult<&[u8], ValueType> {
    let (input, first_byte) = le_u8(input)?;
    let two_most_significant_bits = first_byte >> 6;
    debug!(
        "First byte: {:08b} two most significant bits: {:02b}",
        first_byte, two_most_significant_bits
    );

    let (input, length) = match two_most_significant_bits {
        0b00 => {
            // 00: The next 6 bits represent the length
            let length = (first_byte & 0b0011_1111) as u32;
            let value_type = ValueType::LengthEncoding {
                length,
                special: false,
            };
            debug!("Value type: {:?}", value_type);
            (input, value_type)
        }
        0b01 => {
            // 01: Read one additional byte. The combined 14 bits represent the length
            let (input, next_byte) = le_u8(input)?;
            let length = (((first_byte & 0b0011_1111) as u32) << 8) | next_byte as u32;
            let value_type = ValueType::LengthEncoding {
                length,
                special: false,
            };
            debug!("Value type: {:?}", value_type);
            (input, value_type)
        }
        0b10 => {
            // 10: Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
            let (input, length) = le_u32(input)?;
            let length = length as u32;
            let value_type = ValueType::LengthEncoding {
                length,
                special: false,
            };
            debug!("Value type: {:?}", value_type);
            (input, value_type)
        }
        0b11 => {
            // 11: The next object is encoded in a special format. The remaining 6 bits indicate the format.
            let format = (first_byte & 0b0011_1111) as u32;
            debug!("Special format detected, type: {:b}", format);

            let value_type = ValueType::LengthEncoding {
                length: format,
                special: true,
            };
            debug!("Value type: {:?}", value_type);
            (input, value_type)
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

    Ok((input, length))
}

/// Auxiliary field. May contain arbitrary metadata such as
/// Redis version, creation time, used memory.
/// first comes the key, then the value. Both are strings.
//
fn parse_rdb_aux(input: &[u8]) -> IResult<&[u8], Rdb> {
    let (input, _aux_opcode) = tag([0xFA])(input)?;

    // taking the key first
    let (input, key) = (parse_string)(input)?;
    // let (input, key) = take(key_length)(input)?;
    debug!("Aux key detected: {}, parsing value next.", key);

    // taking the value next
    let (input, value) = (parse_string)(input)?;
    debug!("Aux key: {} value: {}", key, value);

    Ok((
        input,
        Rdb::OpCode {
            opcode: RdbOpCode::Aux,
        },
    ))
}

fn parse_value_type(input: &[u8]) -> IResult<&[u8], ValueType> {
    alt((
        // value: The value combinator is used to map the result of a parser to a specific value.
        value(ValueType::StringEncoding, tag([0x0])),
        value(ValueType::ListEncoding, tag([0x1])),
    ))(input)
}

fn parse_string(input: &[u8]) -> IResult<&[u8], String> {
    let (input, string_type) = (parse_string_length)(input)?;
    // let (input, parsed_string) = take(string_length)(input)?;

    if !string_type.is_special() {
        //not special

        let (input, parsed_string) = take(string_type.get_length())(input)?;
        debug!(
            "Parsed these bytes as string: {:?}",
            parsed_string.to_ascii_lowercase()
        );
        debug!(
            "Parsed string type: {:?} string: {}",
            string_type,
            std::str::from_utf8(parsed_string)
                .expect("Key [u8] to str conversion failed")
                .to_string(),
        );
        Ok((
            input,
            std::str::from_utf8(parsed_string)
                .expect("Key [u8] to str conversion failed")
                .to_string(),
        ))
    } else {
        // special format, most likely integers as strings
        // https://rdb.fnordig.de/file_format.html#string-encoding
        match string_type.get_length() {
            0 => {
                // 8 bit integer
                let (input, parsed_string) = (le_u8)(input)?;
                debug!(
                    "Parsed string length: {:?} parsed bytes: {:?} string: {}",
                    string_type,
                    parsed_string,
                    format!("{}", parsed_string),
                );
                Ok((input, format!("{}", parsed_string)))
            }
            1 => {
                let (input, parsed_string) = (le_u16)(input)?;
                debug!(
                    "Parsed string length: {:?} parsed bytes: {:?} string: {}",
                    string_type,
                    parsed_string,
                    format!("{}", parsed_string),
                );
                Ok((input, format!("{}", parsed_string)))
            }
            2 => {
                let (input, parsed_string) = (le_u32)(input)?;
                debug!(
                    "Parsed string length: {:?} parsed bytes: {:?} string: {}",
                    string_type,
                    parsed_string,
                    format!("{}", parsed_string),
                );
                Ok((input, format!("{}", parsed_string)))
            }
            _ => Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::LengthValue,
            ))),
        }
    }
}

fn parse_rdb_key_value_without_expiry(input: &[u8]) -> IResult<&[u8], Rdb> {
    let (input, (value_type, key, value)) =
        tuple((parse_value_type, parse_string, parse_string))(input)?;

        debug!(
        "Parsed kv pair type: {:?} key: {} value: {}",
        value_type, key, value
    );

    Ok((
        input,
        Rdb::KeyValuePair {
            key_expiry_time: None,
            value_type,
            key,
            value,
        },
    ))
}
fn parse_expire_option_px(input: &[u8]) -> IResult<&[u8], SetCommandExpireOption> {
    let (input, _) = tag([0xFC])(input)?;
    let (input, value) = le_u64(input)?;
    debug!("Expiry unix timestamp {} ms.", value);

    // let (input, value) = nom::sequence::Tuple::parse(&mut (tag([0xFC]), le_u64), input)
    //     .map(|(input, (_, val))| (input, val))?;

    Ok((input, SetCommandExpireOption::PX(value)))
}

fn parse_expire_option_ex(input: &[u8]) -> IResult<&[u8], SetCommandExpireOption> {
    let (input, _) = tag([0xFD])(input)?;
    let (input, value) = le_u32(input)?;
    debug!("Expiry Unix timestamp {} secs.", value);
    Ok((input, SetCommandExpireOption::EX(value)))
}

fn parse_rdb_value_with_expiry(input: &[u8]) -> IResult<&[u8], Rdb> {
    let (input, (expiry_time, value_type, key, value)) = tuple((
        // opt: The opt combinator is used to make the parsing of the optional.
        // If these options are not present in the input string, opt will return None.
        // alt: The alt combinator is used to try multiple parsers in order until one succeeds.
        //
        // alt((
        //     // value: The value combinator is used to map the result of a parser to a specific value.
        //     //
        //     // value(4usize, tag([0xFD])),
        //     // value(8usize, tag([0xFC])),
        //     value(SetCommandExpireOption::EX(4usize), tag([0xFD])),
        //     value(SetCommandExpireOption::PX(8usize), tag([0xFC])),
        // )),
        alt((parse_expire_option_px, parse_expire_option_ex)),
        parse_value_type,
        parse_string,
        parse_string,
    ))(input)?;

    let rdb_value_with_expiry = Rdb::KeyValuePair {
        key_expiry_time: Some(expiry_time),
        value_type,
        key,
        value,
    };

    debug!("Rdb value with expiry: {:?}", rdb_value_with_expiry);
    Ok((input, rdb_value_with_expiry))
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
    let (input, db_hash_table_value_type) = (parse_string_length)(input)?;
    let db_hash_table_length = db_hash_table_value_type.get_length();

    // value next
    // let (input, _db_hash_table_size) = take(db_hash_table_length)(input)?;

    let (input, expiry_hash_table_value_type) = (parse_string_length)(input)?;
    let expiry_hash_table_length = expiry_hash_table_value_type.get_length();

    // value next
    // let (input, _expiry_hash_table_size) = take(expiry_hash_table_length)(input)?;

    debug!("Resize db 0xFB detected.");

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
    debug!("Parsing: {:?}", input.to_ascii_lowercase());
    alt((
        // map(tag_no_case("*1\r\n$4\r\nPING\r\n"), |_| RedisCommand::Ping),
        parse_rdb_header,
        parse_eof,
        parse_selectdb,
        parse_rdb_aux,
        parse_rdb_key_value_without_expiry,
        parse_rdb_value_with_expiry,
        parse_resize_db,
    ))(input)
}
