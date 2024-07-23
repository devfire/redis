use super::value::RespValue;

use nom::{
    branch::alt,
    bytes::{
        complete::{tag, tag_no_case},
        streaming::{take, take_while},
    },
    character::{complete::crlf, streaming::digit1},
    combinator::{map, map_res},
    multi::count,
    sequence::{preceded, terminated},
    IResult,
};
use tracing::info;

// strings are encoded as a plus (+) character, followed by a string.
fn parse_simple_string(input: &[u8]) -> IResult<&[u8], RespValue> {
    info!("Parsing simple string: {:?}", input);
    map(
        terminated(preceded(tag("+"), take_while(|c| c != b'\r')), crlf),
        |s: &[u8]| RespValue::SimpleString(String::from_utf8_lossy(s).to_string()),
    )(input)
}

// integers are encoded as a colon (:) character, followed by a number.
fn parse_integer(input: &[u8]) -> IResult<&[u8], RespValue> {
    info!("Parsing integer: {:?}", input);
    map(
        terminated(
            preceded(
                tag(":"),
                map_res(take_while(|c: u8| c.is_ascii_digit()), |s| {
                    String::from_utf8_lossy(s).parse::<i64>()
                }),
            ),
            crlf,
        ),
        RespValue::Integer,
    )(input)
}

// errors are encoded as a minus (-) character, followed by an error message.
fn parse_error(input: &[u8]) -> IResult<&[u8], RespValue> {
    info!("Parsing error: {:?}", input);
    map(
        terminated(preceded(tag("-"), take_while(|c| c != b'\r')), crlf),
        |s: &[u8]| RespValue::Error(String::from_utf8_lossy(s).to_string()),
    )(input)
}

// bulk strings are encoded as a dollar sign ($) character,
// followed by the number of bytes in the string, followed by CRLF,
// followed by the string itself.
fn parse_bulk_string(input: &[u8]) -> IResult<&[u8], RespValue> {
    info!("Parsing bulk string: {:?}", input);
    let (input, length) = preceded(
        tag("$"),
        map_res(take_while(|c: u8| c.is_ascii_digit()), |s| {
            String::from_utf8_lossy(s).parse::<i64>()
        }),
    )(input)?;
    let (input, _) = crlf(input)?;

    if length == -1 {
        Ok((input, RespValue::BulkString(None)))
    } else {
        let (input, data) = take(length as usize)(input)?;
        let (input, _) = crlf(input)?;
        Ok((input, RespValue::BulkString(Some(data.to_vec()))))
    }
}

fn parse_array(input: &[u8]) -> IResult<&[u8], RespValue> {
    let (input, array_size) = preceded(
        tag("*"),
        map_res(digit1, |s: &[u8]| {
            std::str::from_utf8(s).unwrap().parse::<i64>()
        }),
    )(input)?;
    let (input, _) = crlf(input)?;

    if array_size < 0 {
        Ok((input, RespValue::NullArray))
    } else {
        let (input, elements) = count(parse_resp, array_size as usize)(input)?;
        Ok((input, RespValue::Array(elements)))
    }
}

// parse in-bound RDB file in memory representation. This follows FULLRESYNC redis command.
// The file is sent using the following format:
// $<length_of_file>\r\n<contents_of_file>
// (This is similar to how Bulk Strings are encoded, but without the trailing \r\n)
fn parse_rdb(input: &[u8]) -> IResult<&[u8], RespValue> {
    let (input, length) = preceded(
        tag("$"),
        map_res(take_while(|c: u8| c.is_ascii_digit()), |s| {
            String::from_utf8_lossy(s).parse::<i64>()
        }),
    )(input)?;
    let (input, _) = crlf(input)?;

    let (input, data) = take(length as usize)(input)?;
    Ok((input, RespValue::Rdb(data.to_vec())))
}

pub fn parse_resp(input: &[u8]) -> IResult<&[u8], RespValue> {
    info!("Parsing resp: {:?}", input);
    alt((
        map(tag_no_case("$-1\r\n"), |_| RespValue::Null),
        map(tag_no_case("*-1\r\n"), |_| RespValue::NullArray),
        parse_simple_string,
        parse_error,
        parse_integer,
        parse_bulk_string,
        parse_array,
        parse_rdb,
    ))(input)
}
