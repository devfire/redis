use super::value::RespValue;

use nom::{
    branch::alt,
    bytes::{
        complete::{tag, tag_no_case},
        streaming::take_while1,
    },
    character::{
        complete::{crlf, not_line_ending},
        streaming::{alphanumeric1, digit1},
    },
    combinator::{map, map_res, opt, value, verify},
    multi::count,
    sequence::{preceded, terminated, tuple},
    IResult,
};
use tracing::info;

fn parse_number(input: &str) -> IResult<&str, i64> {
    map_res(digit1, |s: &str| s.parse::<i64>())(input)
}

// strings are encoded as a plus (+) character, followed by a string.
fn parse_simple_string(input: &str) -> IResult<&str, RespValue> {
    info!("Parsing string: {}", input);
    map(
        terminated(preceded(tag("+"), take_while1(|c| c != '\r')), crlf),
        |s: &str| RespValue::SimpleString(s.to_string()),
    )(input)
}

// integers are encoded as a colon (:) character, followed by a number.
fn parse_integer(input: &str) -> IResult<&str, RespValue> {
    info!("Parsing integer: {}", input);
    let (input, _) = tag(":")(input)?;
    let (input, num) = alphanumeric1(input)?;
    let (input, _) = crlf(input)?;
    let num = num.parse::<i64>().unwrap();
    Ok((input, RespValue::Integer(num)))
}

// errors are encoded as a minus (-) character, followed by an error message.
fn parse_error(input: &str) -> IResult<&str, RespValue> {
    info!("Parsing error: {}", input);
    map(
        terminated(preceded(tag("-"), take_while1(|c| c != '\r')), crlf),
        |s: &str| RespValue::Error(s.to_string()),
    )(input)
}

// bulk strings are encoded as a dollar sign ($) character,
// followed by the number of bytes in the string, followed by CRLF,
// followed by the string itself.
fn parse_bulk_string(input: &str) -> IResult<&str, RespValue> {
    info!("Parsing bulk string: {}", input);
    let (input, length) = preceded(tag("$"), parse_number)(input)?;
    let (input, _) = crlf(input)?;

    if length == -1 {
        Ok((input, RespValue::BulkString(None)))
    } else {
        let (input, data) = take_while1(|c| c != '\r')(input)?;
        let (input, _) = crlf(input)?;
        Ok((input, RespValue::BulkString(Some(data.to_string()))))
    }
}

fn parse_array(input: &str) -> IResult<&str, RespValue> {
    let (input, array_size) = preceded(tag("*"), parse_number)(input)?;
    let (input, _) = crlf(input)?;

    if array_size < 0 {
        Ok((input, RespValue::Array(vec![])))
    } else {
        let (input, elements) = count(parse_resp, array_size as usize)(input)?;
        Ok((input, RespValue::Array(elements)))
    }
}

pub fn parse_resp(input: &str) -> IResult<&str, RespValue> {
    info!("Parsing resp: {}", input);
    alt((
        map(tag_no_case("$-1\r\n"), |_| RespValue::Null),
        map(tag_no_case("*-1\r\n"), |_| RespValue::NullArray),
        parse_simple_string,
        parse_error,
        parse_integer,
        parse_bulk_string,
        parse_array,
    ))(input)
}
