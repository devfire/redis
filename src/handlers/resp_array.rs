use std::str::FromStr;

use log::info;

use nom::{
    branch::alt,
    bytes::complete::{tag, take, take_until},
    character::complete::{crlf, not_line_ending},
    combinator::{map, opt},
    multi::many1,
    sequence::{preceded, terminated, tuple},
    IResult,
};
use resp::Value;

use crate::{errors::RedisError, protocol::RedisCommand};

/// Goes through the array one element at a time.
/// If it detects a matching command, attempts to assemble the command with its proper parameters.
// pub fn parse_command(
//     resp_encoded_string: &str,
// ) -> Result<Option<RedisCommand>, RedisError> {

// }

fn length(input: &[u8]) -> IResult<&[u8], usize> {
    let (input, len) = terminated(not_line_ending, crlf)(input)?;
    Ok((input, String::from_utf8_lossy(len).parse().unwrap()))
}

fn parse_resp_string(input: &[u8]) -> IResult<&[u8], String> {
    let (input, len) = length(input)?;
    if len == 0 {
        return Ok((input, "".to_string()));
    }
    let (input, val) = terminated(take(len), crlf)(input)?;

    Ok((
        input,
        String::from_utf8(val.to_vec()).expect("Unable to convert [u8] to String"),
    ))
}




pub fn parse_command(input: &str) -> IResult<&str, RedisCommand> {
    alt((
        map(tag("*1\r\n$4\r\nPING\r\n"), |_| RedisCommand::Ping),
        map(tag("*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n"), |_| RedisCommand::Command),
        // map(tag("*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n"), |_| {
        //     RedisCommand::Echo("hello".to_string())
        // }),
    ))(input)
}