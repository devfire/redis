use std::str::FromStr;

use log::info;

use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take, take_until},
    character::complete::{crlf, not_line_ending},
    combinator::{map, opt},
    multi::many1,
    sequence::{preceded, terminated, tuple},
    IResult,
};
use resp::Value;

use crate::{
    errors::RedisError,
    protocol::{RedisCommand, SetCommandParameters},
};

/// Goes through the array one element at a time.
/// If it detects a matching command, attempts to assemble the command with its proper parameters.
// pub fn parse_command(
//     resp_encoded_string: &str,
// ) -> Result<Option<RedisCommand>, RedisError> {

// }

fn length(input: &str) -> IResult<&str, usize> {
    let (input, len) = terminated(not_line_ending, crlf)(input)?;
    Ok((
        input,
        len.parse().expect("Length str to usize conversion failed."),
    ))
}

fn parse_resp_string(input: &str) -> IResult<&str, String> {
    let (input, len) = length(input)?;
    if len == 0 {
        return Ok((input, "".to_string()));
    }
    let (input, val) = terminated(take(len), crlf)(input)?;

    Ok((input, val.to_string()))
}

fn parse_echo(input: &str) -> IResult<&str, RedisCommand> {
    let (input, _) = tag("*")(input)?;
    let (input, _len) = (length)(input)?; // length eats crlf
    let (input, _) = tag_no_case("$4\r\nECHO\r\n$")(input)?;
    let (input, _echo_length) = (length)(input)?;
    let (input, echo_string) = terminated(not_line_ending, crlf)(input)?;

    Ok((input, RedisCommand::Echo(echo_string.to_string())))
}

fn parse_set(input: &str) -> IResult<&str, RedisCommand> {
    // test string: *3\r\n$3\r\nset\r\n$5\r\nhello\r\n$7\r\noranges\r\n
    let (input, _) = tag("*")(input)?;
    let (input, _len) = (length)(input)?; // length eats crlf
    let (input, _) = tag_no_case("$3\r\nSET\r\n$")(input)?;

    // get the key first
    let (input, _key_length) = (length)(input)?;
    let (input, key) = terminated(not_line_ending, crlf)(input)?;

    // let's get the value next
    let (input, _) = tag("$")(input)?;
    let (input, _value_length) = (length)(input)?;
    let (input, value) = terminated(not_line_ending, crlf)(input)?;

    // everything from here on is optional
    // let (input, _) = opt(tag("$"))(input)?;
    let set_params = SetCommandParameters {
        key: key.to_string(),
        value: value.to_string(),
        option: None,
        expire: None,
    };
    Ok((input, RedisCommand::Set(set_params)))
}

pub fn parse_command(input: &str) -> IResult<&str, RedisCommand> {
    alt((
        map(tag_no_case("*1\r\n$4\r\nPING\r\n"), |_| RedisCommand::Ping),
        map(tag_no_case("*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n"), |_| {
            RedisCommand::Command
        }),
        parse_echo,
        parse_set,
    ))(input)
}
