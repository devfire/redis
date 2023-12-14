use log::info;
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{crlf, not_line_ending},
    combinator::{map, opt},
    sequence::{terminated, tuple},
    IResult,
};

use crate::protocol::{RedisCommand, SetCommandParameters};

fn length(input: &str) -> IResult<&str, usize> {
    let (input, len) = terminated(not_line_ending, crlf)(input)?;
    Ok((
        input,
        len.parse().expect("Length str to usize conversion failed."),
    ))
}

// RESP bulk string format: $<length>\r\n<data>\r\n
fn parse_resp_string(input: &str) -> IResult<&str, String> {
    let (input, _) = tag("$")(input)?;
    let (input, _len) = length(input)?;
    // if len == 0 {
    //     return Ok((input, "".to_string()));
    // }
    let (input, value) = terminated(not_line_ending, crlf)(input)?;

    Ok((input, value.to_string()))
}

fn parse_echo(input: &str) -> IResult<&str, RedisCommand> {
    let (input, _) = tag("*")(input)?;
    let (input, _len) = (length)(input)?; // length eats crlf
    let (input, _) = tag_no_case("$4\r\nECHO\r\n")(input)?;
    // let (input, _echo_length) = (length)(input)?;
    let (input, echo_string) = (parse_resp_string)(input)?;

    Ok((input, RedisCommand::Echo(echo_string.to_string())))
}

fn parse_set(input: &str) -> IResult<&str, RedisCommand> {
    // test string: *3\r\n$3\r\nset\r\n$5\r\nhello\r\n$7\r\noranges\r\n
    let (input, _) = tag("*")(input)?;
    let (input, _len) = (length)(input)?; // length eats crlf
    let (input, _) = tag_no_case("$3\r\nSET\r\n")(input)?;

    let (input, (key, value, option, get, expire)) = tuple((
        parse_resp_string,
        parse_resp_string,
        // opt(alt((map(tag("NX"), |_| "NX"), map(tag("NX"), |_| "NX")))),
        opt(map(
            alt((tag_no_case("NX"), tag_no_case("XX"))),
            |s: &str| s.to_string(),
        )),
        opt(map(tag_no_case("GET"), |_| "GET".to_string())),
        opt(parse_resp_string),
    ))(input)?;
    
    let set_params = SetCommandParameters {
        key,
        value,
        option,
        get,
        expire,
    };
    info!("Parsed SET: {:?}", set_params);

    Ok((input, RedisCommand::Set(set_params)))
}

fn parse_get(input: &str) -> IResult<&str, RedisCommand> {
    let (input, _) = tag("*")(input)?;
    let (input, _len) = (length)(input)?; // length eats crlf
    let (input, _) = tag_no_case("$3\r\nGET\r\n")(input)?;
    // let (input, _echo_length) = (length)(input)?;
    let (input, key) = (parse_resp_string)(input)?;

    Ok((input, RedisCommand::Get(key.to_string())))
}

pub fn parse_command(input: &str) -> IResult<&str, RedisCommand> {
    alt((
        map(tag_no_case("*1\r\n$4\r\nPING\r\n"), |_| RedisCommand::Ping),
        map(tag_no_case("*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n"), |_| {
            RedisCommand::Command
        }),
        parse_echo,
        parse_set,
        parse_get,
    ))(input)
}
