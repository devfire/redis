use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{crlf, not_line_ending},
    combinator::{map, opt},
    sequence::terminated,
    IResult,
};

use crate::protocol::{RedisCommand, SetCommandParameters, SetCommandSetOption};

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

    // get the key first
    let (input, key) = (parse_resp_string)(input)?;

    // let's get the value next
    let (input, value) = (parse_resp_string)(input)?;
    // let (input, value) = terminated(not_line_ending, crlf)(input)?;

    let mut set_params = SetCommandParameters {
        key: key.to_string(),
        value: value.to_string(),
        option: None,
        get: None,
        expire: None,
    };

    // everything from here on is optional
    let (input, set_options) = opt(parse_resp_string)(input)?;

    if let Some(option) = set_options {
        match option.to_uppercase().as_str() {
            "NX" => {
                set_params.option = Some(SetCommandSetOption::NX);
            }
            "XX" => {
                set_params.option = Some(SetCommandSetOption::XX);
            }
            _ => {}
        }
    }

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
