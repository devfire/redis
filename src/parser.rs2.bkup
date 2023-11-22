use nom::{
    branch::alt,
    bytes::complete::{tag, take_while},
    combinator::{map, map_res},
    sequence::{delimited, preceded, tuple},
    IResult, character::complete::digit1,
};

use crate::protocol::Command;

fn parse_number(input: &str) -> IResult<&str, u32> {
    map_res(digit1, |digit_str: &str| digit_str.parse::<u32>())(input)
 }
 
fn parse_bulk_string(input: &str) -> IResult<&str, &str> {
    preceded(
        delimited(tag("$"), parse_number, tag("\r\n")),
        take_while(|c: char| c != '\r'),
    )(input)
}

fn parse_echo_command(input: &str) -> IResult<&str, &str> {
    map(
        tuple((
            delimited(tag("*2\r\n"), tag(r"\$4\r\nECHO\r\n"), tag("\r\n")),
            parse_bulk_string,
        )),
        |(_command_name, message)| message,
    )(input)
}

fn parse_command(input: &str) -> IResult<&str, Command> {
    alt((
        map(tag(r"*1\r\n\$4\r\nPING\r\n"), |_| Command::Ping),
        map(parse_echo_command, |msg| Command::Echo(msg.into())),
        // Map other commands here
    ))(input)
}
