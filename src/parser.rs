use nom::{
    branch::alt,
    bytes::complete::{tag, take_while},
    character::complete::{digit1, newline},
    combinator::{map, map_res},
    multi::separated_list0,
    // multi::many1,
    sequence::{delimited, preceded, tuple},
    IResult,
};

use crate::protocol::Command;

// Define the parsers
fn parse_number(input: &str) -> IResult<&str, u32> {
    map_res(digit1, |digit_str: &str| digit_str.parse::<u32>())(input)
}

fn parse_bulk_string(input: &str) -> IResult<&str, &str> {
    preceded(
        delimited(tag("$"), parse_number, tag("\r\n")),
        take_while(|c: char| c != '\r'),
    )(input)
}

fn parse_ping_command(input: &str) -> IResult<&str, Command> {
    map(tuple((tag(r"\$4\r\nPING\r\n"), newline)), |_message| {
        Command::Ping
    })(input)
}

fn parse_echo_command(input: &str) -> IResult<&str, Command> {
    map(
        tuple((tag(r"\$4\r\nECHO\r\n"), parse_bulk_string, newline)),
        |(_command, message, _)| Command::Echo(message.to_string()),
    )(input)
}

pub fn parse_commands(input: &str) -> IResult<&str, Vec<Command>> {
    separated_list0(newline, alt((parse_ping_command, parse_echo_command)))(input)
}

// Test the parser
#[test]
fn test_parser() {
    let result = parse_commands(r"\$4\r\nPING\r\n\$4\r\nECHO\r\nHello\r\n\$4\r\nPING\r\n");
    assert_eq!(
        result,
        Ok((
            "",
            vec![
                Command::Ping,
                Command::Echo("Hello".to_string()),
                Command::Ping
            ],
        ))
    );
}
