use log::info;
use nom::{
    branch::alt,
    bytes::{complete::take_while, streaming::tag},
    character::streaming::{crlf, digit1, not_line_ending},
    sequence::{delimited, preceded, terminated},
    IResult,
};

use crate::protocol::Command;

fn length(input: &[u8]) -> IResult<&[u8], usize> {
    let (input, len) = terminated(not_line_ending, crlf)(input)?;
    Ok((input, String::from_utf8_lossy(len).parse().unwrap()))
}

fn parse_ping(input: &[u8]) -> IResult<&[u8], Command> {
    let (input, _) = tag(r"\$4\r\nPING\r\n")(input)?;
    Ok((input, Command::Ping))
}

// fn parse_bulk_string(input: &str) -> IResult<&str, &str> {
//     preceded(
//         delimited(tag("$"), length, tag("\r\n")),
//         take_while(|c: char| c != '\r'),
//     )(input)
// }

// fn parse_echo(input: &[u8]) -> IResult<&[u8], Command> {
//     let (input, _) = tag(r"\$4\r\nECHO\r\n")(input)?;
//     let (input, _) = tag(r"\$")(input)?;
//     let (input, len) =
//     Ok((input, Command::Ping))
// }

fn parse_array(input: &[u8]) -> IResult<&[u8], Command> {
    let (input, _) = tag(r"*")(input)?;
    let (input, _len) = length(input)?;
    Ok((input, Command::Unknown))
}

fn parse_unsupported(input: &[u8]) -> IResult<&[u8], Command> {
    let (input, _) = tag(r"$")(input)?;
    let (input, _len) = length(input)?;
    let (input, _unsupported_command) = terminated(not_line_ending, crlf)(input)?;
    Ok((input, Command::Unknown))
}

pub fn parse_command(input: &[u8]) -> IResult<&[u8], Command> {
    let (input, message) = alt((parse_array, parse_ping, parse_unsupported))(input)?;
    info!("Parser finished, inbound message: {:?}", message);
    Ok((input, message))
}
