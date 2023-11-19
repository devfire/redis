use log::info;
use nom::{
    branch::alt,
    bytes::{
        streaming::tag,
    },
    character::streaming::{crlf, not_line_ending},
    sequence::{terminated},
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
//         delimited(tag("$"), parse_number, tag("\r\n")),
//         take_while(|c: char| c != '\r'),
//     )(input)
// }

// fn parse_number(input: &str) -> IResult<&str, u32> {
//     map_res(digit1, |digit_str: &str| digit_str.parse::<u32>())(input)
// }

fn parse_echo(input: &[u8]) -> IResult<&[u8], Command> {
    let (input, _) = tag(r"\$4\r\nECHO\r\n\$")(input)?;
    let (input, _len) = length(input)?;
    let (input, message) = terminated(not_line_ending, crlf)(input)?;
    Ok((
        input,
        Command::Echo(
            String::from_utf8_lossy(message)
                .parse()
                .expect("Unable to convert message [u8] to String"),
        ),
    ))
}

fn parse_array(input: &[u8]) -> IResult<&[u8], Command> {
    let (input, _) = tag(r"*")(input)?;
    let (input, _len) = length(input)?;
    Ok((input, Command::Unknown))
}

// fn parse_unsupported(input: &[u8]) -> IResult<&[u8], Command> {
//     let (input, _) = tag(r"$")(input)?;
//     let (input, _len) = length(input)?;
//     let (input, _unsupported_command) = terminated(not_line_ending, crlf)(input)?;
//     Ok((input, Command::Unknown))
// }

pub fn parse_command(input: &[u8]) -> IResult<&[u8], Command> {
    let (input, message) = alt((parse_array, parse_ping, parse_echo))(input)?;
    info!("Parser finished, inbound message: {:?}", message);
    Ok((input, message))
}
