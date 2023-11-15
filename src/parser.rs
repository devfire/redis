// borrowed from https://raw.githubusercontent.com/pawelkobojek/respirator/main/src/parser.rs
use nom::{
    branch::alt,
    bytes::complete::{tag, take, take_while},
    character::complete::{crlf, multispace0, not_line_ending},
    combinator::map,
    multi::{count, many0},
    sequence::{delimited, preceded, terminated, tuple},
    IResult,
};

use crate::protocol::{Command, RespFrame};

fn parse_ping(input: &str) -> IResult<&str, Command> {
    map(tag("PING"), |_| Command::Ping)(input)
}

// fn parse_ping(input: RespFrame) -> IResult<RespFrame, Command> {
//     map(tag("PING"), |_| Command::Ping)(input)
// }

fn parse_echo(input: &str) -> IResult<&str, Command> {
    map(
        preceded(
            tag("ECHO"),
            take_while(|c: char| c.is_whitespace()),
        ),
        |s: &str| Command::Echo(s.to_string()),
    )(input)
}

fn parse_command(input: &str) -> IResult<&str, Command> {
    alt((parse_ping, parse_echo))(input)
}

fn parse_vec_of_enums(input: &str) -> IResult<&str, Vec<Command >> {
    delimited(
        tag("["),
        preceded(multispace0, many0(preceded(multispace0, parse_command))),
        preceded(multispace0, tag("]")),
    )(input)
}

pub fn parse_resp(input: &[u8]) -> IResult<&[u8], RespFrame> {
    let (input, val) = take(1usize)(input)?;
    match val[0] {
        b'+' => simple_string(input),
        b':' => integer(input),
        b'-' => error(input),
        b'$' => bulk_string(input),
        b'*' => array(input),
        _ => panic!("Unknown type byte: {:?}", val),
    }
}

fn simple_string(input: &[u8]) -> IResult<&[u8], RespFrame> {
    let (input, val) = terminated(not_line_ending, crlf)(input)?;
    Ok((input, RespFrame::SimpleString(val.to_vec())))
}

fn integer(input: &[u8]) -> IResult<&[u8], RespFrame> {
    let (input, val) = terminated(not_line_ending, crlf)(input)?;
    Ok((
        input,
        RespFrame::Integer(String::from_utf8_lossy(val).parse::<i64>().unwrap()),
    ))
}

fn error(input: &[u8]) -> IResult<&[u8], RespFrame> {
    let (input, val) = terminated(not_line_ending, crlf)(input)?;
    Ok((input, RespFrame::Error(val.to_vec())))
}

fn bulk_string(input: &[u8]) -> IResult<&[u8], RespFrame> {
    let (input, len) = length(input)?;
    if len == 0 {
        return Ok((input, RespFrame::BulkString(None)));
    }
    let (input, val) = terminated(take(len), crlf)(input)?;

    Ok((input, RespFrame::BulkString(Some(val.to_vec()))))
}

fn length(input: &[u8]) -> IResult<&[u8], usize> {
    let (input, len) = terminated(not_line_ending, crlf)(input)?;
    Ok((input, String::from_utf8_lossy(len).parse().unwrap()))
}

fn array(input: &[u8]) -> IResult<&[u8], RespFrame> {
    let (input, len) = length(input)?;
    if len == 0 {
        return Ok((input, RespFrame::Array(None)));
    }
    let (input, res) = count(parse_resp, len)(input)?;
    Ok((input, RespFrame::Array(Some(res))))
}
