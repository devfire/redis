// borrowed from https://raw.githubusercontent.com/pawelkobojek/respirator/main/src/parser.rs
use nom::{
    bytes::complete::take,
    character::complete::{crlf, not_line_ending},
    multi::count,
    sequence::terminated,
    IResult,
};

use crate::protocol::RespFrame;

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
