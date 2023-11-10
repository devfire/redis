// borrowed from https://raw.githubusercontent.com/pawelkobojek/respirator/main/src/parser.rs
use nom::{
    bytes::complete::take,
    character::complete::{crlf, not_line_ending},
    multi::count,
    sequence::terminated,
    IResult,
};

use crate::protocol::Resp;

pub fn resp(input: &[u8]) -> IResult<&[u8], Resp> {
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

fn simple_string(input: &[u8]) -> IResult<&[u8], Resp> {
    let (input, val) = terminated(not_line_ending, crlf)(input)?;
    Ok((input, Resp::SimpleString(val.to_vec())))
}

fn integer(input: &[u8]) -> IResult<&[u8], Resp> {
    let (input, val) = terminated(not_line_ending, crlf)(input)?;
    Ok((
        input,
        Resp::Integer(String::from_utf8_lossy(val).parse::<i64>().unwrap()),
    ))
}

fn error(input: &[u8]) -> IResult<&[u8], Resp> {
    let (input, val) = terminated(not_line_ending, crlf)(input)?;
    Ok((input, Resp::Error(val.to_vec())))
}

fn bulk_string(input: &[u8]) -> IResult<&[u8], Resp> {
    let (input, len) = length(input)?;
    if len == 0 {
        return Ok((input, Resp::BulkString(None)));
    }
    let (input, val) = terminated(take(len), crlf)(input)?;

    Ok((input, Resp::BulkString(Some(val.to_vec()))))
}

fn length(input: &[u8]) -> IResult<&[u8], usize> {
    let (input, len) = terminated(not_line_ending, crlf)(input)?;
    Ok((input, String::from_utf8_lossy(len).parse().unwrap()))
}

fn array(input: &[u8]) -> IResult<&[u8], Resp> {
    let (input, len) = length(input)?;
    if len == 0 {
        return Ok((input, Resp::Array(None)));
    }
    let (input, res) = count(resp, len)(input)?;
    Ok((input, Resp::Array(Some(res))))
}
