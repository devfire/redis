use log::info;
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{crlf, digit1, not_line_ending},
    combinator::{cut, map, map_opt, opt, value},
    error::{Error, ErrorKind},
    sequence::{terminated, tuple},
    IResult, Parser,
};

use anyhow::anyhow;

use crate::{
    errors::RedisError,
    protocol::{RedisCommand, SetCommandExpireOption, SetCommandParameters, SetCommandSetOption},
};

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

    let (input, value) = terminated(not_line_ending, crlf)(input)?;

    Ok((input, value.to_string()))
}

// fn parse_expire_time(input: &str) -> Result<usize, RedisError> {
//     // let (input, time_as_str) =
//     //     digit1(input).map_err(|_| anyhow!("Failed to parse expire time"))?;

//     let time = input
//         .lines()
//         .map(|time| time.parse::<usize>())
//         .filter_map(|r| r.map_err(|e| e).ok());
//     // input.lines().map(|time| Ok(time?.parse::<usize>()?));
//     // // let time = time_as_s = tr
//     //     .parse::<usize>()
//     //     .map_err(|_| anyhow!("Failed to parse expire time"))?;

//     Ok((input,time))
// }

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

    // tuple: The tuple combinator is used to apply a tuple of parsers one by one and return their results as a tuple.
    // In this case, it's used to parse two strings followed by an optional option, a GET flag, and an expiration option
    //
    // opt: The opt combinator is used to make the parsing of the option, GET flag, and expiration option optional.
    // If these options are not present in the input string, opt will return None.
    //
    // alt: The alt combinator is used to try multiple parsers in order until one succeeds.
    // In this case, it's used to parse either the "NX" or "XX" option.
    //
    // map: The map combinator is used to transform the output of the parser.
    // In this case, it's used to map the result of the tag_no_case combinator to true for the GET flag,
    // and to parse the seconds or milliseconds for the expiration option.
    //
    // tag_no_case: The tag_no_case combinator is used to match a case-insensitive string.
    // In this case, it's used to match the strings "$2\r\nNX\r\n", "$2\r\nXX\r\n", "$3\r\nGET\r\n", "$2\r\nEX\r\n", and "$2\r\nPX\r\n",
    // each one a potential expiration option in redis SET command.
    //
    // value: The value combinator is used to map the result of a parser to a specific value.
    // In this case, it's used to map the result of the tag_no_case combinator to SetCommandSetOption::NX or
    // SetCommandSetOption::XX for the option.
    //
    // parse_resp_string: This parser is used to parse a RESP string.
    //
    // Summary: This parser returns a tuple containing the parsed key, value, option, GET flag, and expiration option.
    // If the option, GET flag, or expiration option are not present in the input string, they will be None.
    let (input, (key, value, option, get, expire)) = tuple((
        parse_resp_string, // key
        parse_resp_string, // value
        opt(alt((
            // either NX or XX
            value(SetCommandSetOption::NX, tag_no_case("$2\r\nNX\r\n")),
            value(SetCommandSetOption::XX, tag_no_case("$2\r\nXX\r\n")),
        ))),
        // GET: Return the old string stored at key, or nil if key did not exist.
        opt(map(tag_no_case("$3\r\nGET\r\n"), |_| true)),
        // These maps all handle the various expiration options.
        opt(alt((
            cut(map_opt(
                tuple((tag_no_case("$2\r\nEX\r\n"), parse_resp_string)),
                |(_expire_option, seconds)| {
                    // seconds
                    //     .parse::<usize>()
                    //     .ok()
                    //     .map(SetCommandExpireOption::EX)
                    // buf.lines().map(|l| Ok(l?.parse()?)).collect()

                    // seconds.parse::<usize>().map(|seconds| Ok(seconds?.parse))
                    seconds
                        .parse::<usize>()
                        .ok()
                        .map(|seconds| SetCommandExpireOption::EX(seconds))
                },
            )),
            map_opt(
                tuple((tag_no_case("$2\r\nPX\r\n"), parse_resp_string)),
                |(_expire_option, milliseconds)| {
                    milliseconds
                        .parse::<usize>()
                        .ok()
                        .map(SetCommandExpireOption::PX)

                    // seconds
                    //     .parse::<usize>().into_iter()
                    //     .filter_map(|seconds: usize|SetCommandExpireOption::EX(seconds))
                    // .map(|seconds| SetCommandExpireOption::EX(seconds))
                    // .or_else(|_| Err(RedisError::ParseIntError))
                },
            ),
        ))),
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
