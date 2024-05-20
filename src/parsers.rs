use std::{
    time::{SystemTime, UNIX_EPOCH},
    usize,
};

use log::info;
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{crlf, not_line_ending},
    combinator::{map, opt, value},
    sequence::{terminated, tuple},
    IResult,
};

use crate::protocol::{
    ConfigCommandParameter, ExpiryOption, InfoCommandParameter, RedisCommand, SetCommandExpireOption,
    SetCommandParameter, SetCommandSetOption,
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

fn parse_echo(input: &str) -> IResult<&str, RedisCommand> {
    let (input, _) = tag("*")(input)?;
    let (input, _len) = (length)(input)?; // length eats crlf
    let (input, _) = tag_no_case("$4\r\nECHO\r\n")(input)?;
    // let (input, _echo_length) = (length)(input)?;
    let (input, echo_string) = (parse_resp_string)(input)?;

    Ok((input, RedisCommand::Echo(echo_string.to_string())))
}

/// https://redis.io/commands/strlen/
/// STRLEN key
fn parse_strlen(input: &str) -> IResult<&str, RedisCommand> {
    let (input, _) = tag("*")(input)?;
    let (input, _len) = (length)(input)?; // length eats crlf
    let (input, _) = tag_no_case("$6\r\nSTRLEN\r\n")(input)?;
    // let (input, _echo_length) = (length)(input)?;
    let (input, key_string) = (parse_resp_string)(input)?;

    Ok((input, RedisCommand::Strlen(key_string.to_string())))
}

fn parse_append(input: &str) -> IResult<&str, RedisCommand> {
    let (input, _) = tag("*")(input)?;
    let (input, _len) = (length)(input)?; // length eats crlf
    let (input, _) = tag_no_case("$6\r\nAPPEND\r\n")(input)?;

    // let's get the key to append to, first
    let (input, key) = (parse_resp_string)(input)?;

    // now let's grab the value we are appending
    let (input, value) = (parse_resp_string)(input)?;

    Ok((
        input,
        RedisCommand::Append(key.to_string(), value.to_string()),
    ))
}

fn parse_del(input: &str) -> IResult<&str, RedisCommand> {
    let (input, _) = tag("*")(input)?;
    let (input, _len) = (length)(input)?; // length eats crlf
    let (input, _) = tag_no_case("$3\r\nDEL\r\n")(input)?;

    // many1 runs the embedded parser, gathering the results in a Vec.
    // This stops on Err::Error if there is at least one result,
    // and returns the results that were accumulated.
    let (input, keys_to_delete) = nom::multi::many1(parse_resp_string)(input)?;
    Ok((input, RedisCommand::Del(keys_to_delete)))
}

fn parse_mget(input: &str) -> IResult<&str, RedisCommand> {
    let (input, _) = tag("*")(input)?;
    let (input, _len) = (length)(input)?; // length eats crlf
    let (input, _) = tag_no_case("$4\r\nMGET\r\n")(input)?;

    // many1 runs the embedded parser, gathering the results in a Vec.
    // This stops on Err::Error if there is at least one result,
    // and returns the results that were accumulated.
    let (input, keys_to_get) = nom::multi::many1(parse_resp_string)(input)?;
    Ok((input, RedisCommand::Mget(keys_to_get)))
}

fn expiry_to_timestamp(expiry: ExpiryOption) -> u64 {
    // u64 always since u32 secs fits into u64
    // get the current system time
    let now = SystemTime::now();

    // how many seconds have elapsed since beginning of time
    let duration_since_epoch = now
        .duration_since(UNIX_EPOCH)
        .expect("Failed to calculate duration since epoch"); // Handle potential error

    // we don't want to lose precision between seconds & milliseconds
    match expiry {
        ExpiryOption::Seconds(seconds) => seconds as u64 + duration_since_epoch.as_secs(),
        ExpiryOption::Milliseconds(milliseconds) => {
            milliseconds + duration_since_epoch.as_millis() as u64 // nominally millis are u128
        }
    }
}

fn parse_set(input: &str) -> IResult<&str, RedisCommand> {
    // test string: *3\r\n$3\r\nset\r\n$5\r\nhello\r\n$7\r\noranges\r\n
    let (input, _) = tag("*")(input)?;
    let (input, _len) = (length)(input)?; // length eats crlf
    let (input, _) = tag_no_case("$3\r\nSET\r\n")(input)?;

    // Summary: This parser returns a tuple containing the parsed key, value, option, GET flag, and expiration option.
    // If the option, GET flag, or expiration option are not present in the input string, they will be None.
    // tuple: The tuple combinator is used to apply a tuple of parsers one by one and return their results as a tuple.
    // In this case, it's used to parse two strings followed by an optional option, a GET flag, and an expiration option
    //
    let (input, (key, value, option, get, expire)) = tuple((
        parse_resp_string, // key
        parse_resp_string, // value
        // opt: The opt combinator is used to make the parsing of the option, GET flag, and expiration option optional.
        // If these options are not present in the input string, opt will return None.
        // alt: The alt combinator is used to try multiple parsers in order until one succeeds.
        // In this case, it's used to parse either the "NX" or "XX" option.
        opt(alt((
            // value: The value combinator is used to map the result of a parser to a specific value.
            // In this case, it's used to map the result of the tag_no_case combinator to SetCommandSetOption::NX or
            // SetCommandSetOption::XX for the option.
            //
            value(SetCommandSetOption::NX, tag_no_case("$2\r\nNX\r\n")),
            value(SetCommandSetOption::XX, tag_no_case("$2\r\nXX\r\n")),
        ))),
        // GET: Return the old string stored at key, or nil if key did not exist.
        // tag_no_case: The tag_no_case combinator is used to match a case-insensitive string.
        // In this case, it's used to match the strings "$2\r\nNX\r\n", "$2\r\nXX\r\n", "$3\r\nGET\r\n", "$2\r\nEX\r\n", and "$2\r\nPX\r\n",
        // each one a potential expiration option in redis SET command.
        //
        opt(map(tag_no_case("$3\r\nGET\r\n"), |_| true)),
        // These maps all handle the various expiration options.
        opt(alt((
            // map: The map combinator is used to transform the output of the parser.
            // In this case, it's used to map the result of the tag_no_case combinator to true for the GET flag,
            // and to parse the seconds or milliseconds for the expiration option.
            //
            map(
                tuple((tag_no_case("$2\r\nEX\r\n"), parse_resp_string)),
                |(_expire_option, seconds)| {
                    SetCommandExpireOption::EX(expiry_to_timestamp(ExpiryOption::Seconds(
                        seconds.parse::<u32>().expect("Seconds conversion failed"),
                    )) as u32) // back to u32 since that's what seconds are
                },
            ),
            map(
                // we have to convert milliseconds to seconds and parse as u64
                tuple((tag_no_case("$2\r\nPX\r\n"), parse_resp_string)),
                |(_expire_option, milliseconds)| {
                    SetCommandExpireOption::PX(expiry_to_timestamp(ExpiryOption::Milliseconds(
                        milliseconds
                            .parse::<u64>()
                            .expect("Milliseconds conversion failed"),
                    )))
                },
            ),
        ))),
    ))(input)?;

    let set_params = SetCommandParameter {
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

fn parse_config(input: &str) -> IResult<&str, RedisCommand> {
    let (input, _) = tag("*")(input)?;
    let (input, _len) = (length)(input)?; // length eats crlf
    let (input, _) = tag_no_case("$6\r\nCONFIG\r\n$3\r\nGET\r\n")(input)?;

    let (input, key) = (alt((
        // value: The value combinator is used to map the result of a parser to a specific value.
        // In this case, it's used to map the result of the tag_no_case combinator to ConfigCommandParameters::Dir or
        // ConfigCommandParameters::Dbfilename for the option.
        //
        value(ConfigCommandParameter::Dir, tag_no_case("$3\r\ndir\r\n")),
        value(
            ConfigCommandParameter::DbFilename,
            tag_no_case("$10\r\ndbfilename\r\n"),
        ),
    )))(input)?;

    Ok((input, RedisCommand::Config(key)))
}

fn parse_keys(input: &str) -> IResult<&str, RedisCommand> {
    let (input, _) = tag("*")(input)?;
    let (input, _len) = (length)(input)?; // length eats crlf
    let (input, _) = tag_no_case("$4\r\nKEYS\r\n")(input)?;

    let (input, pattern) = (parse_resp_string)(input)?;

    Ok((input, RedisCommand::Keys(pattern.to_string())))
}

fn parse_info(input: &str) -> IResult<&str, RedisCommand> {
    let (input, _) = tag("*")(input)?;
    let (input, _len) = (length)(input)?; // length eats crlf
    let (input, _) = tag_no_case("$4\r\nINFO\r\n")(input)?;

    let (input, info_parameter) = (opt(alt((
        // value: The value combinator is used to map the result of a parser to a specific value.
        //
        value(InfoCommandParameter::All, tag_no_case("$3\r\nall\r\n")),
        value(InfoCommandParameter::Default, tag_no_case("$7\r\ndefault\r\n")),
        value(InfoCommandParameter::Replication, tag_no_case("$11\r\nreplication\r\n")),
    ))))(input)?;

    Ok((input, RedisCommand::Info(info_parameter)))
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
        parse_del,
        parse_strlen,
        parse_mget,
        parse_append,
        parse_config,
        parse_keys,
        parse_info,
    ))(input)
}
