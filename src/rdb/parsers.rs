use nom::{
    branch::alt,
    bytes::{complete::tag, streaming::take},
    combinator::map,
    IResult,
};

use crate::rdb::format;

use super::format::Rdb;

// fn parse_rdb_magic(input: &str) -> IResult<&str, Rdb> {
//     map(tag("REDIS"), |_| format::Rdb::MagicString)(input)
// }

fn parse_rdb_header(input: &[u8]) -> IResult<&[u8], Rdb> {
    let (input, magic) = tag("REDIS")(input)?;
    let (input, _) = take(1usize)(input)?; // Skip the newline character
    let (input, version) = take(4usize)(input)?;
    let version = String::from_utf8_lossy(version).to_string();

    Ok((
        input,
        Rdb::RdbHeader {
            magic: "REDIS".to_string(),
            version,
        },
    ))
}

pub fn parse_rdb_file(input: &[u8]) -> IResult<&[u8], Rdb> {
    alt((
        // map(tag_no_case("*1\r\n$4\r\nPING\r\n"), |_| RedisCommand::Ping),
        parse_rdb_header,
    ))(input)
}
