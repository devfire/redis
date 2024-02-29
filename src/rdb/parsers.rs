use nom::{
    branch::alt,
    bytes::{complete::tag, streaming::take},
    combinator::map,
    number::streaming::le_u8,
    IResult,
};

use super::format::{Rdb, RdbOpCode};

// fn parse_rdb_magic(input: &str) -> IResult<&str, Rdb> {
//     map(tag("REDIS"), |_| format::Rdb::MagicString)(input)
// }

fn parse_rdb_header(input: &[u8]) -> IResult<&[u8], Rdb> {
    let (input, _magic) = tag("REDIS")(input)?;
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

fn parse_length(input: &[u8]) -> IResult<&[u8], usize> {
    le_u8(input).map(|(remaining, value)| (remaining, value as usize))
}

fn parse_op_code(input: &[u8]) -> IResult<&[u8], Rdb> {
    
    Ok((
        input,
        Rdb::OpCode {
            opcode: RdbOpCode::Aux,
        },
    ))
}

pub fn parse_rdb_file(input: &[u8]) -> IResult<&[u8], Rdb> {
    alt((
        // map(tag_no_case("*1\r\n$4\r\nPING\r\n"), |_| RedisCommand::Ping),
        parse_rdb_header,
        parse_op_code,
    ))(input)
}
