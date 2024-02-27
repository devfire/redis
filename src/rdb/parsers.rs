use nom::{
    bytes::complete::tag,
    combinator::map,
    IResult,
};

use crate::rdb::format;

fn parse_rdb_magic(input: &str) -> IResult<&str, format::Rdb> {
    map(tag("REDIS"), |_| format::Rdb::MagicString)(input)
}

