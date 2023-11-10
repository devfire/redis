pub mod protocol;
pub mod parser;

use nom::{
    bytes::complete::{tag, take_while},
    character::complete::digit1,
    sequence::{preceded, tuple},
    IResult,
 };

 