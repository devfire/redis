use log::info;
use nom::{
    branch::alt,
    bytes::complete::{tag, take_while},
    character::complete::{digit1, newline},
    combinator::{map, map_res},
    multi::many1,
    sequence::{delimited, preceded, tuple},
    IResult,
};

use crate::protocol::Command;

// Define the parsers
fn parse_number(input: &str) -> IResult<&str, u32> {
    map_res(digit1, |digit_str: &str| digit_str.parse::<u32>())(input)
}

fn parse_set_command(input: &str) -> IResult<&str, Command> {
    map(
        tuple((tag(r"\$4\r\nSET\r\n"), parse_bulk_string, newline)),
        |(_command, key, _)| Command::Set(key.to_string()),
    )(input)
}

fn parse_bulk_string(input: &str) -> IResult<&str, &str> {
    preceded(
        delimited(tag("$"), parse_number, tag("\r\n")),
        take_while(|c: char| c != '\r'),
    )(input)
}

fn parse_ping_command(input: &str) -> IResult<&str, Command> {
    map(tag("PING"), |_| Command::Ping)(input)
}

fn parse_echo_command(input: &str) -> IResult<&str, Command> {
    map(tuple((tag("ECHO"), parse_bulk_string)), |(_, argument)| {
        Command::Echo(argument.to_string())
    })(input)
}

pub fn parse_command(input: &str) -> IResult<&str, Command> {
    alt((parse_ping_command, parse_echo_command, parse_set_command))(input)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_parse_commands() {
      let input = r"*3\r\n\$4\r\nPING\r\n\$4\r\nECHO\r\n\$4\r\nSET\r\n";
      let result = parse_command(input);
      assert!(result.is_ok());

      let (_, command) = result.unwrap();
      match command {
          Command::Ping => println!("Ping command"),
          Command::Echo(argument) => println!("Echo command with argument: {}", argument),
          Command::Set(argument) => println!("Set command with argument: {}", argument),
      }
  }
}

