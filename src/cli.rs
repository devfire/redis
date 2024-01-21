use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// The directory where RDB files are stored
    #[arg(short, long)]
    pub dir: Option<String>,

    /// The name of the RDB file
    #[arg(short, long, value_name = "FILE")]
    pub dbfilename: Option<PathBuf>,
}