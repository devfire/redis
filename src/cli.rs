use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// The directory where RDB files are stored
    #[arg(long)]
    #[clap(default_value = ".")]
    pub dir: Option<String>,

    /// The name of the RDB file
    #[arg(long, value_name = "FILE")]
    #[clap(default_value = "dump.rdb")]
    pub dbfilename: Option<PathBuf>,
}