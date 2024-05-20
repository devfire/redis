use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// The directory where RDB files are stored
    #[arg(long)]
    pub dir: Option<String>,

    /// The name of the RDB file
    #[arg(long, value_name = "FILE")]
    pub dbfilename: Option<PathBuf>,

    /// TCP port to listen on
    #[arg(short, long)]
    #[clap(default_value = "6379")]
    pub port: u16,

    /// Assume the "slave" role instead
    #[arg(long)]
    pub replicaof: String,

}
