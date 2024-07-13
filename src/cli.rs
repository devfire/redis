use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// The directory where RDB files are stored
    #[arg(long, default_value = "dump.rdb")]
    pub dir: Option<String>,

    /// The name of the RDB file
    #[arg(long, default_value = "dump.rdb", value_name = "FILE")]
    pub dbfilename: Option<PathBuf>,

    /// TCP port to listen on
    #[arg(short, long, value_parser=clap::value_parser!(u16))]
    #[clap(default_value = "6379")]
    pub port: u16,

    /// Assume the "slave" role instead
    #[arg(long, value_name = "MASTER_HOST MASTER_PORT")]
    pub replicaof: Option<String>,
}
