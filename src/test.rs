use futures::sink::SinkExt;
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::net::TcpStream;
use tokio::stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let saddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8081);
    let conn = TcpStream::connect(saddr).await?;

    let mut server = Framed::new(conn, LinesCodec::new_with_max_length(1024));
    while let Some(Ok(line)) = server.next().await {
        match line.as_str() {
            "READY" => server.send("foo").await?,
            _ => println!("{}", line),
        }
    }
    Ok({})
}
