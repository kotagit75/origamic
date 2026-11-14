use std::{
    io::{self, Write},
    net::SocketAddr,
    str::FromStr,
    sync::Arc,
};

use http_body_util::{BodyExt, Empty};
use hyper::{Request, body::Bytes, client::conn::http1};
use hyper_util::rt::TokioIo;
use origamic::{
    core::{
        factory::Factory,
        overlay::{address::OverlayAddress, context::OverlayContext},
    },
    tcp::factory::TcpFactory,
};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listen_addr: SocketAddr = "127.0.0.1:8081".parse()?;
    let tcp_peers: Vec<SocketAddr> = vec!["127.0.0.1:8080".parse()?];

    print!("Network Id: ");
    io::stdout().flush().unwrap();
    let mut network_id_str = String::new();
    io::stdin()
        .read_line(&mut network_id_str)
        .expect("Failed to read line");

    print!("Server Addr: ");
    io::stdout().flush().unwrap();
    let mut server_addr_str = String::new();
    io::stdin()
        .read_line(&mut server_addr_str)
        .expect("Failed to read line");

    let context = OverlayContext::try_with_advertise(
        Uuid::from_str(&network_id_str.trim()).unwrap(),
        Arc::new(TcpFactory::new()),
        tcp_peers,
        listen_addr,
    )
    .await?;

    println!("Client Addr: {}", context.address());

    let server_addr: OverlayAddress = server_addr_str.trim().parse()?;
    let stream = context.connect(server_addr).await?;

    let io = TokioIo::new(stream);
    let (mut sender, conn) = http1::handshake::<_, Empty<Bytes>>(io).await?;

    tokio::spawn(async move {
        if let Err(err) = conn.await {
            eprintln!("Connection failed: {:?}", err);
        }
    });

    let req = Request::builder()
        .uri("http://dummy/hello")
        .body(Empty::<Bytes>::new())?;

    let res = sender.send_request(req).await?;
    let body = res.collect().await?.to_bytes();

    println!("Response: {}", String::from_utf8_lossy(&body));
    Ok(())
}
