use std::io::{self, Write};

use http_body_util::{BodyExt, Empty};
use hyper::{Request, body::Bytes, client::conn::http1};
use hyper_util::rt::TokioIo;
use origamic::{
    core::{
        builder::NetworkBuilder,
        factory::Factory,
        id::OMNI_NETWORK_ID,
        overlay::{address::OverlayAddress, builder::OverlayContextBuilder},
    },
    tcp::factory::TcpFactory,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let builder = OverlayContextBuilder::new()
        .network_id(OMNI_NETWORK_ID)
        .lower_peers(vec!["127.0.0.1:8080".parse()?])
        .advertise_addr("127.0.0.1:8081".parse()?);
    let context = NetworkBuilder::new(TcpFactory::new())
        .overlay(builder)
        .build()
        .await?;

    println!("Client Addr: {}", context.address());

    let server_addr = ask_server_address()?;
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

fn ask_server_address() -> Result<OverlayAddress, Box<dyn std::error::Error>> {
    print!("Server Addr: ");
    io::stdout().flush().unwrap();
    let mut server_addr_str = String::new();
    io::stdin()
        .read_line(&mut server_addr_str)
        .expect("Failed to read line");

    Ok(server_addr_str.trim().parse::<OverlayAddress>()?)
}
