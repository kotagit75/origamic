use std::{
    io::{self, Write},
    net::SocketAddr,
    str::FromStr,
    sync::Arc,
};

use origamic::{
    core::{
        factory::Factory,
        overlay::{address::OverlayAddress, builder::OverlayContextBuilder},
    },
    tcp::factory::TcpFactory,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listen_addr: SocketAddr = "127.0.0.1:8081".parse()?;
    let tcp_peers: Vec<SocketAddr> = vec!["127.0.0.1:8080".parse()?];

    let (network_id, server_addr) = ask_user()?;

    let context = OverlayContextBuilder::new()
        .network_id(network_id)
        .lower_factory(Arc::new(TcpFactory::new()))
        .lower_peers(tcp_peers)
        .advertise_addr(listen_addr)
        .build()
        .await?;

    println!("Client Addr: {}", context.address());

    let mut stream = context.connect(server_addr).await?;

    println!("Enter messages to send:");

    loop {
        let mut input = String::new();
        print!("> ");
        io::stdout().flush().unwrap();
        io::stdin()
            .read_line(&mut input)
            .expect("Failed to read line");
        stream.write_all(input.trim().as_bytes()).await?;
        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf).await?;
        let received = String::from_utf8_lossy(&buf[..n]);
        println!("[Server] {}", received);
    }
}

fn ask_user() -> Result<(Uuid, OverlayAddress), Box<dyn std::error::Error>> {
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

    Ok((
        Uuid::from_str(&network_id_str.trim())?,
        server_addr_str.trim().parse::<OverlayAddress>()?,
    ))
}
