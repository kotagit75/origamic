use std::{
    io::{self, Write},
    net::SocketAddr,
    str::FromStr,
    sync::Arc,
};

use origamic::{
    core::{
        factory::Factory,
        overlay::{address::OverlayAddress, context::OverlayContext},
    },
    tcp::factory::TcpFactory,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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

    let context = OverlayContext::with_advertise(
        Uuid::from_str(&network_id_str.trim()).unwrap(),
        Arc::new(TcpFactory::new()),
        tcp_peers,
        listen_addr,
    );

    println!("Client Addr: {}", context.address());

    let server_addr: OverlayAddress = server_addr_str.trim().parse().unwrap();
    let mut stream = context.connect(server_addr).await.unwrap();

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
