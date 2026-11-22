use std::io::{self, Write};

use origamic::{
    core::{
        builder::NetworkBuilder,
        factory::Factory,
        id::OMNI_NETWORK_ID,
        overlay::{address::OverlayAddress, builder::OverlayContextBuilder},
    },
    tcp::factory::TcpFactory,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

fn ask_server_address() -> Result<OverlayAddress, Box<dyn std::error::Error>> {
    print!("Server Addr: ");
    io::stdout().flush().unwrap();
    let mut server_addr_str = String::new();
    io::stdin()
        .read_line(&mut server_addr_str)
        .expect("Failed to read line");

    Ok(server_addr_str.trim().parse::<OverlayAddress>()?)
}
