use std::{net::SocketAddr, sync::Arc};

use origamic::{
    core::{id::OMNI_NETWORK_ID, overlay::builder::OverlayContextBuilder},
    tcp::factory::TcpFactory,
};
use tokio::io;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listen_addr: SocketAddr = "127.0.0.1:8080".parse()?;
    let tcp_peers: Vec<SocketAddr> = vec!["127.0.0.1:8081".parse()?];

    let context = OverlayContextBuilder::new()
        .network_id(OMNI_NETWORK_ID)
        .lower_factory(Arc::new(TcpFactory::new()))
        .lower_peers(tcp_peers)
        .advertise_addr(listen_addr)
        .build()
        .await?;

    println!("Server Addr: {}", context.address());

    let mut listener = context.listen();

    loop {
        let (stream, addr) = listener.accept().await?;
        println!("Accepted connection from {}", addr);

        tokio::spawn(async move {
            let (mut reader, mut writer) = io::split(stream);
            if io::copy(&mut reader, &mut writer).await.is_err() {
                eprintln!("echo stream io copy failed");
            }
        });
    }
}
