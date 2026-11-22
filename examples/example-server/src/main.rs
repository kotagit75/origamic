use origamic::{
    core::{builder::NetworkBuilder, id::OMNI_NETWORK_ID, overlay::builder::OverlayContextBuilder},
    tcp::factory::TcpFactory,
};
use tokio::io;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let builder = OverlayContextBuilder::new()
        .network_id(OMNI_NETWORK_ID)
        .lower_peers(vec!["127.0.0.1:8081".parse()?])
        .advertise_addr("127.0.0.1:8080".parse()?);
    let context = NetworkBuilder::new(TcpFactory::new())
        .overlay(builder)
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
