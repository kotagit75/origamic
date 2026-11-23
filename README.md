<div align="center">
    <h1>Origamic</h1>
    <p>A self-organizing, decentralized P2P library in Rust for building recursive virtual networks on top of existing networks.</p>
</div>

Origamic is a P2P overlay network library built in Rust.
By combining Ed25519 public key-based routing with a gossip engine using epidemic broadcast, it helps build flexible and robust networks.

## ✨ Why Origamic?

1. 📡 **IP-independent communication**
   Node addresses are based on 32-byte Ed25519 public keys. Therefore, as long as you know the other party's public key, continuous communication is possible even if their physical IP address changes due to network movement.
2. 🔐 **Strong communication obfuscation (Experimental stage)**
   By placing an encryption layer between the overlay network and the upper application layer, it is possible to achieve a high level of communication obfuscation.

> [!CAUTION]
>This feature is extremely powerful, for both better and worse. Please give thorough consideration to your purpose of use and laws/regulations, and operate with caution at your own risk. Furthermore, at present, **it is unverified (with no guarantees) whether this obfuscation and encryption layer implementation actually functions as expected**.

## 💎 Features
- 🔁 **Recursive Overlay Network**: Deploy virtual networks using any communication stream (TCP, WebSocket, or **Origamic itself**) as a foundation.
- 🌐 **Virtual Stream Multiplexing**: Build and multiplex multiple TCP-like bidirectional virtual stream communications simultaneously on top of a single-shot gossip message network.
- 🔐 **Secure Design**: Verify the authenticity of all messages using addresses and digital signatures based on Ed25519 key pairs. The network is securely isolated by UUIDs.

## 🚀 Get Started
Please enable the origamic crate:
```bash
cargo add tokio --features full
cargo add --git https://github.com/kotagit75/origamic origamic
```

## 🧩 Example

A basic echo server with Origamic.

```rust
use origamic::{
    core::{builder::NetworkBuilder, id::OMNI_NETWORK_ID, overlay::builder::OverlayContextBuilder},
    tcp::factory::TcpFactory,
};
use tokio::io;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let builder = OverlayContextBuilder::new().network_id(OMNI_NETWORK_ID);
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
```

There are other examples as well.
- [Client](examples/example-client)
- [Server](examples/example-server)
- [HTTP Client](examples/example-http-client)
- [HTTP Server](examples/example-http-server)

## License
This project is licensed under the [MIT License](LICENSE).
