# origamic-tcp

`origamic-tcp` provides the standard TCP transport implementation for the Origamic P2P network.
It implements the `Factory` trait from `origamic-core`, allowing you to establish and listen for physical TCP connections as the foundation of your overlay network.

## Usage
Use `TcpFactory` to build your network base:

```rust
use origamic_tcp::factory::TcpFactory;

// Combine this factory with `origamic_core::builder::NetworkBuilder`
let tcp_factory = TcpFactory::new();
```

## License
MIT License
