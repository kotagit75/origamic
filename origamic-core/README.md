# origamic-core

`origamic-core` provides the fundamental building blocks for the Origamic P2P network. 
It contains the core routing logic, epidemic gossip engine, virtual stream multiplexer, and transport-agnostic `Factory` abstractions.

> **Note:** For general application development, we recommend using the `origamic` facade crate instead of depending on this crate directly.

## Features
- **Overlay Router**: Multiplexes and reconstructs bidirectional virtual streams over single-shot gossip messages.
- **Gossip Engine**: Handles message deduplication, signature verification (Ed25519), and peer discovery (PEX).
- **Transport Abstraction**: Defines the `Factory` and `Listener` traits to support any underlying communication protocol.

## License
MIT License
