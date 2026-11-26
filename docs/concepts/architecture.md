# Origamic Architecture

Origamic is a Rust library that builds a P2P message delivery network and virtual bidirectional streams on top of existing bidirectional communications. Applications communicate with other nodes (peers) through the overlay layer provided by Origamic. Complex processes such as underlying connection establishment and message relaying along the path are concealed by the library.

```text
┌──────────────────┐
│   Application    │
└────────┬─────────┘
         │ Bidirectional streams
┌────────▼─────────┐
│  Overlay Layer   │ ── Virtual stream multiplexing and routing
└────────┬─────────┘
         │
┌────────▼─────────┐
│   Gossip Layer   │ ── Signature verification, deduplication, relay, and peer discovery
└────────┬─────────┘
         │
┌────────┬─────────┐
│ Transport Layer  │ ── Communication infrastructure such as TCP and Noise (Factory)
└────────┬─────────┘
         │
┌────────▼─────────┐
│ Physical Network │
└──────────────────┘
```

## Crates and Main Components

Origamic consists of multiple crates, allowing you to build a network by assembling layers like blocks to suit your use case. The starting point for this assembly is the `Factory` abstract interface, which generates and accepts communication streams.

* **`origamic-core`**: Provides core functions such as communication abstraction (`Factory`), P2P relay engine, and virtual stream routing. It does not depend on any specific communication means.
* **`origamic-tcp`**: Provides TCP as the transport layer.
* **`origamic-noise`**: A transport layer that wraps the underlying stream with the Noise protocol to provide path encryption.
* **`origamic`**: A facade that integrates the above components and provides them in an easy-to-use form for users.

## Logical Addresses and Transport Addresses

Origamic uses two types of addresses with different roles.

* **Logical Address**: A node-specific ID generated from a cryptographic key (public key). It is used when an application specifies a destination or identifies a communication partner.
* **Transport Address (Physical Address)**: An address used to actually reach nodes on the network, similar to an IP address and port number in TCP. It is used for initial connections and for nodes to inform each other of their connection destinations.

Each node has the ID of the logical network it belongs to, and a group of nodes with the same ID forms a single P2P network. **Note that a logical address is merely an identifier and does not guarantee reachability on the network by itself.** Ensuring reachability or reconnection when a node moves depends on whether the underlying transport layer can reach the peer.

## Node Participation and Message Delivery

Upon startup, a node starts accepting connections from the underlying transport and joins the network using known initial peers as contact points. After joining, peers periodically exchange connection destination information with each other to autonomously expand the connection network of the entire network.

Sent messages include a digital signature of the sender, allowing relay nodes to detect tampering. On the other hand, setting a hop limit (TTL) that decrements with each relay prevents unlimited diffusion.

Furthermore, even for messages targeted at a specific node, direct routing through the delivery network is not performed. They are transported via gossip relay through all peers along the path, and the final receiver determines whether the message is addressed to its own node.

## Virtual Streams

The overlay layer multiplexes bidirectional virtual streams that are easy for applications to handle on top of the single-message delivery network. Data written by an application is internally split into small units and flows through the network, and the receiving side reconstructs it into the original stream based on the sender and identifier.

**[Important Restrictions]**
This virtual stream does not provide TCP-like features such as "order guarantee," "retransmission control," or "delivery confirmation." In addition, even if stream connection initiation (`connect`) succeeds, it does not mean that the destination node has been reached or that it has been accepted at the application layer.
For use cases that cannot tolerate data loss or out-of-order delivery, users must ensure reliability using higher-level application protocols or underlying transport layer mechanisms.

## Layer Composition and Recursivity

Since each layer (transport layer, encryption layer, overlay layer, etc.) shares the same `Factory` interface, they can be composed in any order. For example, recursive configurations are possible, such as sandwiching Noise encryption on top of TCP communication, or building another independent overlay network on top of a certain overlay network.

However, simply stacking layers does not automatically improve the reachability of underlying layers or the message delivery characteristics of each layer. Please design configurations carefully according to your requirements.

## Security Boundaries

When designing the architecture, it is important to correctly understand the security boundaries.

* **Signatures (Tamper Prevention)**: The digital signature of a gossip message only guarantees that the "logical sender" and "message content" have not been tampered with. It does not encrypt the communication path or authenticate whether the sender is a trusted party.
* **Encryption and Authentication**: Although communication path encryption is possible by inserting Noise or similar protocols, how to trust the other party's public key depends on the user's settings. Since all connections are accepted by default, key verification logic (such as pinning) must be explicitly incorporated if mutual authentication is required.

Design "logical address proof" and "communication path encryption and authentication" as independent layers.
