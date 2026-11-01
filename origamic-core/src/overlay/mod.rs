//! Overlay network abstraction layer.
//!
//! This module implements a virtual peer-to-peer network on top of any
//! lower-level transport that implements the [`Factory`] trait.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────┐
//! │                  Application Layer                  │
//! │          (OverlayContext as Factory<Stream=         │
//! │           OverlayStream, Address=OverlayAddress>)   │
//! ├─────────────────────────────────────────────────────┤
//! │              Overlay / Multiplexing Layer           │
//! │   stream   – OverlayStream, OverlayListener         │
//! │   context  – OverlayContext, AnyPeerAdder           │
//! │   message  – GossipMessage, StreamFrame, PEX        │
//! │   address  – OverlayAddress (Ed25519), Keypair      │
//! ├─────────────────────────────────────────────────────┤
//! │             Gossip Transport Layer                  │
//! │   (GossipNode running over any Factory impl)        │
//! └─────────────────────────────────────────────────────┘
//! ```
//!
//! **Addressing** is based on Ed25519 public keys: each node's [`OverlayAddress`]
//! is derived from the verifying key of its [`Keypair`]. Addresses are
//! serialised as 32-byte arrays and displayed as 64-character lowercase hex
//! strings.
//!
//! **Stream multiplexing** allows multiple logical duplex streams to share a
//! single gossip channel. Each stream is identified by a `(sender_address,
//! stream_id)` pair; data frames are wrapped in [`StreamFrame`] messages and
//! forwarded via the gossip layer.
//!
//! [`Factory`]: crate::factory::Factory
//! [`OverlayAddress`]: address::OverlayAddress
//! [`Keypair`]: address::Keypair
//! [`StreamFrame`]: message::StreamFrame

pub mod address;
pub mod context;
pub mod message;
pub mod stream;
