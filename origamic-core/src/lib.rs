//! # origamic-core
//!
//! Core abstractions, gossip engine, and overlay network implementation for origamic.
//!
//! ## Architecture
//!
//! ```text
//! ┌───────────────────────────────────────────┐
//! │               Application                 │
//! └──────────────┬────────────────────────────┘
//!                │ OverlayStream / OverlayListener
//! ┌──────────────▼────────────────────────────┐
//! │            OverlayContext                 │  ← virtual stream multiplexer
//! │  (implements Factory<Stream=OverlayStream │
//! │              Address=OverlayAddress>)     │
//! └──────────────┬────────────────────────────┘
//!                │ GossipMessage frames
//! ┌──────────────▼────────────────────────────┐
//! │              GossipNode                   │  ← epidemic broadcast / PEX
//! └──────────────┬────────────────────────────┘
//!                │ lower Factory::Stream
//! ┌──────────────▼────────────────────────────┐
//! │          Lower Transport (F: Factory)     │  ← e.g. TcpFactory
//! └───────────────────────────────────────────┘
//! ```
//!
//! The crate is designed to be transport-agnostic: any type implementing
//! [`factory::Factory`] can be used as the underlying carrier.

pub mod error;
pub mod factory;
pub mod gossip;
pub mod overlay;
pub mod stream;

pub use error::{AddressParseError, Error, ProtocolError, SendError};
