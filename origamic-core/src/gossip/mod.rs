//! Gossip protocol subsystem.
//!
//! This module contains the three building blocks of the epidemic broadcast layer:
//!
//! - [`cache`] – deduplication cache that prevents a node from processing the
//!   same gossip message more than once.
//! - [`peers`] – thread-safe peer address store shared between the gossip event
//!   loop and the transport layer.
//! - [`node`] – the [`GossipNode`](node::GossipNode) event loop and its
//!   [`GossipNodeHandle`](node::GossipNodeHandle) returned to callers.

pub mod cache;
pub mod node;
pub mod peers;
