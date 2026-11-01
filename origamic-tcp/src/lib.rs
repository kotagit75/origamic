//! TCP transport adapter for the origamic overlay network.
//!
//! This crate provides [`TcpFactory`], which implements the
//! [`Factory`](origamic_core::factory::Factory) trait using standard TCP
//! connections. It is the default lower-level transport for an
//! [`OverlayContext`](origamic_core::overlay::context::OverlayContext).
//!
//! # Usage
//!
//! ```no_run
//! use std::sync::Arc;
//! use origamic_core::overlay::context::OverlayContext;
//! use origamic_tcp::factory::TcpFactory;
//!
//! # #[tokio::main]
//! # async fn main() -> std::io::Result<()> {
//! let factory = Arc::new(TcpFactory::listening("0.0.0.0:7000".parse().unwrap()));
//! let ctx = OverlayContext::try_new(factory, vec![]).await?;
//! # Ok(())
//! # }
//! ```

pub mod factory;
