//! # origamic
//!
//! `origamic` is the top-level façade crate for the origamic overlay networking library.
//!
//! It re-exports the three main components:
//!
//! - [`core`] – protocol primitives, gossip engine, and overlay context.
//! - [`tcp`] – TCP transport adapter implementing [`core::factory::Factory`].
//! - [`error`] – unified, user-facing error types.
//!
//! # Quick Start
//!
//! ```no_run
//! use std::{net::SocketAddr, sync::Arc};
//! use origamic::{core::overlay::context::OverlayContext, tcp::factory::TcpFactory};
//!
//! #[tokio::main]
//! async fn main() -> std::io::Result<()> {
//!     let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
//!     let ctx = OverlayContext::try_with_advertise(
//!         uuid::Uuid::new_v4(),
//!         Arc::new(TcpFactory::new()),
//!         vec![],
//!         addr,
//!     )
//!     .await?;
//!
//!     println!("Listening at overlay address: {}", ctx.address());
//!     Ok(())
//! }
//! ```

pub use origamic_core as core;
pub use origamic_core::error;
pub use origamic_tcp as tcp;
