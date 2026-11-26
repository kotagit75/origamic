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
//! use origamic::{
//!    core::{builder::NetworkBuilder, id::OMNI_NETWORK_ID, overlay::builder::OverlayContextBuilder},
//!    tcp::factory::TcpFactory,
//! };
//! use tokio::io;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!    let builder = OverlayContextBuilder::new().network_id(OMNI_NETWORK_ID);
//!    let context = NetworkBuilder::new(TcpFactory::new())
//!        .overlay(builder)
//!        .build()
//!        .await?;
//!
//!    println!("Listening at overlay address: {}", context.address());
//!    Ok(())
//! }
//! ```

pub use origamic_core as core;
pub use origamic_core::error;
pub use origamic_noise as noise;
pub use origamic_tcp as tcp;
