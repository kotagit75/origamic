//! Marker trait for bidirectional byte streams used inside the overlay layers.

use tokio::io::{AsyncRead, AsyncWrite};

/// A bidirectional async byte stream.
///
/// This is a marker trait that combines [`AsyncRead`] and [`AsyncWrite`] and is
/// used as the `Stream` associated type bound in [`Factory`](crate::factory::Factory).
/// It has a blanket implementation for every type that already implements both
/// halves, so implementors do not need to write any extra code.
pub trait LayerStream: AsyncRead + AsyncWrite {}

impl<T> LayerStream for T where T: AsyncRead + AsyncWrite {}
