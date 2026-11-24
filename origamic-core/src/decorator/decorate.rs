//! Stateless byte transformation hooks for decorated streams.

use tokio::io::ReadBuf;

/// Defines how decorated streams transform bytes while crossing a transport boundary.
///
/// Implementations must be `Copy`, `Send`, `Sync`, and `'static` so they can be
/// reused by factories and streams. This trait is intended for stateless
/// transformations; stateful protocols should implement [`crate::factory::Factory`]
/// and [`crate::factory::Listener`] directly.
pub trait Decorate: Unpin + Sync + Send + 'static + Copy {
    /// Transforms a plaintext or application byte slice before writing it to
    /// the underlying transport.
    fn from_upper(buf: &[u8]) -> Vec<u8>;

    /// Transforms bytes after they have been read from the underlying
    /// transport.
    ///
    /// Implementations may inspect or modify the contents already placed in
    /// `buf`.
    fn from_lower(buf: &mut ReadBuf<'_>);
}
