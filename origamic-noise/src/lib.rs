//! Noise Protocol transport layer for Origamic.
//!
//! This crate wraps an [`origamic_core::factory::Factory`] with a stateful
//! Noise transport. Each lower-layer connection performs a Noise XX
//! handshake, then exchanges length-prefixed encrypted records through a
//! [`NoiseStream`].
//!
//! The local Noise static key identifies this transport endpoint. It is
//! separate from Origamic's Ed25519 [`OverlayAddress`] and must be persisted
//! by the application if it should remain stable across restarts.
//!
//! Remote static keys are accepted by default. Applications that need peer
//! authentication should configure a verifier, such as [`PinnedKeys`], on
//! both sides. A successful Noise handshake alone does not bind a Noise key
//! to an Origamic [`OverlayAddress`].
//!
//! # Example
//!
//! ```
//! use origamic_core::factory::Factory;
//! use origamic_noise::{
//!     NoiseConfig, NoiseFactory, NoiseIdentity, PinnedKeys,
//! };
//!
//! fn wrap_with_noise<F: Factory>(inner: F, peer_key: [u8; 32]) -> NoiseFactory<F> {
//!    let identity = NoiseIdentity::from_private([0x24; 32]);
//!    let config = NoiseConfig::new(identity).with_verifier(PinnedKeys::new([peer_key]));
//!    NoiseFactory::with_config(inner, config)
//! }
//! ```
//!
//! [`OverlayAddress`]: origamic_core::overlay::address::OverlayAddress

pub mod factory;
pub mod listener;
pub mod stream;

pub use factory::{
    AcceptAll, AcceptNew, MAX_NOISE_FRAME, NOISE_PARAMS, NoiseConfig, NoiseFactory, NoiseIdentity,
    PinnedKeys, RemoteStaticKeyVerifier,
};
pub use listener::NoiseListener;
pub use stream::NoiseStream;
