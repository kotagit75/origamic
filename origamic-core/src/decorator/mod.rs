//! Transport stream decorators.
//!
//! This module provides a generic wrapper layer for [`crate::factory::Factory`]
//! implementations. A decorator can transform bytes or behavior on top of an
//! existing transport without changing the transport address or listener API.
//!
//! The [`decorate::Decorate`] trait supplies stateless, copyable byte
//! transformations for outgoing and incoming streams.

pub mod decorate;
pub mod factory;
pub mod listener;
pub mod stream;
