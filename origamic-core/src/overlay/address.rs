//! Overlay network addressing based on Ed25519 public keys.
//!
//! An [`OverlayAddress`] is a 32-byte Ed25519 verifying key that uniquely
//! identifies a node on the overlay network. [`Keypair`] bundles the signing
//! key with its corresponding address, providing a convenient handle for
//! signing gossip messages.

use ed25519_dalek::{SigningKey, VerifyingKey};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::error::AddressParseError;

/// A 32-byte Ed25519 public key used as a node's overlay network address.
///
/// Addresses are derived from the verifying (public) key of an Ed25519
/// keypair.  They are serialised as raw bytes in binary protocols and displayed
/// as 64-character lowercase hexadecimal strings in text contexts.
///
/// # Examples
///
/// ```
/// use origamic_core::overlay::address::{Keypair, OverlayAddress};
///
/// let keypair = Keypair::generate();
/// let addr = keypair.address;
///
/// // Round-trip through hex string
/// let hex = addr.to_hex();
/// let parsed: OverlayAddress = hex.parse().unwrap();
/// assert_eq!(addr, parsed);
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OverlayAddress(pub [u8; 32]);

impl OverlayAddress {
    /// Creates an `OverlayAddress` directly from a 32-byte array.
    ///
    /// No validation is performed; the caller is responsible for ensuring the
    /// bytes represent a valid Ed25519 point when cryptographic operations are
    /// needed.
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Derives an `OverlayAddress` from an Ed25519 verifying key.
    pub fn from_verifying_key(key: &VerifyingKey) -> Self {
        Self(key.to_bytes())
    }

    /// Attempts to reconstruct an Ed25519 [`VerifyingKey`] from this address.
    ///
    /// Returns an error if the stored bytes are not a valid compressed
    /// Ed25519 curve point.
    pub fn to_verifying_key(&self) -> Result<VerifyingKey, ed25519_dalek::SignatureError> {
        VerifyingKey::from_bytes(&self.0)
    }

    /// Returns a reference to the underlying 32-byte array.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Encodes the address as a 64-character lowercase hexadecimal string.
    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(64);
        for byte in &self.0 {
            use std::fmt::Write;
            let _ = write!(s, "{:02x}", byte);
        }
        s
    }

    /// Parses an `OverlayAddress` from a 64-character hexadecimal string.
    ///
    /// # Errors
    ///
    /// - [`AddressParseError::InvalidLength`] – if `s` is not exactly 64 characters.
    /// - [`AddressParseError::InvalidHex`] – if any character is not a valid hex digit.
    pub fn from_hex(s: &str) -> Result<Self, AddressParseError> {
        if s.len() != 64 {
            return Err(AddressParseError::InvalidLength {
                expected: 64,
                actual: s.len(),
            });
        }

        let mut bytes = [0u8; 32];
        for i in 0..32 {
            let chunk = &s[i * 2..i * 2 + 2];
            bytes[i] = u8::from_str_radix(chunk, 16).map_err(|_| {
                let first_invalid = chunk
                    .chars()
                    .enumerate()
                    .find(|(_, c)| !c.is_ascii_hexdigit())
                    .map(|(offset, c)| (i * 2 + offset, c))
                    .unwrap_or((i * 2, '?'));
                AddressParseError::InvalidHex {
                    index: first_invalid.0,
                    character: first_invalid.1,
                }
            })?;
        }

        Ok(Self(bytes))
    }
}

/// Formats the address as `OverlayAddress(<first 8 hex chars>)` for brevity.
impl fmt::Debug for OverlayAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OverlayAddress({})", &self.to_hex()[..8])
    }
}

/// Formats the address as its full 64-character hexadecimal representation.
impl fmt::Display for OverlayAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// Parses an `OverlayAddress` from a 64-character hexadecimal string.
///
/// This is a convenience wrapper around [`OverlayAddress::from_hex`].
impl std::str::FromStr for OverlayAddress {
    type Err = AddressParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

/// An Ed25519 keypair together with the node's [`OverlayAddress`].
///
/// The signing key is used to sign all outgoing [`GossipMessage`]s so that
/// recipients can authenticate the sender via the embedded `OverlayAddress`.
///
/// [`GossipMessage`]: crate::overlay::message::GossipMessage
#[derive(Clone)]
pub struct Keypair {
    /// The Ed25519 signing (private) key.
    pub signing_key: SigningKey,
    /// The overlay address derived from `signing_key`'s verifying key.
    pub address: OverlayAddress,
}

impl Keypair {
    /// Generates a fresh `Keypair` using the OS cryptographic RNG.
    pub fn generate() -> Self {
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let address = OverlayAddress::from_verifying_key(&signing_key.verifying_key());
        Self {
            signing_key,
            address,
        }
    }

    /// Creates a `Keypair` from an existing Ed25519 signing key.
    ///
    /// The corresponding `OverlayAddress` is derived automatically.
    pub fn from_signing_key(signing_key: SigningKey) -> Self {
        let address = OverlayAddress::from_verifying_key(&signing_key.verifying_key());
        Self {
            signing_key,
            address,
        }
    }
}
