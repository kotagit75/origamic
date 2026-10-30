use ed25519_dalek::{SigningKey, VerifyingKey};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::error::AddressParseError;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OverlayAddress(pub [u8; 32]);

impl OverlayAddress {
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn from_verifying_key(key: &VerifyingKey) -> Self {
        Self(key.to_bytes())
    }

    pub fn to_verifying_key(&self) -> Result<VerifyingKey, ed25519_dalek::SignatureError> {
        VerifyingKey::from_bytes(&self.0)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(64);
        for byte in &self.0 {
            use std::fmt::Write;
            let _ = write!(s, "{:02x}", byte);
        }
        s
    }

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

impl fmt::Debug for OverlayAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OverlayAddress({})", &self.to_hex()[..8])
    }
}

impl fmt::Display for OverlayAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl std::str::FromStr for OverlayAddress {
    type Err = AddressParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

#[derive(Clone)]
pub struct Keypair {
    pub signing_key: SigningKey,
    pub address: OverlayAddress,
}

impl Keypair {
    pub fn generate() -> Self {
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let address = OverlayAddress::from_verifying_key(&signing_key.verifying_key());
        Self {
            signing_key,
            address,
        }
    }

    pub fn from_signing_key(signing_key: SigningKey) -> Self {
        let address = OverlayAddress::from_verifying_key(&signing_key.verifying_key());
        Self {
            signing_key,
            address,
        }
    }
}
