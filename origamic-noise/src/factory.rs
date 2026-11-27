//! Noise identity, verification policy, configuration, and [`Factory`] adapter.

use std::{future::Future, pin::Pin, sync::Arc};

use origamic_core::factory::Factory;
use snow::{Builder, params::NoiseParams};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use x25519_dalek::{PublicKey, StaticSecret};

use crate::{
    listener::NoiseListener,
    stream::{NoiseStream, NoiseStreamExt},
};

/// Noise protocol suite used by this transport.
///
/// The suite uses the XX handshake pattern, Curve25519, ChaChaPoly, and
/// BLAKE2s.
pub const NOISE_PARAMS: &str = "Noise_XX_25519_ChaChaPoly_BLAKE2s";

/// Maximum accepted Noise record size, in bytes.
///
/// This includes the Noise record but not the four-byte framing prefix.
pub const MAX_NOISE_FRAME: usize = 65535;

/// A persistent Noise static keypair.
///
/// The public key is derived from the private key. Store the private key
/// securely and reuse it when the endpoint's Noise identity should remain
/// stable. This key is independent of Origamic's Ed25519 node address.
#[derive(Clone)]
pub struct NoiseIdentity {
    private: [u8; 32],
    public: [u8; 32],
}

impl NoiseIdentity {
    /// Generates a new random static keypair using Snow's configured
    /// cryptographic provider.
    ///
    /// The returned private key should be persisted by the application if it
    /// must survive process restarts.
    pub fn generate() -> Result<Self, snow::Error> {
        let params: NoiseParams = NOISE_PARAMS
            .parse()
            .map_err(|_| snow::Error::State(snow::error::StateProblem::HandshakeNotFinished))?;
        let keypair = Builder::new(params).generate_keypair()?;
        let private: [u8; 32] = keypair.private.try_into().map_err(|_| snow::Error::Input)?;
        let public: [u8; 32] = keypair.public.try_into().map_err(|_| snow::Error::Input)?;
        Ok(Self { private, public })
    }

    /// Creates an identity from a 32-byte X25519 private key.
    ///
    /// The corresponding public key is derived automatically. Use a securely
    /// generated key and avoid reusing keys across unrelated identities.
    pub fn from_private(private: [u8; 32]) -> Self {
        let secret = StaticSecret::from(private);
        let public = PublicKey::from(&secret).to_bytes();
        Self { private, public }
    }

    /// Returns this identity's 32-byte X25519 public key.
    pub fn public_key(&self) -> [u8; 32] {
        self.public
    }

    pub(crate) fn private_key(&self) -> &[u8; 32] {
        &self.private
    }
}

/// Policy for accepting the remote static key learned during a Noise
/// handshake.
///
/// Implementations must be thread-safe because the same configured verifier
/// can be used by multiple concurrent connections. Returning `false` rejects
/// the handshake with `PermissionDenied`.
pub trait RemoteStaticKeyVerifier: Send + Sync {
    /// Returns whether `remote_static` is trusted.
    fn verify(&self, remote_static: &[u8]) -> bool;
}

/// Verifier that accepts every remote static key.
///
/// This is the default policy. It provides Noise encryption and
/// cryptographic peer key possession, but does not authenticate the peer's
/// identity. Use [`PinnedKeys`] or an application-specific verifier when
/// peer authentication is required.
pub struct AcceptAll;

impl RemoteStaticKeyVerifier for AcceptAll {
    fn verify(&self, _remote_static: &[u8]) -> bool {
        true
    }
}

/// Verifier that accepts a remote key if it matches one of a configured set
/// of pinned keys.
pub struct PinnedKeys {
    keys: std::collections::HashSet<[u8; 32]>,
}

impl PinnedKeys {
    /// Creates a verifier from the trusted 32-byte remote public keys.
    pub fn new(keys: impl IntoIterator<Item = [u8; 32]>) -> Self {
        Self {
            keys: keys.into_iter().collect(),
        }
    }
}

impl RemoteStaticKeyVerifier for PinnedKeys {
    fn verify(&self, remote_static: &[u8]) -> bool {
        <[u8; 32]>::try_from(remote_static)
            .map(|key| self.keys.contains(&key))
            .unwrap_or(false)
    }
}

/// Verifier that accepts remote keys and records them in memory.
///
/// This is a trust-on-first-use helper, but the current implementation does
/// not persist keys or reject later changes; callers can inspect neither the
/// stored set nor associate entries with a peer address. Use [`PinnedKeys`]
/// for enforceable pinning.
pub struct AcceptNew {
    seen: std::sync::RwLock<std::collections::HashSet<[u8; 32]>>,
}

impl AcceptNew {
    /// Creates an empty in-memory set of observed remote keys.
    pub fn new() -> Self {
        Self {
            seen: std::sync::RwLock::new(std::collections::HashSet::new()),
        }
    }
}

impl Default for AcceptNew {
    fn default() -> Self {
        Self::new()
    }
}

impl RemoteStaticKeyVerifier for AcceptNew {
    fn verify(&self, remote_static: &[u8]) -> bool {
        let Ok(key) = <[u8; 32]>::try_from(remote_static) else {
            return false;
        };
        self.seen
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(key);
        true
    }
}

/// Configuration shared by a [`NoiseFactory`] and its accepted connections.
///
/// The configuration contains the local static identity, remote-key
/// verification policy, and Noise prologue. Cloning it shares the verifier
/// and prologue while retaining the same local identity.
#[derive(Clone)]
pub struct NoiseConfig {
    identity: NoiseIdentity,
    verifier: Arc<dyn RemoteStaticKeyVerifier>,
    prologue: Arc<[u8]>,
}

impl NoiseConfig {
    /// Creates a configuration with `identity`, the default
    /// [`AcceptAll`] verifier, and the crate's default protocol prologue.
    pub fn new(identity: NoiseIdentity) -> Self {
        Self {
            identity,
            verifier: Arc::new(AcceptAll),
            prologue: Arc::from(b"origamic-noise-v1".as_slice()),
        }
    }

    /// Sets the verifier used for remote static keys.
    pub fn with_verifier<V: RemoteStaticKeyVerifier + 'static>(mut self, verifier: V) -> Self {
        self.verifier = Arc::new(verifier);
        self
    }

    /// Sets a shared verifier implementation.
    pub fn with_verifier_arc(mut self, verifier: Arc<dyn RemoteStaticKeyVerifier>) -> Self {
        self.verifier = verifier;
        self
    }

    /// Replaces the Noise prologue.
    ///
    /// Both peers must configure identical prologue bytes or the handshake
    /// will fail. Use this to bind the handshake to application-level context.
    pub fn with_prologue(mut self, prologue: impl Into<Vec<u8>>) -> Self {
        self.prologue = Arc::from(prologue.into());
        self
    }

    /// Returns the local static identity used by this configuration.
    pub fn identity(&self) -> &NoiseIdentity {
        &self.identity
    }
}

/// A [`Factory`] adapter that establishes a Noise session over each lower
/// transport connection.
///
/// `connect` performs the initiator side of the handshake; streams returned
/// by `listen` perform the responder side. The resulting stream carries
/// encrypted Noise records while preserving the lower factory's address
/// type.
pub struct NoiseFactory<F: Factory> {
    inner: F,
    config: NoiseConfig,
}

impl<F: Factory> NoiseFactory<F> {
    /// Wraps `inner` with a Noise layer using `identity` and the default
    /// [`AcceptAll`] remote-key policy.
    pub fn new(inner: F, identity: NoiseIdentity) -> Self {
        Self {
            inner,
            config: NoiseConfig::new(identity),
        }
    }

    /// Wraps `inner` with the supplied Noise configuration.
    pub fn with_config(inner: F, config: NoiseConfig) -> Self {
        Self { inner, config }
    }

    /// Returns the factory's local Noise static public key.
    pub fn local_public_key(&self) -> [u8; 32] {
        self.config.identity.public_key()
    }
}

impl<F: Factory> Factory for NoiseFactory<F> {
    type Stream = NoiseStream<F::Stream>;
    type Listener = NoiseListener<F>;
    type Address = F::Address;

    fn connect(
        &self,
        addr: Self::Address,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Stream>> + Send>> {
        let lower_future = self.inner.connect(addr);
        let config = self.config.clone();
        Box::pin(async move {
            let lower = lower_future.await?;
            perform_initiator_handshake(lower, &config).await
        })
    }

    fn listen(
        &self,
        bind_addr: Option<Self::Address>,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Listener>> + Send>> {
        let lower_future = self.inner.listen(bind_addr);
        let config = self.config.clone();
        Box::pin(async move {
            let inner = lower_future.await?;
            Ok(NoiseListener::new(inner, config))
        })
    }
}

pub(crate) async fn perform_initiator_handshake<S>(
    mut lower: S,
    config: &NoiseConfig,
) -> std::io::Result<NoiseStream<S>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let params: NoiseParams = NOISE_PARAMS
        .parse()
        .map_err(|_| invalid_data("invalid Noise protocol parameters"))?;
    let mut handshake = Builder::new(params)
        .local_private_key(config.identity.private_key())
        .map_err(snow_io_error)?
        .prologue(&config.prologue)
        .map_err(snow_io_error)?
        .build_initiator()
        .map_err(snow_io_error)?;

    let mut record = vec![0u8; MAX_NOISE_FRAME];
    let mut payload = vec![0u8; MAX_NOISE_FRAME];

    let len = handshake
        .write_message(&[], &mut record)
        .map_err(snow_io_error)?;
    write_record(&mut lower, &record[..len]).await?;

    let reply = read_record(&mut lower).await?;
    handshake
        .read_message(&reply, &mut payload)
        .map_err(snow_io_error)?;

    let len = handshake
        .write_message(&[], &mut record)
        .map_err(snow_io_error)?;
    write_record(&mut lower, &record[..len]).await?;

    let transport = finish_handshake(handshake, config.verifier.as_ref())?;
    Ok(NoiseStream::new(lower, transport))
}

pub(crate) async fn perform_responder_handshake<S>(
    mut lower: S,
    config: &NoiseConfig,
) -> std::io::Result<NoiseStream<S>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let params: NoiseParams = NOISE_PARAMS
        .parse()
        .map_err(|_| invalid_data("invalid Noise protocol parameters"))?;
    let mut handshake = Builder::new(params)
        .local_private_key(config.identity.private_key())
        .map_err(snow_io_error)?
        .prologue(&config.prologue)
        .map_err(snow_io_error)?
        .build_responder()
        .map_err(snow_io_error)?;

    let mut record = vec![0u8; MAX_NOISE_FRAME];
    let mut payload = vec![0u8; MAX_NOISE_FRAME];

    let request = read_record(&mut lower).await?;
    handshake
        .read_message(&request, &mut payload)
        .map_err(snow_io_error)?;

    let len = handshake
        .write_message(&[], &mut record)
        .map_err(snow_io_error)?;
    write_record(&mut lower, &record[..len]).await?;

    let final_message = read_record(&mut lower).await?;
    handshake
        .read_message(&final_message, &mut payload)
        .map_err(snow_io_error)?;

    let transport = finish_handshake(handshake, config.verifier.as_ref())?;
    Ok(NoiseStream::new(lower, transport))
}

fn finish_handshake(
    handshake: snow::HandshakeState,
    verifier: &dyn RemoteStaticKeyVerifier,
) -> std::io::Result<snow::TransportState> {
    if !handshake.is_handshake_finished() {
        return Err(invalid_data("Noise handshake did not finish"));
    }

    let remote_static = handshake
        .get_remote_static()
        .ok_or_else(|| invalid_data("Noise handshake did not provide a remote static key"))?
        .to_vec();
    if !verifier.verify(&remote_static) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "remote Noise static key rejected",
        ));
    }

    handshake.into_transport_mode().map_err(snow_io_error)
}

pub(crate) async fn write_record<S>(lower: &mut S, record: &[u8]) -> std::io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    if record.len() > MAX_NOISE_FRAME {
        return Err(invalid_data("Noise record exceeds the maximum frame size"));
    }
    let len = u32::try_from(record.len())
        .map_err(|_| invalid_data("Noise record length does not fit a u32"))?;
    lower.write_all(&len.to_be_bytes()).await?;
    lower.write_all(record).await
}

pub(crate) async fn read_record<S>(lower: &mut S) -> std::io::Result<Vec<u8>>
where
    S: AsyncRead + Unpin,
{
    let mut len_bytes = [0u8; 4];
    lower.read_exact(&mut len_bytes).await?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    if len > MAX_NOISE_FRAME {
        return Err(invalid_data("Noise frame exceeds the maximum size"));
    }
    let mut record = vec![0u8; len];
    lower.read_exact(&mut record).await?;
    Ok(record)
}

pub(crate) fn snow_io_error(error: snow::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, error)
}

pub(crate) fn invalid_data(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

    #[tokio::test]
    async fn xx_handshake_enables_bidirectional_transport() {
        let initiator_identity = NoiseIdentity::generate().unwrap();
        let responder_identity = NoiseIdentity::generate().unwrap();
        let initiator_config = NoiseConfig::new(initiator_identity.clone())
            .with_verifier(PinnedKeys::new([responder_identity.public_key()]));
        let responder_config = NoiseConfig::new(responder_identity.clone())
            .with_verifier(PinnedKeys::new([initiator_identity.public_key()]));

        let (initiator_lower, responder_lower) = duplex(1024 * 1024);
        let initiator = tokio::spawn(async move {
            perform_initiator_handshake(initiator_lower, &initiator_config).await
        });
        let responder = tokio::spawn(async move {
            perform_responder_handshake(responder_lower, &responder_config).await
        });

        let mut initiator_stream = initiator.await.unwrap().unwrap();
        let mut responder_stream = responder.await.unwrap().unwrap();

        initiator_stream
            .write_all(b"hello responder")
            .await
            .unwrap();
        initiator_stream.flush().await.unwrap();
        let mut buf = [0u8; 15];
        responder_stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello responder");

        responder_stream
            .write_all(b"hello initiator")
            .await
            .unwrap();
        responder_stream.flush().await.unwrap();
        let mut buf = [0u8; 15];
        initiator_stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello initiator");
    }

    #[tokio::test]
    async fn pinned_remote_key_rejects_handshake() {
        let initiator_identity = NoiseIdentity::generate().unwrap();
        let responder_identity = NoiseIdentity::generate().unwrap();
        let wrong_key = [0x42; 32];
        let initiator_config =
            NoiseConfig::new(initiator_identity).with_verifier(PinnedKeys::new([wrong_key]));
        let responder_config = NoiseConfig::new(responder_identity).with_verifier(AcceptAll);

        let (initiator_lower, responder_lower) = duplex(1024 * 1024);
        let initiator = tokio::spawn(async move {
            perform_initiator_handshake(initiator_lower, &initiator_config).await
        });
        let responder = tokio::spawn(async move {
            perform_responder_handshake(responder_lower, &responder_config).await
        });

        let (initiator_result, responder_result) = tokio::join!(initiator, responder);
        assert!(initiator_result.unwrap().is_err());
        assert!(responder_result.unwrap().is_ok());
    }

    #[tokio::test]
    async fn accepts_oversize_and_malformed_frames_as_io_errors() {
        let (mut client, mut server) = duplex(1024);
        let oversize = 65536u32.to_be_bytes();
        client.write_all(&oversize).await.unwrap();

        let error = read_record(&mut server).await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);

        let malformed = 3u32.to_be_bytes();
        client.write_all(&malformed).await.unwrap();
        client.write_all(b"xy").await.unwrap();
        client.shutdown().await.unwrap();
        let error = read_record(&mut server).await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn identity_derivation_is_stable_for_the_same_private_key() {
        let private_key = [0x24; 32];
        let first = NoiseIdentity::from_private(private_key);
        let second = NoiseIdentity::from_private(private_key);

        assert_eq!(first.public_key(), second.public_key());
        assert_ne!(first.public_key(), [0; 32]);
    }

    #[test]
    fn pinned_keys_requires_an_exact_match() {
        let trusted_key = [0x42; 32];
        let verifier = PinnedKeys::new([trusted_key]);

        assert!(verifier.verify(&trusted_key));
        assert!(!verifier.verify(&[0x24; 32]));
        assert!(!verifier.verify(&trusted_key[..31]));
    }

    #[test]
    fn accept_new_allows_reconnecting_with_an_observed_key() {
        let verifier = AcceptNew::new();
        let remote_key = [0x42; 32];

        assert!(verifier.verify(&remote_key));
        assert!(verifier.verify(&remote_key));
        assert!(!verifier.verify(&remote_key[..31]));
    }

    #[tokio::test]
    async fn handshake_rejects_different_prologues() {
        let initiator_config = NoiseConfig::new(NoiseIdentity::generate().unwrap())
            .with_prologue(b"initiator context".to_vec());
        let responder_config = NoiseConfig::new(NoiseIdentity::generate().unwrap())
            .with_prologue(b"responder context".to_vec());
        let (initiator_lower, responder_lower) = duplex(1024 * 1024);

        let initiator = tokio::spawn(async move {
            perform_initiator_handshake(initiator_lower, &initiator_config).await
        });
        let responder = tokio::spawn(async move {
            perform_responder_handshake(responder_lower, &responder_config).await
        });

        let initiator_result = tokio::time::timeout(std::time::Duration::from_secs(2), initiator)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            initiator_result.err().unwrap().kind(),
            std::io::ErrorKind::InvalidData
        );
        responder.abort();
    }
}
