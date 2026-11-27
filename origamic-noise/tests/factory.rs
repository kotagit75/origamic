use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex as StdMutex},
    task::{Context, Poll},
};

use origamic_core::factory::{Factory, Listener};
use origamic_noise::{
    NoiseConfig, NoiseFactory, NoiseIdentity, PinnedKeys, stream::NoiseStreamExt,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf, duplex},
    sync::{Mutex, mpsc},
};

#[derive(Clone)]
struct MemoryFactory {
    incoming_tx: mpsc::Sender<RecordingStream>,
    incoming_rx: Arc<Mutex<mpsc::Receiver<RecordingStream>>>,
    wire: Arc<StdMutex<Vec<u8>>>,
}

impl MemoryFactory {
    fn new() -> Self {
        let (incoming_tx, incoming_rx) = mpsc::channel(8);
        Self {
            incoming_tx,
            incoming_rx: Arc::new(Mutex::new(incoming_rx)),
            wire: Arc::new(StdMutex::new(Vec::new())),
        }
    }

    fn wire_bytes(&self) -> Vec<u8> {
        self.wire.lock().unwrap().clone()
    }
}

impl Factory for MemoryFactory {
    type Stream = RecordingStream;
    type Listener = MemoryListener;
    type Address = ();

    fn connect(
        &self,
        _addr: Self::Address,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Stream>> + Send>> {
        let incoming_tx = self.incoming_tx.clone();
        let wire = self.wire.clone();
        Box::pin(async move {
            let (outbound, inbound) = duplex(64 * 1024);
            incoming_tx
                .send(RecordingStream::new(inbound, wire.clone()))
                .await
                .map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "test listener has been dropped",
                    )
                })?;
            Ok(RecordingStream::new(outbound, wire))
        })
    }

    fn listen(
        &self,
        _bind_addr: Option<Self::Address>,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Listener>> + Send>> {
        let incoming_rx = self.incoming_rx.clone();
        Box::pin(async move { Ok(MemoryListener { incoming_rx }) })
    }
}

struct MemoryListener {
    incoming_rx: Arc<Mutex<mpsc::Receiver<RecordingStream>>>,
}

impl Listener for MemoryListener {
    type Stream = RecordingStream;

    fn accept(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Stream>> + Send + '_>> {
        let incoming_rx = self.incoming_rx.clone();
        Box::pin(async move {
            incoming_rx.lock().await.recv().await.ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "test listener has been closed",
                )
            })
        })
    }
}

struct RecordingStream {
    inner: DuplexStream,
    wire: Arc<StdMutex<Vec<u8>>>,
}

impl RecordingStream {
    fn new(inner: DuplexStream, wire: Arc<StdMutex<Vec<u8>>>) -> Self {
        Self { inner, wire }
    }
}

impl AsyncRead for RecordingStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for RecordingStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.as_mut().get_mut();
        match Pin::new(&mut this.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(written)) => {
                this.wire.lock().unwrap().extend_from_slice(&buf[..written]);
                Poll::Ready(Ok(written))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn factory_connect_and_accept_exchange_encrypted_stream_data() {
    let lower = MemoryFactory::new();
    let client_identity = NoiseIdentity::from_private([0x11; 32]);
    let server_identity = NoiseIdentity::from_private([0x22; 32]);

    let client = NoiseFactory::with_config(
        lower.clone(),
        NoiseConfig::new(client_identity.clone())
            .with_verifier(PinnedKeys::new([server_identity.public_key()])),
    );
    let server = NoiseFactory::with_config(
        lower.clone(),
        NoiseConfig::new(server_identity.clone())
            .with_verifier(PinnedKeys::new([client_identity.public_key()])),
    );
    let mut listener = server.listen(None).await.unwrap();

    let (client_result, server_result) = tokio::join!(client.connect(()), listener.accept());
    let mut client_stream = client_result.unwrap();
    let mut server_stream = server_result.unwrap();

    assert_eq!(
        client_stream.remote_static_key(),
        Some(server_identity.public_key().as_slice())
    );
    assert_eq!(
        server_stream.remote_static_key(),
        Some(client_identity.public_key().as_slice())
    );

    let client_message = b"client plaintext that must stay private";
    client_stream.write_all(client_message).await.unwrap();
    client_stream.flush().await.unwrap();
    let mut received = vec![0; client_message.len()];
    server_stream.read_exact(&mut received).await.unwrap();
    assert_eq!(received, client_message);

    let server_message = b"server plaintext that must stay private";
    server_stream.write_all(server_message).await.unwrap();
    server_stream.flush().await.unwrap();
    let mut received = vec![0; server_message.len()];
    client_stream.read_exact(&mut received).await.unwrap();
    assert_eq!(received, server_message);

    let wire = lower.wire_bytes();
    assert!(!contains_subslice(&wire, client_message));
    assert!(!contains_subslice(&wire, server_message));
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}
