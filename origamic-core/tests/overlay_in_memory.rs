use std::{collections::HashMap, future::Future, io, pin::Pin, sync::Arc, time::Duration};

use origamic_core::{
    factory::{Factory, Listener},
    overlay::context::OverlayContext,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    sync::{Mutex, mpsc},
    time::timeout,
};
use uuid::Uuid;

#[derive(Clone, Default)]
struct MemoryFactory {
    listeners: Arc<Mutex<HashMap<u8, mpsc::Sender<DuplexStream>>>>,
}

struct MemoryListener {
    incoming: mpsc::Receiver<DuplexStream>,
}

impl Listener for MemoryListener {
    type Stream = DuplexStream;

    fn accept(&mut self) -> Pin<Box<dyn Future<Output = io::Result<Self::Stream>> + Send + '_>> {
        Box::pin(async move {
            self.incoming
                .recv()
                .await
                .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "listener closed"))
        })
    }
}

impl Factory for MemoryFactory {
    type Stream = DuplexStream;
    type Listener = MemoryListener;
    type Address = u8;

    fn connect(
        &self,
        addr: Self::Address,
    ) -> Pin<Box<dyn Future<Output = io::Result<Self::Stream>> + Send>> {
        let listeners = self.listeners.clone();
        Box::pin(async move {
            let sender =
                listeners.lock().await.get(&addr).cloned().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "peer is not listening")
                })?;
            let (outgoing, incoming) = tokio::io::duplex(16 * 1024);
            sender
                .send(incoming)
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "peer listener closed"))?;
            Ok(outgoing)
        })
    }

    fn listen(
        &self,
        bind_addr: Option<Self::Address>,
    ) -> Pin<Box<dyn Future<Output = io::Result<Self::Listener>> + Send>> {
        let listeners = self.listeners.clone();
        Box::pin(async move {
            let addr = bind_addr.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "memory listener requires an address",
                )
            })?;
            let (sender, incoming) = mpsc::channel(64);
            let previous = listeners.lock().await.insert(addr, sender);
            if previous.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    "memory address is already bound",
                ));
            }
            Ok(MemoryListener { incoming })
        })
    }
}

#[tokio::test]
async fn virtual_streams_route_through_an_intermediate_overlay_node() {
    let factory = Arc::new(MemoryFactory::default());
    let network_id = Uuid::from_u128(42);

    let _relay = OverlayContext::try_with_options(
        network_id,
        origamic_core::overlay::address::Keypair::generate(),
        factory.clone(),
        vec![1, 3],
        Some(2),
    )
    .await
    .unwrap();
    let sender = OverlayContext::try_with_options(
        network_id,
        origamic_core::overlay::address::Keypair::generate(),
        factory.clone(),
        vec![2],
        Some(1),
    )
    .await
    .unwrap();
    let receiver = OverlayContext::try_with_options(
        network_id,
        origamic_core::overlay::address::Keypair::generate(),
        factory,
        vec![2],
        Some(3),
    )
    .await
    .unwrap();
    let mut receiver_listener = receiver.listen();

    let mut outbound = timeout(Duration::from_secs(5), sender.connect(receiver.address()))
        .await
        .expect("overlay connect timed out")
        .unwrap();
    let payload = b"routed through gossip";
    outbound.write_all(payload).await.unwrap();

    let (mut inbound, peer) = timeout(Duration::from_secs(5), receiver_listener.accept())
        .await
        .expect("incoming stream timed out")
        .unwrap();
    assert_eq!(peer, sender.address());

    let mut received = vec![0; payload.len()];
    timeout(Duration::from_secs(5), inbound.read_exact(&mut received))
        .await
        .expect("stream read timed out")
        .unwrap();
    assert_eq!(received, payload);

    inbound.write_all(b"relay reply").await.unwrap();
    let mut reply = [0; 11];
    timeout(Duration::from_secs(5), outbound.read_exact(&mut reply))
        .await
        .expect("reply timed out")
        .unwrap();
    assert_eq!(&reply, b"relay reply");
}
