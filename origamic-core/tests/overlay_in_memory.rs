use std::{collections::HashMap, future::Future, io, pin::Pin, sync::Arc, time::Duration};

use origamic_core::{
    discovery::PeerDiscovery,
    factory::{Factory, Listener},
    overlay::{builder::OverlayContextBuilder, context::OverlayContext},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
    sync::{Mutex, mpsc},
    time::timeout,
};
use uuid::Uuid;

#[derive(Clone, Default)]
struct MemoryNetwork {
    listeners: Arc<Mutex<HashMap<u8, mpsc::Sender<(DuplexStream, u8)>>>>,
}

impl MemoryNetwork {
    fn factory(&self, local_addr: u8) -> MemoryFactory {
        MemoryFactory {
            network: self.clone(),
            local_addr,
        }
    }
}

#[derive(Clone)]
struct MemoryFactory {
    network: MemoryNetwork,
    local_addr: u8,
}

struct MemoryListener {
    incoming: mpsc::Receiver<(DuplexStream, u8)>,
}

impl Listener for MemoryListener {
    type Stream = DuplexStream;
    type Address = u8;

    fn accept(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = io::Result<(Self::Stream, Option<Self::Address>)>> + Send + '_>>
    {
        Box::pin(async move {
            self.incoming
                .recv()
                .await
                .map(|(stream, peer_addr)| (stream, Some(peer_addr)))
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
        let network = self.network.clone();
        let local_addr = self.local_addr;
        Box::pin(async move {
            let sender = network
                .listeners
                .lock()
                .await
                .get(&addr)
                .cloned()
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "peer is not listening"))?;
            let (outgoing, incoming) = tokio::io::duplex(16 * 1024);
            sender
                .send((incoming, local_addr))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "peer listener closed"))?;
            Ok(outgoing)
        })
    }

    fn listen(
        &self,
        bind_addr: Option<Self::Address>,
    ) -> Pin<Box<dyn Future<Output = io::Result<Self::Listener>> + Send>> {
        let network = self.network.clone();
        Box::pin(async move {
            let addr = bind_addr.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "memory listener requires an address",
                )
            })?;
            let (sender, incoming) = mpsc::channel(64);
            let previous = network.listeners.lock().await.insert(addr, sender);
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
    let network = MemoryNetwork::default();
    let network_id = Uuid::from_u128(42);

    let _relay = OverlayContext::try_with_options(
        network_id,
        origamic_core::overlay::address::Keypair::generate(),
        Arc::new(network.factory(2)),
        vec![1, 3],
        Some(2),
    )
    .await
    .unwrap();
    let sender = OverlayContext::try_with_options(
        network_id,
        origamic_core::overlay::address::Keypair::generate(),
        Arc::new(network.factory(1)),
        vec![2],
        Some(1),
    )
    .await
    .unwrap();
    let receiver = OverlayContext::try_with_options(
        network_id,
        origamic_core::overlay::address::Keypair::generate(),
        Arc::new(network.factory(3)),
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

#[tokio::test]
async fn upper_overlay_learns_lower_overlay_peer_from_accepted_stream() {
    let network = MemoryNetwork::default();
    let lower_network_id = Uuid::from_u128(100);
    let lower_a = OverlayContext::try_with_options(
        lower_network_id,
        origamic_core::overlay::address::Keypair::generate(),
        Arc::new(network.factory(10)),
        vec![11],
        Some(10),
    )
    .await
    .unwrap();
    let lower_b = OverlayContext::try_with_options(
        lower_network_id,
        origamic_core::overlay::address::Keypair::generate(),
        Arc::new(network.factory(11)),
        vec![10],
        Some(11),
    )
    .await
    .unwrap();

    timeout(Duration::from_secs(5), async {
        while !lower_a.known_peers().contains(&lower_b.address())
            || !lower_b.known_peers().contains(&lower_a.address())
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("lower overlays did not discover each other's identities");

    let upper_network_id = Uuid::from_u128(101);
    let upper_a = OverlayContextBuilder::new()
        .network_id(upper_network_id)
        .advertise_addr(lower_a.address())
        .peer_discovery(Arc::new(lower_a.clone()))
        .lower_factory(Arc::new(lower_a.clone()))
        .build()
        .await
        .unwrap();
    let upper_b = OverlayContextBuilder::new()
        .network_id(upper_network_id)
        .advertise_addr(lower_b.address())
        .peer_discovery(Arc::new(lower_b.clone()))
        .lower_factory(Arc::new(lower_b.clone()))
        .build()
        .await
        .unwrap();

    let mut upper_a_listener = upper_a.listen();
    let mut upper_b_listener = upper_b.listen();

    let mut a_to_b = timeout(Duration::from_secs(5), upper_a.connect(upper_b.address()))
        .await
        .expect("upper-layer connect timed out")
        .unwrap();
    a_to_b
        .write_all(b"initial discovered connection")
        .await
        .unwrap();

    let (mut b_inbound, _) = timeout(Duration::from_secs(5), upper_b_listener.accept())
        .await
        .expect("upper-layer inbound stream timed out")
        .unwrap();
    let mut initial = vec![0; b"initial discovered connection".len()];
    timeout(Duration::from_secs(5), b_inbound.read_exact(&mut initial))
        .await
        .expect("initial stream read timed out")
        .unwrap();
    assert_eq!(initial, b"initial discovered connection");

    let mut b_to_a = timeout(Duration::from_secs(5), upper_b.connect(upper_a.address()))
        .await
        .expect("reverse upper-layer connect timed out")
        .unwrap();
    b_to_a.write_all(b"learned peer connection").await.unwrap();

    let (mut a_inbound, _) = timeout(Duration::from_secs(5), upper_a_listener.accept())
        .await
        .expect("reverse upper-layer inbound stream timed out")
        .unwrap();
    let mut response = vec![0; b"learned peer connection".len()];
    timeout(Duration::from_secs(5), a_inbound.read_exact(&mut response))
        .await
        .expect("reverse stream read timed out")
        .unwrap();
    assert_eq!(response, b"learned peer connection");
}

#[derive(Clone)]
struct ManualDiscovery<A> {
    peers: Arc<std::sync::Mutex<Vec<A>>>,
    updates: tokio::sync::broadcast::Sender<A>,
}

impl<A: Clone> ManualDiscovery<A> {
    fn new(peers: impl IntoIterator<Item = A>) -> Self {
        let (updates, _) = tokio::sync::broadcast::channel(16);
        Self {
            peers: Arc::new(std::sync::Mutex::new(peers.into_iter().collect())),
            updates,
        }
    }

    fn add_peer(&self, peer: A)
    where
        A: PartialEq,
    {
        let mut peers = self.peers.lock().unwrap();
        if !peers.contains(&peer) {
            peers.push(peer.clone());
            let _ = self.updates.send(peer);
        }
    }
}

impl<A: Clone + Send + Sync + 'static> PeerDiscovery for ManualDiscovery<A> {
    type Address = A;

    fn known_peers(&self) -> Vec<Self::Address> {
        self.peers.lock().unwrap().clone()
    }

    fn subscribe_peers(&self) -> tokio::sync::broadcast::Receiver<Self::Address> {
        self.updates.subscribe()
    }
}

#[tokio::test]
async fn upper_overlay_connects_to_peers_discovered_after_startup() {
    let network = MemoryNetwork::default();
    let mut discovered_peer_listener = network.factory(99).listen(Some(99)).await.unwrap();
    let discovery = Arc::new(ManualDiscovery::<u8>::new([]));

    let context = OverlayContextBuilder::new()
        .peer_discovery(discovery.clone())
        .advertise_addr(1)
        .lower_factory(Arc::new(network.factory(1)))
        .build()
        .await
        .unwrap();

    let accept_task = tokio::spawn(async move { discovered_peer_listener.accept().await });
    discovery.add_peer(99);

    timeout(Duration::from_secs(5), async {
        loop {
            context
                .broadcast(b"probe discovered peer".to_vec())
                .await
                .unwrap();
            if accept_task.is_finished() {
                let accepted = accept_task.await.unwrap().unwrap();
                assert_eq!(accepted.1, Some(1));
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("upper overlay did not connect to dynamically discovered peer");
}
