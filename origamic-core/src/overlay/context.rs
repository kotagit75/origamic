use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, mpsc};
use uuid::Uuid;

use crate::{
    factory::Factory,
    gossip::{
        node::{GossipNode, GossipNodeHandle, OutgoingLocalMessage, process_inbound_stream},
        peers::PeerStore,
    },
    overlay::{
        address::{Keypair, OverlayAddress},
        message::{GossipMessage, StreamFrame},
        stream::{OverlayListener, OverlayStream},
    },
};

type ActiveStreamMap = HashMap<(OverlayAddress, u64), mpsc::Sender<Vec<u8>>>;

/// 型消去されたピア追加インターフェース
pub trait AnyPeerAdder: Send + Sync {
    fn add_any(&self, peer: Box<dyn std::any::Any + Send>) -> bool;
}

struct TypedPeerAdder<A: Clone + Eq + std::hash::Hash + Send + Sync + 'static> {
    store: PeerStore<A>,
}

impl<A: Clone + Eq + std::hash::Hash + Send + Sync + 'static> AnyPeerAdder for TypedPeerAdder<A> {
    fn add_any(&self, peer: Box<dyn std::any::Any + Send>) -> bool {
        if let Ok(addr) = peer.downcast::<A>() {
            self.store.add_peer(*addr)
        } else {
            false
        }
    }
}

/// オーバーレイネットワークのノード状態を管理するコンテキスト。
/// 自身が `Factory` を実装しており、最上位オーバーレイ層でのストリーム接続や、
/// インバウンドリスナーの取得が行える。
#[derive(Clone)]
pub struct OverlayContext {
    network_id: Uuid,
    keypair: Arc<Keypair>,
    peer_adder: Arc<dyn AnyPeerAdder>,
    pub(crate) local_tx: mpsc::Sender<OutgoingLocalMessage>,
    pub(crate) active_streams: Arc<Mutex<ActiveStreamMap>>,
    pub(crate) inbound_stream_rx: Arc<Mutex<mpsc::Receiver<(OverlayStream, OverlayAddress)>>>,
}

const DEFAULT_GOSSIP_TTL: u8 = 7;
const DEFAULT_GOSSIP_BUFFER_SIZE: usize = 256;

impl OverlayContext {
    /// 新規ネットワーク（ランダムなUuid）を立ち上げてノードを生成
    pub fn new<F: Factory>(lower_factory: Arc<F>, lower_peers: Vec<F::Address>) -> Self {
        Self::with_network(Uuid::new_v4(), lower_factory, lower_peers)
    }

    /// 既存のネットワークID（Uuid）に参加するノードを生成
    pub fn with_network<F: Factory>(
        network_id: Uuid,
        lower_factory: Arc<F>,
        lower_peers: Vec<F::Address>,
    ) -> Self {
        Self::with_network_and_keypair(network_id, Keypair::generate(), lower_factory, lower_peers)
    }

    /// ネットワークIDおよび鍵ペアを明示指定してノードを生成
    pub fn with_network_and_keypair<F: Factory>(
        network_id: Uuid,
        keypair: Keypair,
        lower_factory: Arc<F>,
        lower_peers: Vec<F::Address>,
    ) -> Self {
        Self::with_options(network_id, keypair, lower_factory, lower_peers, None)
    }

    /// 広告用アドレス（自身の受付アドレス）を指定してノードを生成
    pub fn with_advertise<F: Factory>(
        network_id: Uuid,
        lower_factory: Arc<F>,
        lower_peers: Vec<F::Address>,
        advertise_addr: F::Address,
    ) -> Self {
        Self::with_options(
            network_id,
            Keypair::generate(),
            lower_factory,
            lower_peers,
            Some(advertise_addr),
        )
    }

    /// 全オプションを指定してノードを生成
    pub fn with_options<F: Factory>(
        network_id: Uuid,
        keypair: Keypair,
        lower_factory: Arc<F>,
        lower_peers: Vec<F::Address>,
        advertise_addr: Option<F::Address>,
    ) -> Self {
        let address = keypair.address;
        let GossipNodeHandle {
            address: _,
            network_id: _,
            local_tx,
            upper_rx,
            peer_tx,
            peer_store,
        } = GossipNode::spawn(
            network_id,
            keypair.clone(),
            lower_factory.clone(),
            lower_peers,
            advertise_addr.clone(),
            DEFAULT_GOSSIP_TTL,
            DEFAULT_GOSSIP_BUFFER_SIZE,
        );

        let active_streams: Arc<Mutex<ActiveStreamMap>> = Arc::new(Mutex::new(HashMap::new()));
        let (inbound_tx, inbound_rx) = mpsc::channel(128);

        start_stream_router_task(
            address,
            upper_rx,
            active_streams.clone(),
            local_tx.clone(),
            inbound_tx,
        );

        // 下位トランスポートのインバウンドリスナーを自動起動
        let factory_for_listener = Arc::clone(&lower_factory);
        let addr_for_listener = advertise_addr;
        let peer_tx_for_listener = peer_tx.clone();
        tokio::spawn(async move {
            if let Err(e) = factory_for_listener
                .start_listener(addr_for_listener, peer_tx_for_listener)
                .await
            {
                eprintln!("Failed to start lower transport listener: {}", e);
            }
        });

        Self {
            network_id,
            keypair: Arc::new(keypair),
            peer_adder: Arc::new(TypedPeerAdder { store: peer_store }),
            local_tx,
            active_streams,
            inbound_stream_rx: Arc::new(Mutex::new(inbound_rx)),
        }
    }

    /// 下位トランスポートのアドレス型に応じたピアを追加
    pub fn add_peer<A: Clone + Eq + std::hash::Hash + Send + Sync + 'static>(&self, addr: A) -> bool {
        self.peer_adder.add_any(Box::new(addr))
    }

    pub fn network_id(&self) -> Uuid {
        self.network_id
    }

    pub fn address(&self) -> OverlayAddress {
        self.keypair.address
    }

    /// インバウンド仮想ストリームを待受けるリスナーを取得
    pub fn listen(&self) -> OverlayListener {
        OverlayListener::new(self.inbound_stream_rx.clone())
    }

    pub fn inbound_stream_rx(&self) -> Arc<Mutex<mpsc::Receiver<(OverlayStream, OverlayAddress)>>> {
        self.inbound_stream_rx.clone()
    }

    /// 自ネットワーク全体へブロードキャスト発信
    pub async fn broadcast(
        &self,
        payload: Vec<u8>,
    ) -> Result<(), mpsc::error::SendError<OutgoingLocalMessage>> {
        self.local_tx
            .send(OutgoingLocalMessage::broadcast(payload))
            .await
    }

    /// 自ネットワーク内の特定公開鍵宛てにメッセージ発信
    pub async fn send_to(
        &self,
        recipient: OverlayAddress,
        payload: Vec<u8>,
    ) -> Result<(), mpsc::error::SendError<OutgoingLocalMessage>> {
        self.local_tx
            .send(OutgoingLocalMessage::to(recipient, payload))
            .await
    }
}

impl Factory for OverlayContext {
    type Stream = OverlayStream;
    type Address = OverlayAddress;

    fn connect(
        &self,
        addr: Self::Address,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Stream>> + Send>> {
        let (user_stream, internal_stream) = tokio::io::duplex(65536);
        let stream_id: u64 = rand::random();
        let (to_stream_tx, from_router_rx) = mpsc::channel(256);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        spawn_outbound_stream_bridge(
            addr,
            stream_id,
            internal_stream,
            to_stream_tx,
            from_router_rx,
            shutdown_tx,
            self.active_streams.clone(),
            self.local_tx.clone(),
        );

        let overlay_stream = OverlayStream::with_shutdown_rx(user_stream, addr, shutdown_rx);
        Box::pin(async move { Ok(overlay_stream) })
    }

    fn start_listener(
        &self,
        _bind_addr: Option<Self::Address>,
        peer_tx: mpsc::Sender<GossipMessage>,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>> {
        let mut listener = self.listen();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let tx = peer_tx.clone();
                tokio::spawn(async move {
                    let _ = process_inbound_stream(stream, tx).await;
                });
            }
        });
        Box::pin(async move { Ok(()) })
    }
}

/// アウトバウンド仮想ストリームの送受信ブリッジタスクを起動
pub(crate) fn spawn_outbound_stream_bridge(
    addr: OverlayAddress,
    stream_id: u64,
    internal_stream: tokio::io::DuplexStream,
    to_stream_tx: mpsc::Sender<Vec<u8>>,
    mut from_router_rx: mpsc::Receiver<Vec<u8>>,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    active_streams: std::sync::Arc<
        tokio::sync::Mutex<HashMap<(OverlayAddress, u64), mpsc::Sender<Vec<u8>>>>,
    >,
    local_tx: mpsc::Sender<OutgoingLocalMessage>,
) {
    tokio::spawn(async move {
        {
            let mut map = active_streams.lock().await;
            map.insert((addr, stream_id), to_stream_tx);
        }

        let (mut read_half, mut write_half) = tokio::io::split(internal_stream);

        // 送信ブリッジ: user_stream への write を相手宛ての StreamFrame としてゴシップ網へ発信
        let mut shutdown_tx = Some(shutdown_tx);
        let send_task = async {
            let mut buf = vec![0u8; 4096];
            let mut is_first = true;
            loop {
                match read_half.read(&mut buf).await {
                    Ok(0) => {
                        let close_frame = StreamFrame::new_close(stream_id);
                        if let Ok(encoded) = close_frame.encode() {
                            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                            let out = OutgoingLocalMessage::to_with_done(addr, encoded, done_tx);
                            let _ = local_tx.send(out).await;
                            let _ = done_rx.await;
                        }
                        if let Some(tx) = shutdown_tx.take() {
                            let _ = tx.send(());
                        }
                        break;
                    }
                    Ok(n) => {
                        let frame = if is_first {
                            is_first = false;
                            StreamFrame::new_init(stream_id, buf[..n].to_vec())
                        } else {
                            StreamFrame::new_data(stream_id, buf[..n].to_vec())
                        };

                        if let Ok(encoded) = frame.encode() {
                            let out = OutgoingLocalMessage::to(addr, encoded);
                            if local_tx.send(out).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(_) => {
                        let close_frame = StreamFrame::new_close(stream_id);
                        if let Ok(encoded) = close_frame.encode() {
                            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                            let out = OutgoingLocalMessage::to_with_done(addr, encoded, done_tx);
                            let _ = local_tx.send(out).await;
                            let _ = done_rx.await;
                        }
                        if let Some(tx) = shutdown_tx.take() {
                            let _ = tx.send(());
                        }
                        break;
                    }
                }
            }
        };

        // 受信ブリッジ: router から届いた相手からのデータを user_stream の読み出し側へ書き込む
        let recv_task = async {
            while let Some(data) = from_router_rx.recv().await {
                if write_half.write_all(&data).await.is_err() {
                    break;
                }
            }
            let _ = write_half.shutdown().await;
        };

        tokio::select! {
            _ = send_task => {},
            _ = recv_task => {},
        }

        // 終了処理: active_streams から削除
        let mut map = active_streams.lock().await;
        map.remove(&(addr, stream_id));
    });
}

/// 仮想ストリーム多重化ルータタスク（上位受信メッセージを各ストリームへ振り分け）
fn start_stream_router_task(
    self_address: OverlayAddress,
    mut upper_rx: mpsc::Receiver<GossipMessage>,
    active_streams: Arc<Mutex<ActiveStreamMap>>,
    local_tx: mpsc::Sender<OutgoingLocalMessage>,
    inbound_tx: mpsc::Sender<(OverlayStream, OverlayAddress)>,
) {
    tokio::spawn(async move {
        while let Some(msg) = upper_rx.recv().await {
            // 宛先チェック: 自分宛て、またはブロードキャストのみ処理
            if let Some(target) = msg.recipient
                && target != self_address
            {
                continue;
            }

            // StreamFrame のデコード
            let Ok(frame) = StreamFrame::decode(&msg.payload) else {
                continue;
            };

            let key = (msg.sender, frame.stream_id);
            let mut streams = active_streams.lock().await;

            if frame.is_close {
                streams.remove(&key);
                continue;
            }

            if let Some(stream_tx) = streams.get(&key) {
                let _ = stream_tx.send(frame.data).await;
            } else if frame.is_init || !frame.data.is_empty() {
                // 新規インバウンドストリームの生成
                spawn_inbound_stream_bridge(
                    key,
                    frame.data,
                    &mut streams,
                    active_streams.clone(),
                    local_tx.clone(),
                    &inbound_tx,
                )
                .await;
            }
        }
    });
}

/// 新規インバウンド接続用の duplex ストリームおよび送受信ブリッジを起動
async fn spawn_inbound_stream_bridge(
    (sender_addr, stream_id): (OverlayAddress, u64),
    initial_data: Vec<u8>,
    streams: &mut ActiveStreamMap,
    active_streams: Arc<Mutex<ActiveStreamMap>>,
    local_tx: mpsc::Sender<OutgoingLocalMessage>,
    inbound_tx: &mpsc::Sender<(OverlayStream, OverlayAddress)>,
) {
    let (user_stream, internal_stream) = tokio::io::duplex(65536);
    let (to_stream_tx, mut from_router_rx) = mpsc::channel(256);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    let _ = to_stream_tx.send(initial_data).await;
    streams.insert((sender_addr, stream_id), to_stream_tx);

    let (mut read_half, mut write_half) = tokio::io::split(internal_stream);

    // インバウンドストリーム用の送受信ブリッジタスク
    let active_streams_clone = active_streams.clone();
    tokio::spawn(async move {
        let mut shutdown_tx = Some(shutdown_tx);
        let send_task = async {
            let mut buf = vec![0u8; 4096];
            loop {
                match read_half.read(&mut buf).await {
                    Ok(0) => {
                        let close_frame = StreamFrame::new_close(stream_id);
                        if let Ok(encoded) = close_frame.encode() {
                            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                            let out = OutgoingLocalMessage::to_with_done(sender_addr, encoded, done_tx);
                            let _ = local_tx.send(out).await;
                            let _ = done_rx.await;
                        }
                        if let Some(tx) = shutdown_tx.take() {
                            let _ = tx.send(());
                        }
                        break;
                    }
                    Ok(n) => {
                        let frame = StreamFrame::new_data(stream_id, buf[..n].to_vec());
                        if let Ok(encoded) = frame.encode() {
                            let out = OutgoingLocalMessage::to(sender_addr, encoded);
                            if local_tx.send(out).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(_) => {
                        let close_frame = StreamFrame::new_close(stream_id);
                        if let Ok(encoded) = close_frame.encode() {
                            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                            let out = OutgoingLocalMessage::to_with_done(sender_addr, encoded, done_tx);
                            let _ = local_tx.send(out).await;
                            let _ = done_rx.await;
                        }
                        if let Some(tx) = shutdown_tx.take() {
                            let _ = tx.send(());
                        }
                        break;
                    }
                }
            }
        };

        let recv_task = async {
            while let Some(data) = from_router_rx.recv().await {
                if write_half.write_all(&data).await.is_err() {
                    break;
                }
            }
            let _ = write_half.shutdown().await;
        };

        tokio::select! {
            _ = send_task => {},
            _ = recv_task => {},
        }

        // 終了処理: active_streams から削除
        let mut map = active_streams_clone.lock().await;
        map.remove(&(sender_addr, stream_id));
    });

    let overlay_stream = OverlayStream::with_shutdown_rx(user_stream, sender_addr, shutdown_rx);
    let _ = inbound_tx.send((overlay_stream, sender_addr)).await;
}
