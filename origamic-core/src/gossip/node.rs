use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use uuid::Uuid;

use crate::{
    factory::Factory,
    gossip::{cache::SeenCache, peers::PeerStore},
    overlay::{
        address::{Keypair, OverlayAddress},
        message::{GossipMessage, PeerExchangePayload},
    },
};

#[derive(Debug)]
pub struct OutgoingLocalMessage {
    pub recipient: Option<OverlayAddress>,
    pub payload: Vec<u8>,
    pub done_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl OutgoingLocalMessage {
    pub fn broadcast(payload: Vec<u8>) -> Self {
        Self {
            recipient: None,
            payload,
            done_tx: None,
        }
    }

    pub fn to(recipient: OverlayAddress, payload: Vec<u8>) -> Self {
        Self {
            recipient: Some(recipient),
            payload,
            done_tx: None,
        }
    }

    pub fn to_with_done(
        recipient: OverlayAddress,
        payload: Vec<u8>,
        done_tx: tokio::sync::oneshot::Sender<()>,
    ) -> Self {
        Self {
            recipient: Some(recipient),
            payload,
            done_tx: Some(done_tx),
        }
    }
}

pub struct GossipNodeHandle<F: ?Sized + Factory> {
    pub address: OverlayAddress,
    pub network_id: Uuid,
    pub local_tx: mpsc::Sender<OutgoingLocalMessage>,
    pub upper_rx: mpsc::Receiver<GossipMessage>,
    pub peer_tx: mpsc::Sender<GossipMessage>,
    pub peer_store: PeerStore<F::Address>,
}

pub struct GossipNode<F: ?Sized + Factory> {
    pub network_id: Uuid,
    pub keypair: Arc<Keypair>,
    pub lower_factory: Arc<F>,
    pub peer_store: PeerStore<F::Address>,
    pub cache: SeenCache,
    pub local_rx: mpsc::Receiver<OutgoingLocalMessage>,
    pub peer_rx: mpsc::Receiver<GossipMessage>,
    pub upper_tx: mpsc::Sender<GossipMessage>,
    pub default_ttl: u8,
}

impl<F: ?Sized + Factory> GossipNode<F> {
    pub fn spawn(
        network_id: Uuid,
        keypair: Keypair,
        lower_factory: Arc<F>,
        lower_peers: Vec<F::Address>,
        advertise_addr: Option<F::Address>,
        default_ttl: u8,
        buffer_size: usize,
    ) -> GossipNodeHandle<F> {
        let address = keypair.address;
        let keypair = Arc::new(keypair);

        let (local_tx, local_rx) = mpsc::channel(buffer_size);
        let (peer_tx, peer_rx) = mpsc::channel(buffer_size);
        let (upper_tx, upper_rx) = mpsc::channel(buffer_size);

        let peer_store = PeerStore::new(advertise_addr, lower_peers);

        let node = Self {
            network_id,
            keypair,
            lower_factory: Arc::clone(&lower_factory),
            peer_store: peer_store.clone(),
            cache: SeenCache::with_default(),
            local_rx,
            peer_rx,
            upper_tx,
            default_ttl,
        };

        tokio::spawn(node.run());

        GossipNodeHandle {
            address,
            network_id,
            local_tx,
            upper_rx,
            peer_tx,
            peer_store,
        }
    }

    /// イベントループの実行
    pub async fn run(mut self) {
        // 起動直後に初回 PEX アナウンス
        self.send_peer_exchange();

        let mut discovery_interval = tokio::time::interval(tokio::time::Duration::from_secs(3));
        discovery_interval.tick().await;

        loop {
            tokio::select! {
                _ = discovery_interval.tick() => {
                    self.send_peer_exchange();
                }
                Some(local_msg) = self.local_rx.recv() => {
                    self.handle_local(local_msg).await;
                }
                Some(msg) = self.peer_rx.recv() => {
                    self.handle_peer(msg).await;
                }
                else => break,
            }
        }
    }

    /// 定期またはイベント駆動での PEX (Peer Exchange) 発信
    fn send_peer_exchange(&mut self) {
        let pex = PeerExchangePayload::new(
            self.peer_store.self_addr().cloned(),
            self.peer_store.sample_peers(10),
        );
        if let Ok(payload) = pex.encode() {
            let id = Uuid::new_v4().to_string();
            let msg = GossipMessage::new_control(
                &self.keypair.signing_key,
                self.network_id,
                self.keypair.address,
                id,
                payload,
                self.default_ttl,
            );
            self.cache.check_and_insert(&msg.id);
            self.broadcast(&msg);
        }
    }

    /// 自ノードからの新規メッセージ発信処理 (FromLocal)
    async fn handle_local(&mut self, local_msg: OutgoingLocalMessage) {
        let done_tx = local_msg.done_tx;
        let msg = self.create_signed_message(local_msg.recipient, local_msg.payload);
        self.cache.check_and_insert(&msg.id);
        self.broadcast_with_done(&msg, done_tx);
    }

    /// ピアから受信したメッセージのパイプライン処理 (FromPeer)
    async fn handle_peer(&mut self, msg: GossipMessage) {
        // 1. 署名検証
        if !msg.verify() {
            return;
        }

        // 2. 既読チェック（重複排除）
        if !self.cache.check_and_insert(&msg.id) {
            return;
        }

        // 3. 制御メッセージ（PeerExchange等）の処理
        if msg.is_control {
            if let Ok(pex) = PeerExchangePayload::<F::Address>::decode(&msg.payload) {
                if let Some(listen_addr) = pex.listen_addr {
                    self.peer_store.add_peer(listen_addr);
                }
                self.peer_store.add_peers(pex.known_peers);
            }
            // 制御メッセージもゴシップ網の拡散のためにリレー
            self.maybe_relay(&msg);
            return;
        }

        // 4. 他ネットワークのパケットであってもゴシップ網の接続性のためにリレー
        self.maybe_relay(&msg);

        // 5. 自ネットワークに属するメッセージのみ上位層へ配送
        self.maybe_deliver_upper(msg).await;
    }

    /// メッセージ作成ヘルパー
    fn create_signed_message(
        &self,
        recipient: Option<OverlayAddress>,
        payload: Vec<u8>,
    ) -> GossipMessage {
        let id = Uuid::new_v4().to_string();
        GossipMessage::new_signed(
            &self.keypair.signing_key,
            self.network_id,
            self.keypair.address,
            recipient,
            id,
            payload,
            self.default_ttl,
        )
    }

    /// TTLが残っていれば減衰させて再拡散
    fn maybe_relay(&self, msg: &GossipMessage) {
        if msg.ttl > 1 {
            let mut relay_msg = msg.clone();
            relay_msg.ttl -= 1;
            self.broadcast(&relay_msg);
        }
    }

    /// 自ネットワークIDと一致する場合のみ上位層へ配信
    async fn maybe_deliver_upper(&self, msg: GossipMessage) {
        if msg.network_id == self.network_id {
            let _ = self.upper_tx.send(msg).await;
        }
    }

    /// 下位ピアへのメッセージブロードキャスト
    fn broadcast(&self, msg: &GossipMessage) {
        self.broadcast_with_done(msg, None);
    }

    /// 下位ピアへのメッセージブロードキャスト（完了通知付き）
    fn broadcast_with_done(
        &self,
        msg: &GossipMessage,
        done_tx: Option<tokio::sync::oneshot::Sender<()>>,
    ) {
        let Ok(encoded) = msg.encode() else {
            if let Some(tx) = done_tx {
                let _ = tx.send(());
            }
            return;
        };
        let bytes = Bytes::from(encoded);
        let peers = self.peer_store.get_peers();

        if peers.is_empty() {
            if let Some(tx) = done_tx {
                let _ = tx.send(());
            }
            return;
        }

        let total = peers.len();
        let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let done_tx_shared = Arc::new(tokio::sync::Mutex::new(done_tx));

        for peer_addr in peers {
            let factory = self.lower_factory.clone();
            let data = bytes.clone();
            let counter = counter.clone();
            let done_tx_shared = done_tx_shared.clone();

            tokio::spawn(async move {
                if let Ok(stream) = factory.connect(peer_addr).await {
                    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
                    let _ = framed.send(data).await;
                }
                let prev = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if prev + 1 == total {
                    if let Some(tx) = done_tx_shared.lock().await.take() {
                        let _ = tx.send(());
                    }
                }
            });
        }
    }
}

/// 下位ストリームから受信したフレームをデコードし、ゴシップノードの peer_tx に渡すリスナー関数
pub async fn process_inbound_stream<S>(
    stream: S,
    peer_tx: mpsc::Sender<GossipMessage>,
) -> std::io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
    while let Some(result) = framed.next().await {
        match result {
            Ok(bytes) => {
                if let Ok(msg) = GossipMessage::decode(&bytes)
                    && peer_tx.send(msg).await.is_err()
                {
                    break;
                }
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
