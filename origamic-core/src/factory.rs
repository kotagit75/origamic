use crate::{overlay::message::GossipMessage, stream::LayerStream};
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;

pub trait Factory: Send + Sync + Unpin + 'static {
    type Stream: LayerStream + Send + Unpin + 'static;
    type Address: Clone
        + Send
        + Sync
        + Unpin
        + Serialize
        + DeserializeOwned
        + Eq
        + Hash
        + Debug
        + 'static;

    fn connect(
        &self,
        addr: Self::Address,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Stream>> + Send>>;

    /// インバウンドリスナーを起動して、受信した下位ストリームを peer_tx に供給する。
    fn start_listener(
        &self,
        bind_addr: Option<Self::Address>,
        peer_tx: mpsc::Sender<GossipMessage>,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>> {
        let _ = (bind_addr, peer_tx);
        Box::pin(async move { Ok(()) })
    }
}

impl<T: ?Sized + Factory> Factory for Arc<T> {
    type Stream = T::Stream;
    type Address = T::Address;

    fn connect(
        &self,
        addr: Self::Address,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<Self::Stream>> + Send>> {
        (**self).connect(addr)
    }

    fn start_listener(
        &self,
        bind_addr: Option<Self::Address>,
        peer_tx: mpsc::Sender<GossipMessage>,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>> {
        (**self).start_listener(bind_addr, peer_tx)
    }
}
