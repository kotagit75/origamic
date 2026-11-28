//! Virtual streams carried as gossip [`StreamFrame`] messages.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::{
    io::{self, AsyncRead, AsyncWrite, DuplexStream, ReadBuf},
    sync::{mpsc, oneshot},
};

use crate::{
    gossip::node::OutgoingLocalMessage,
    overlay::{address::OverlayAddress, message::StreamFrame, router::RouterCommand},
};

/// A logical bidirectional stream between two overlay peers.
///
/// Overlay-managed streams use bounded channels and apply backpressure when
/// sending frames. The original duplex-backed constructors remain available
/// for callers that create `OverlayStream` values directly.
pub struct OverlayStream {
    io: StreamIo,
}

enum StreamIo {
    Duplex {
        inner: DuplexStream,
        peer_addr: OverlayAddress,
        shutdown_rx: Option<oneshot::Receiver<()>>,
    },
    Routed {
        target: OverlayAddress,
        stream_id: u64,
        rx: mpsc::Receiver<Vec<u8>>,
        tx: mpsc::Sender<OutgoingLocalMessage>,
        reserve: Option<
            Pin<
                Box<
                    dyn Future<
                            Output = Result<
                                mpsc::OwnedPermit<OutgoingLocalMessage>,
                                mpsc::error::SendError<()>,
                            >,
                        > + Send,
                >,
            >,
        >,
        cmd_tx: mpsc::UnboundedSender<RouterCommand>,
        leftover: Vec<u8>,
        is_first_write: bool,
        close_queued: bool,
        is_shutdown: bool,
        shutdown_rx: Option<oneshot::Receiver<()>>,
    },
}

impl Unpin for OverlayStream {}

const MAX_STREAM_FRAME_PAYLOAD: usize = 4096;

impl OverlayStream {
    /// Creates an `OverlayStream` around a Tokio duplex stream.
    pub fn new(inner: DuplexStream, peer_addr: OverlayAddress) -> Self {
        Self {
            io: StreamIo::Duplex {
                inner,
                peer_addr,
                shutdown_rx: None,
            },
        }
    }

    /// Creates a duplex-backed stream that waits for `shutdown_rx` after shutdown.
    pub fn with_shutdown_rx(
        inner: DuplexStream,
        peer_addr: OverlayAddress,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> Self {
        Self {
            io: StreamIo::Duplex {
                inner,
                peer_addr,
                shutdown_rx: Some(shutdown_rx),
            },
        }
    }

    /// Returns the address of the remote overlay peer.
    pub fn peer_address(&self) -> OverlayAddress {
        match &self.io {
            StreamIo::Duplex { peer_addr, .. } => *peer_addr,
            StreamIo::Routed { target, .. } => *target,
        }
    }

    pub(crate) fn new_routed(
        target: OverlayAddress,
        stream_id: u64,
        rx: mpsc::Receiver<Vec<u8>>,
        tx: mpsc::Sender<OutgoingLocalMessage>,
        cmd_tx: mpsc::UnboundedSender<RouterCommand>,
    ) -> Self {
        Self {
            io: StreamIo::Routed {
                target,
                stream_id,
                rx,
                tx,
                reserve: None,
                cmd_tx,
                leftover: Vec::new(),
                is_first_write: true,
                close_queued: false,
                is_shutdown: false,
                shutdown_rx: None,
            },
        }
    }
}

impl StreamIo {
    fn poll_reserve(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<mpsc::OwnedPermit<OutgoingLocalMessage>>> {
        let Self::Routed { tx, reserve, .. } = self else {
            unreachable!("duplex streams do not reserve gossip capacity")
        };
        if reserve.is_none() {
            *reserve = Some(Box::pin(tx.clone().reserve_owned()));
        }
        match reserve
            .as_mut()
            .expect("reserve future initialized")
            .as_mut()
            .poll(cx)
        {
            Poll::Ready(Ok(permit)) => {
                *reserve = None;
                Poll::Ready(Ok(permit))
            }
            Poll::Ready(Err(_)) => {
                *reserve = None;
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "overlay gossip channel is closed",
                )))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_routed_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let Self::Routed {
            target,
            stream_id,
            is_first_write,
            is_shutdown,
            ..
        } = self
        else {
            unreachable!("expected a router-managed stream")
        };
        if *is_shutdown {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "overlay stream is shut down",
            )));
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let target = *target;
        let stream_id = *stream_id;
        let is_first_write = *is_first_write;
        let accepted = buf.len().min(MAX_STREAM_FRAME_PAYLOAD);
        let frame = if is_first_write {
            StreamFrame::new_init(stream_id, buf[..accepted].to_vec())
        } else {
            StreamFrame::new_data(stream_id, buf[..accepted].to_vec())
        };
        let encoded = match frame.encode() {
            Ok(encoded) => encoded,
            Err(error) => {
                return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, error)));
            }
        };

        match self.poll_reserve(cx) {
            Poll::Ready(Ok(permit)) => {
                permit.send(OutgoingLocalMessage::to(target, encoded));
                if let Self::Routed { is_first_write, .. } = self {
                    *is_first_write = false;
                }
                Poll::Ready(Ok(accepted))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_routed_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let Self::Routed {
            target,
            stream_id,
            close_queued,
            is_shutdown,
            ..
        } = self
        else {
            unreachable!("expected a router-managed stream")
        };
        if *is_shutdown {
            return Poll::Ready(Ok(()));
        }

        if !*close_queued {
            let target = *target;
            let stream_id = *stream_id;
            let encoded = match StreamFrame::new_close(stream_id).encode() {
                Ok(encoded) => encoded,
                Err(error) => {
                    return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, error)));
                }
            };
            match self.poll_reserve(cx) {
                Poll::Ready(Ok(permit)) => {
                    let (done_tx, done_rx) = oneshot::channel();
                    permit.send(OutgoingLocalMessage::to_with_done(target, encoded, done_tx));
                    if let Self::Routed {
                        close_queued,
                        shutdown_rx,
                        ..
                    } = self
                    {
                        *close_queued = true;
                        *shutdown_rx = Some(done_rx);
                    }
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }

        let result = match self {
            Self::Routed { shutdown_rx, .. } => match shutdown_rx.as_mut() {
                Some(receiver) => Pin::new(receiver).poll(cx),
                None => Poll::Ready(Ok(())),
            },
            Self::Duplex { .. } => unreachable!(),
        };
        match result {
            Poll::Ready(Ok(())) => {
                if let Self::Routed {
                    is_shutdown,
                    shutdown_rx,
                    ..
                } = self
                {
                    *is_shutdown = true;
                    *shutdown_rx = None;
                }
                self.remove_from_router();
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(_)) => {
                if let Self::Routed { is_shutdown, .. } = self {
                    *is_shutdown = true;
                }
                self.remove_from_router();
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "overlay close was not acknowledged",
                )))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn remove_from_router(&self) {
        if let Self::Routed {
            target,
            stream_id,
            cmd_tx,
            ..
        } = self
        {
            let _ = cmd_tx.send(RouterCommand::RemoveStream {
                addr: *target,
                stream_id: *stream_id,
            });
        }
    }
}

impl AsyncRead for OverlayStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut self.io {
            StreamIo::Duplex { inner, .. } => Pin::new(inner).poll_read(cx, buf),
            StreamIo::Routed { rx, leftover, .. } => {
                if buf.remaining() == 0 {
                    return Poll::Ready(Ok(()));
                }
                loop {
                    if !leftover.is_empty() {
                        let take = leftover.len().min(buf.remaining());
                        buf.put_slice(&leftover[..take]);
                        leftover.drain(..take);
                        return Poll::Ready(Ok(()));
                    }
                    match rx.poll_recv(cx) {
                        Poll::Ready(Some(data)) if data.is_empty() => continue,
                        Poll::Ready(Some(mut data)) => {
                            let take = data.len().min(buf.remaining());
                            buf.put_slice(&data[..take]);
                            if take < data.len() {
                                *leftover = data.split_off(take);
                            }
                            return Poll::Ready(Ok(()));
                        }
                        Poll::Ready(None) => return Poll::Ready(Ok(())),
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }
        }
    }
}

impl AsyncWrite for OverlayStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut self.io {
            StreamIo::Duplex { inner, .. } => Pin::new(inner).poll_write(cx, buf),
            StreamIo::Routed { .. } => self.io.poll_routed_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.io {
            StreamIo::Duplex { inner, .. } => Pin::new(inner).poll_flush(cx),
            StreamIo::Routed { .. } => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.io {
            StreamIo::Duplex {
                inner, shutdown_rx, ..
            } => {
                match Pin::new(inner).poll_shutdown(cx) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Pending => return Poll::Pending,
                }
                if let Some(receiver) = shutdown_rx.as_mut() {
                    match Pin::new(receiver).poll(cx) {
                        Poll::Ready(_) => {
                            *shutdown_rx = None;
                            Poll::Ready(Ok(()))
                        }
                        Poll::Pending => Poll::Pending,
                    }
                } else {
                    Poll::Ready(Ok(()))
                }
            }
            StreamIo::Routed { .. } => self.io.poll_routed_shutdown(cx),
        }
    }
}

impl Drop for OverlayStream {
    fn drop(&mut self) {
        match &self.io {
            StreamIo::Duplex { .. } => {}
            StreamIo::Routed {
                target,
                stream_id,
                cmd_tx,
                close_queued,
                is_shutdown,
                ..
            } => {
                let command = if *is_shutdown || *close_queued {
                    RouterCommand::RemoveStream {
                        addr: *target,
                        stream_id: *stream_id,
                    }
                } else {
                    RouterCommand::CloseStream {
                        addr: *target,
                        stream_id: *stream_id,
                    }
                };
                let _ = cmd_tx.send(command);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        time::{Duration, timeout},
    };

    fn routed_stream(
        tx: mpsc::Sender<OutgoingLocalMessage>,
        rx: mpsc::Receiver<Vec<u8>>,
        cmd_tx: mpsc::UnboundedSender<RouterCommand>,
    ) -> OverlayStream {
        OverlayStream::new_routed(OverlayAddress::new([0x42; 32]), 7, rx, tx, cmd_tx)
    }

    #[tokio::test]
    async fn routed_reads_preserve_payload_across_small_read_buffers() {
        let (gossip_tx, _gossip_rx) = mpsc::channel(1);
        let (incoming_tx, incoming_rx) = mpsc::channel(1);
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();
        incoming_tx.send(b"stream-data".to_vec()).await.unwrap();
        drop(incoming_tx);

        let mut stream = routed_stream(gossip_tx, incoming_rx, cmd_tx);
        let mut first = [0; 4];
        stream.read_exact(&mut first).await.unwrap();
        assert_eq!(&first, b"stre");
        let mut rest = [0; 7];
        stream.read_exact(&mut rest).await.unwrap();
        assert_eq!(&rest, b"am-data");
        let mut eof = [0; 1];
        assert_eq!(stream.read(&mut eof).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn routed_writes_wait_for_capacity_and_send_the_accepted_frame() {
        let (gossip_tx, mut gossip_rx) = mpsc::channel(1);
        gossip_tx
            .send(OutgoingLocalMessage::broadcast(Vec::new()))
            .await
            .unwrap();
        let (_incoming_tx, incoming_rx) = mpsc::channel(1);
        let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel();
        let mut stream = routed_stream(gossip_tx, incoming_rx, cmd_tx);

        let mut writer = tokio::spawn(async move { stream.write_all(b"payload").await });
        assert!(
            timeout(Duration::from_millis(20), &mut writer)
                .await
                .is_err()
        );

        gossip_rx.recv().await.unwrap();
        timeout(Duration::from_secs(1), &mut writer)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let outgoing = gossip_rx.recv().await.unwrap();
        let frame = StreamFrame::decode(&outgoing.payload).unwrap();
        assert!(frame.is_init);
        assert_eq!(frame.data, b"payload");
    }

    #[tokio::test]
    async fn shutdown_waits_for_close_dispatch_and_removes_router_state() {
        let (gossip_tx, mut gossip_rx) = mpsc::channel(1);
        gossip_tx
            .send(OutgoingLocalMessage::broadcast(Vec::new()))
            .await
            .unwrap();
        let (_incoming_tx, incoming_rx) = mpsc::channel(1);
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
        let mut stream = routed_stream(gossip_tx, incoming_rx, cmd_tx);

        let mut shutdown = tokio::spawn(async move { stream.shutdown().await });
        assert!(
            timeout(Duration::from_millis(20), &mut shutdown)
                .await
                .is_err()
        );

        gossip_rx.recv().await.unwrap();
        let close = timeout(Duration::from_secs(1), gossip_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(StreamFrame::decode(&close.payload).unwrap().is_close);
        let done_tx = close.done_tx.expect("close frame must request completion");
        assert!(
            timeout(Duration::from_millis(20), &mut shutdown)
                .await
                .is_err()
        );
        done_tx.send(()).unwrap();
        timeout(Duration::from_secs(1), &mut shutdown)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(
            cmd_rx.recv().await,
            Some(RouterCommand::RemoveStream { stream_id: 7, .. })
        ));
    }
}
