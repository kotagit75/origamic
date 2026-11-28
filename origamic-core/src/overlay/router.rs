use std::collections::HashMap;

use tokio::sync::{mpsc, oneshot};

use crate::{
    gossip::node::OutgoingLocalMessage,
    overlay::{
        address::OverlayAddress,
        message::{GossipMessage, StreamFrame},
        stream::OverlayStream,
    },
};

type ActiveStreams = HashMap<(OverlayAddress, u64), mpsc::Sender<Vec<u8>>>;

pub(crate) struct RouterTask {
    self_address: OverlayAddress,
    cmd_rx: mpsc::UnboundedReceiver<RouterCommand>,
    cmd_tx: mpsc::UnboundedSender<RouterCommand>,
    local_tx: mpsc::Sender<OutgoingLocalMessage>,
    inbound_tx: mpsc::Sender<(OverlayStream, OverlayAddress)>,
    active_streams: ActiveStreams,
}

pub(crate) enum RouterCommand {
    InboundGossip(GossipMessage),
    Connect {
        target: OverlayAddress,
        respond_to: oneshot::Sender<OverlayStream>,
    },
    RemoveStream {
        addr: OverlayAddress,
        stream_id: u64,
    },
    CloseStream {
        addr: OverlayAddress,
        stream_id: u64,
    },
}

impl RouterTask {
    pub fn new(
        self_address: OverlayAddress,
        cmd_rx: mpsc::UnboundedReceiver<RouterCommand>,
        cmd_tx: mpsc::UnboundedSender<RouterCommand>,
        local_tx: mpsc::Sender<OutgoingLocalMessage>,
        inbound_tx: mpsc::Sender<(OverlayStream, OverlayAddress)>,
    ) -> Self {
        Self {
            self_address,
            cmd_rx,
            cmd_tx,
            local_tx,
            inbound_tx,
            active_streams: HashMap::new(),
        }
    }

    pub fn spawn(self) {
        tokio::spawn(self.run());
    }

    async fn run(mut self) {
        while let Some(command) = self.cmd_rx.recv().await {
            match command {
                RouterCommand::InboundGossip(message) => {
                    self.handle_inbound_gossip(message).await;
                }
                RouterCommand::Connect { target, respond_to } => {
                    self.handle_connect(target, respond_to);
                }
                RouterCommand::RemoveStream { addr, stream_id } => {
                    self.active_streams.remove(&(addr, stream_id));
                }
                RouterCommand::CloseStream { addr, stream_id } => {
                    self.active_streams.remove(&(addr, stream_id));
                    self.send_close_frame(addr, stream_id);
                }
            }
        }
    }

    async fn handle_inbound_gossip(&mut self, message: GossipMessage) {
        if message
            .recipient
            .is_some_and(|target| target != self.self_address)
        {
            return;
        }

        let Ok(frame) = StreamFrame::decode(&message.payload) else {
            return;
        };
        let key = (message.sender, frame.stream_id);

        if frame.is_close {
            self.active_streams.remove(&key);
            return;
        }

        if let Some(stream_tx) = self.active_streams.get(&key).cloned() {
            if stream_tx.send(frame.data).await.is_err() {
                self.active_streams.remove(&key);
            }
            return;
        }

        if !frame.is_init && frame.data.is_empty() {
            return;
        }

        let (stream_tx, stream_rx) = mpsc::channel(256);
        if !frame.data.is_empty() && stream_tx.send(frame.data).await.is_err() {
            return;
        }
        self.active_streams.insert(key, stream_tx);

        let stream = OverlayStream::new_routed(
            message.sender,
            frame.stream_id,
            stream_rx,
            self.local_tx.clone(),
            self.cmd_tx.clone(),
        );
        let _ = self.inbound_tx.send((stream, message.sender)).await;
    }

    fn handle_connect(
        &mut self,
        target: OverlayAddress,
        respond_to: oneshot::Sender<OverlayStream>,
    ) {
        let stream_id = rand::random();
        let (stream_tx, stream_rx) = mpsc::channel(256);
        let stream = OverlayStream::new_routed(
            target,
            stream_id,
            stream_rx,
            self.local_tx.clone(),
            self.cmd_tx.clone(),
        );
        self.active_streams.insert((target, stream_id), stream_tx);
        if let Err(stream) = respond_to.send(stream) {
            drop(stream);
        }
    }

    fn send_close_frame(&self, addr: OverlayAddress, stream_id: u64) {
        let Ok(payload) = StreamFrame::new_close(stream_id).encode() else {
            return;
        };
        let local_tx = self.local_tx.clone();
        tokio::spawn(async move {
            let (done_tx, done_rx) = oneshot::channel();
            let message = OutgoingLocalMessage::to_with_done(addr, payload, done_tx);
            if local_tx.send(message).await.is_ok() {
                let _ = done_rx.await;
            }
        });
    }
}
