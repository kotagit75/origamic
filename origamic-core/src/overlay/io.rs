use tokio::sync::mpsc;

use crate::{overlay::message::GossipMessage, overlay::router::RouterCommand};

pub(crate) struct IoDriver {
    gossip_rx: mpsc::Receiver<GossipMessage>,
    router_cmd_tx: mpsc::UnboundedSender<RouterCommand>,
}

impl IoDriver {
    pub fn new(
        gossip_rx: mpsc::Receiver<GossipMessage>,
        router_cmd_tx: mpsc::UnboundedSender<RouterCommand>,
    ) -> Self {
        Self {
            gossip_rx,
            router_cmd_tx,
        }
    }

    pub fn spawn(self) {
        tokio::spawn(self.run());
    }

    async fn run(mut self) {
        while let Some(message) = self.gossip_rx.recv().await {
            if self
                .router_cmd_tx
                .send(RouterCommand::InboundGossip(message))
                .is_err()
            {
                break;
            }
        }
    }
}
