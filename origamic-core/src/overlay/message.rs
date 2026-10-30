use ed25519_dalek::{Signature, Signer, SigningKey, Verifier};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::ProtocolError;
use crate::overlay::address::OverlayAddress;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GossipMessage {
    pub network_id: Uuid,
    pub id: String,
    pub sender: OverlayAddress,
    pub recipient: Option<OverlayAddress>, // None: ブロードキャスト, Some: 特定宛先
    pub payload: Vec<u8>,
    pub ttl: u8,
    pub signature: Vec<u8>,
    #[serde(default)]
    pub is_control: bool,
}

impl GossipMessage {
    pub fn new_signed(
        signing_key: &SigningKey,
        network_id: Uuid,
        sender: OverlayAddress,
        recipient: Option<OverlayAddress>,
        id: String,
        payload: Vec<u8>,
        ttl: u8,
    ) -> Self {
        Self::new_signed_with_control(
            signing_key,
            network_id,
            sender,
            recipient,
            id,
            payload,
            ttl,
            false,
        )
    }

    pub fn new_signed_with_control(
        signing_key: &SigningKey,
        network_id: Uuid,
        sender: OverlayAddress,
        recipient: Option<OverlayAddress>,
        id: String,
        payload: Vec<u8>,
        ttl: u8,
        is_control: bool,
    ) -> Self {
        let sign_bytes =
            Self::digest_bytes(&network_id, &id, &sender, &recipient, &payload, is_control);
        let signature = signing_key.sign(&sign_bytes);
        Self {
            network_id,
            id,
            sender,
            recipient,
            payload,
            ttl,
            signature: signature.to_vec(),
            is_control,
        }
    }

    pub fn new_broadcast(
        signing_key: &SigningKey,
        network_id: Uuid,
        sender: OverlayAddress,
        id: String,
        payload: Vec<u8>,
        ttl: u8,
    ) -> Self {
        Self::new_signed(signing_key, network_id, sender, None, id, payload, ttl)
    }

    pub fn new_control(
        signing_key: &SigningKey,
        network_id: Uuid,
        sender: OverlayAddress,
        id: String,
        payload: Vec<u8>,
        ttl: u8,
    ) -> Self {
        Self::new_signed_with_control(
            signing_key,
            network_id,
            sender,
            None,
            id,
            payload,
            ttl,
            true,
        )
    }

    /// 署名の対象となる不変フィールド（TTLなどの可変ホップ情報は除外）
    pub fn digest_bytes(
        network_id: &Uuid,
        id: &str,
        sender: &OverlayAddress,
        recipient: &Option<OverlayAddress>,
        payload: &[u8],
        is_control: bool,
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(network_id.as_bytes());
        buf.extend_from_slice(id.as_bytes());
        buf.extend_from_slice(sender.as_bytes());
        if let Some(r) = recipient {
            buf.push(1);
            buf.extend_from_slice(r.as_bytes());
        } else {
            buf.push(0);
        }
        buf.push(if is_control { 1 } else { 0 });
        buf.extend_from_slice(payload);
        buf
    }

    pub fn verify(&self) -> bool {
        let Ok(verifying_key) = self.sender.to_verifying_key() else {
            return false;
        };
        let Ok(signature) = Signature::from_slice(&self.signature) else {
            return false;
        };
        let digest = Self::digest_bytes(
            &self.network_id,
            &self.id,
            &self.sender,
            &self.recipient,
            &self.payload,
            self.is_control,
        );
        verifying_key.verify(&digest, &signature).is_ok()
    }

    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        bincode::serialize(self).map_err(|e| ProtocolError::Serialization(e.to_string()))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ProtocolError> {
        bincode::deserialize(bytes).map_err(|e| ProtocolError::Deserialization(e.to_string()))
    }
}

/// ピア交換 (Peer Exchange) 用のペイロード
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerExchangePayload<A> {
    pub listen_addr: Option<A>,
    pub known_peers: Vec<A>,
}

impl<A: Serialize + serde::de::DeserializeOwned> PeerExchangePayload<A> {
    pub fn new(listen_addr: Option<A>, known_peers: Vec<A>) -> Self {
        Self {
            listen_addr,
            known_peers,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        bincode::serialize(self).map_err(|e| ProtocolError::Serialization(e.to_string()))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ProtocolError> {
        bincode::deserialize(bytes).map_err(|e| ProtocolError::Deserialization(e.to_string()))
    }
}

/// 仮想ストリーム通信用の多重化フレーム
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamFrame {
    pub stream_id: u64,
    pub data: Vec<u8>,
    pub is_init: bool,
    pub is_close: bool,
}

impl StreamFrame {
    pub fn new_data(stream_id: u64, data: Vec<u8>) -> Self {
        Self {
            stream_id,
            data,
            is_init: false,
            is_close: false,
        }
    }

    pub fn new_init(stream_id: u64, data: Vec<u8>) -> Self {
        Self {
            stream_id,
            data,
            is_init: true,
            is_close: false,
        }
    }

    pub fn new_close(stream_id: u64) -> Self {
        Self {
            stream_id,
            data: Vec::new(),
            is_init: false,
            is_close: true,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        bincode::serialize(self).map_err(|e| ProtocolError::Serialization(e.to_string()))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ProtocolError> {
        bincode::deserialize(bytes).map_err(|e| ProtocolError::Deserialization(e.to_string()))
    }
}

#[derive(Debug)]
pub enum IncomingMessage {
    FromPeer {
        sender: OverlayAddress,
        msg: GossipMessage,
    },
    FromLocal {
        recipient: Option<OverlayAddress>,
        payload: Vec<u8>,
    },
}
