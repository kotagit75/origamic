use thiserror::Error;

/// origamic-core の統一エラー型
#[derive(Error, Debug)]
pub enum Error {
    #[error("Address error: {0}")]
    Address(#[from] AddressParseError),

    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    #[error("Send error: {0}")]
    Send(#[from] SendError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// OverlayAddress の解析エラー
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum AddressParseError {
    #[error("Invalid address length: expected {expected} hex characters, got {actual}")]
    InvalidLength { expected: usize, actual: usize },

    #[error("Invalid hex character at index {index}: '{character}'")]
    InvalidHex { index: usize, character: char },

    #[error("Invalid Ed25519 public key")]
    InvalidPublicKey,
}

/// プロトコル・メッセージのエンコード/デコード/検証エラー
#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("Serialization failed: {0}")]
    Serialization(String),

    #[error("Deserialization failed: {0}")]
    Deserialization(String),

    #[error("Signature verification failed")]
    InvalidSignature,

    #[error("Invalid frame: {0}")]
    InvalidFrame(String),
}

impl From<bincode::Error> for ProtocolError {
    fn from(err: bincode::Error) -> Self {
        ProtocolError::Deserialization(err.to_string())
    }
}

/// メッセージ送信エラー（内部ノードが停止している等）
#[derive(Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendError {
    #[error("Internal channel closed; node may have shut down")]
    ChannelClosed,
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for SendError {
    fn from(_: tokio::sync::mpsc::error::SendError<T>) -> Self {
        SendError::ChannelClosed
    }
}
