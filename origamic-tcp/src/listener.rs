use std::{net::SocketAddr, pin::Pin};

use origamic_core::factory::Listener;
use tokio::net::{TcpListener, TcpStream};

/// TCP listener adapter that exposes accepted streams without protocol decoding.
pub struct TcpListenerAdapter {
    pub(crate) listener: Option<TcpListener>,
}

impl TcpListenerAdapter {
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        match &self.listener {
            Some(listener) => listener.local_addr(),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "TCP listener is not configured",
            )),
        }
    }
}

impl Listener for TcpListenerAdapter {
    type Stream = TcpStream;

    fn accept(
        &mut self,
    ) -> Pin<Box<dyn std::future::Future<Output = std::io::Result<Self::Stream>> + Send + '_>> {
        Box::pin(async move {
            match &self.listener {
                Some(listener) => Ok(listener.accept().await?.0),
                None => std::future::pending().await,
            }
        })
    }
}
