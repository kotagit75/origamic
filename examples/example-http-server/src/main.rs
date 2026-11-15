use std::{net::SocketAddr, sync::Arc};

use hyper::{Method, StatusCode};
use origamic::{core::overlay::context::OverlayContext, tcp::factory::TcpFactory};

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, body::Incoming};
use hyper_util::rt::TokioIo;

async fn handler(req: Request<Incoming>) -> Result<Response<Full<Bytes>>, hyper::Error> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/hello") => Ok(Response::new(Full::new(Bytes::from("Hello, World!")))),
        _ => {
            let mut not_found = Response::new(Full::new(Bytes::from("Not Found")));
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            Ok(not_found)
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listen_addr: SocketAddr = "127.0.0.1:8080".parse()?;
    let tcp_peers: Vec<SocketAddr> = vec!["127.0.0.1:8081".parse()?];

    let context = OverlayContext::try_with_advertise(
        uuid::Uuid::new_v4(),
        Arc::new(TcpFactory::new()),
        tcp_peers,
        listen_addr,
    )
    .await?;

    println!("Network Id: {}", context.network_id());
    println!("Server Addr: {}", context.address());

    let mut listener = context.listen();

    loop {
        let (stream, addr) = listener.accept().await?;
        println!("Accepted connection from {}", addr);

        let io = TokioIo::new(stream);

        tokio::spawn(async move {
            if let Err(e) = http1::Builder::new()
                .serve_connection(io, service_fn(handler))
                .await
            {
                eprintln!("Error: {:?}", e);
            }
        });
    }
}
