use hyper::{Method, StatusCode};
use origamic::core::builder::NetworkBuilder;
use origamic::core::id::OMNI_NETWORK_ID;
use origamic::core::overlay::builder::OverlayContextBuilder;
use origamic::tcp::factory::TcpFactory;

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
    let builder = OverlayContextBuilder::new()
        .network_id(OMNI_NETWORK_ID)
        .lower_peers(vec!["127.0.0.1:8081".parse()?])
        .advertise_addr("127.0.0.1:8080".parse()?);
    let context = NetworkBuilder::new(TcpFactory::new())
        .overlay(builder)
        .build()
        .await?;

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
