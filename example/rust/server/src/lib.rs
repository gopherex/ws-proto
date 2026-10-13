//! The echo.v1 service implementation shared by the binary and the tests.
//! Behaviour mirrors `example/server` (Go) so every client sees the same
//! answers from either server, plus request-metadata echoing (see
//! [`EchoImpl`]) which the Go/TS tests do not exercise.

use echo_proto::echo::v1::{
    echo_service_ws::{
        BidiServerStream, ClientStreamServerStream, EchoServiceHandler, ServerStreamServerStream,
    },
    BidiRequest, BidiResponse, ClientStreamRequest, ClientStreamResponse, ServerStreamRequest,
    ServerStreamResponse, UnaryRequest, UnaryResponse,
};
use ws_proto::{Request, Response, Status};

/// Header a client may send on `Unary`; when present it is echoed back as a
/// leading response header and the same value is set as trailer `x-echo-trailer`.
pub const ECHO_MD_HEADER: &str = "x-echo-md";
pub const ECHO_MD_TRAILER: &str = "x-echo-trailer";

pub struct EchoImpl;

impl EchoServiceHandler for EchoImpl {
    async fn unary(&self, req: Request<UnaryRequest>) -> Result<Response<UnaryResponse>, Status> {
        let mut resp = Response::new(UnaryResponse {
            greeting: format!("hello {}", req.msg.name),
        });
        if let Some(v) = req.header.get(ECHO_MD_HEADER) {
            resp = resp
                .with_header(ECHO_MD_HEADER, v.clone())
                .with_trailer(ECHO_MD_TRAILER, v.clone());
        }
        Ok(resp)
    }

    async fn server_stream(
        &self,
        req: Request<ServerStreamRequest>,
        stream: ServerStreamServerStream,
    ) -> Result<(), Status> {
        for index in 0..req.msg.count {
            stream.send(&ServerStreamResponse { index }).await?;
        }
        Ok(())
    }

    async fn client_stream(
        &self,
        stream: ClientStreamServerStream,
    ) -> Result<Response<ClientStreamResponse>, Status> {
        let mut sum = 0;
        while let Some(req) = stream.recv().await? {
            sum += req.value;
        }
        Ok(Response::new(ClientStreamResponse { sum }))
    }

    async fn bidi(&self, stream: BidiServerStream) -> Result<(), Status> {
        while let Some(req) = stream.recv().await? {
            stream
                .send(&BidiResponse {
                    echo: format!("echo:{}", req.text),
                })
                .await?;
        }
        Ok(())
    }
}

/// The same service written against the `protoc-gen-tonic` trait and served
/// as a plain tonic `EchoServiceServer` through `Server::register_grpc` — no
/// ws-proto-specific code. Behaviour is identical to [`EchoImpl`] so every
/// client scenario passes against both.
pub struct TonicEchoImpl;

type RespStream<T> =
    std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<T, tonic::Status>> + Send>>;

#[tonic::async_trait]
impl echo_proto::echo::v1::echo_service_server::EchoService for TonicEchoImpl {
    async fn unary(
        &self,
        req: tonic::Request<UnaryRequest>,
    ) -> Result<tonic::Response<UnaryResponse>, tonic::Status> {
        let md = req.metadata().get(ECHO_MD_HEADER).cloned();
        let mut resp = tonic::Response::new(UnaryResponse {
            greeting: format!("hello {}", req.into_inner().name),
        });
        if let Some(v) = md {
            resp.metadata_mut().insert(ECHO_MD_HEADER, v);
        }
        Ok(resp)
    }

    type ServerStreamStream = RespStream<ServerStreamResponse>;

    async fn server_stream(
        &self,
        req: tonic::Request<ServerStreamRequest>,
    ) -> Result<tonic::Response<Self::ServerStreamStream>, tonic::Status> {
        let md = req.metadata().get(ECHO_MD_HEADER).cloned();
        let count = req.into_inner().count;
        let items = (0..count).map(|index| Ok(ServerStreamResponse { index }));
        // tonic has no API for trailers on a successful stream; the documented
        // workaround (grpc/grpc-rust#1370) is to end the stream with an
        // `Err(Status)` whose code is `Ok` and whose metadata becomes the
        // trailers. ws-proto's register_grpc maps that to END(OK) + trailers.
        let tail = md.map(|v| {
            let mut meta = tonic::metadata::MetadataMap::new();
            meta.insert(ECHO_MD_TRAILER, v);
            Err(tonic::Status::with_metadata(tonic::Code::Ok, "", meta))
        });
        let s = tokio_stream::iter(items.chain(tail));
        Ok(tonic::Response::new(Box::pin(s)))
    }

    async fn client_stream(
        &self,
        req: tonic::Request<tonic::Streaming<ClientStreamRequest>>,
    ) -> Result<tonic::Response<ClientStreamResponse>, tonic::Status> {
        let mut inbound = req.into_inner();
        let mut sum = 0;
        while let Some(r) = inbound.message().await? {
            sum += r.value;
        }
        Ok(tonic::Response::new(ClientStreamResponse { sum }))
    }

    type BidiStream = RespStream<BidiResponse>;

    async fn bidi(
        &self,
        req: tonic::Request<tonic::Streaming<BidiRequest>>,
    ) -> Result<tonic::Response<Self::BidiStream>, tonic::Status> {
        use tokio_stream::StreamExt;
        let out = req.into_inner().map(|r| {
            r.map(|req| BidiResponse {
                echo: format!("echo:{}", req.text),
            })
        });
        Ok(tonic::Response::new(Box::pin(out)))
    }
}

/// Which implementation backs the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Impl {
    /// Native ws-proto handler ([`EchoImpl`]).
    Native,
    /// tonic service via `register_grpc` ([`TonicEchoImpl`]).
    TonicBridge,
}

impl Impl {
    /// `WS_PROTO_IMPL=tonic` selects the bridge; anything else the native impl.
    pub fn from_env() -> Self {
        match std::env::var("WS_PROTO_IMPL").as_deref() {
            Ok("tonic") => Impl::TonicBridge,
            _ => Impl::Native,
        }
    }
}

/// A server with the echo service registered. Accepts any Origin (example
/// only; production code should restrict browser clients).
pub fn new_server() -> ws_proto::Server {
    new_server_with(Impl::Native)
}

pub fn new_server_with(which: Impl) -> ws_proto::Server {
    use echo_proto::echo::v1::{
        echo_service_server::EchoServiceServer, echo_service_ws::register_echo_service,
    };
    let server = ws_proto::Server::new(ws_proto::ServerConfig {
        origin: ws_proto::server::OriginPolicy::InsecureSkip,
        ..Default::default()
    });
    match which {
        Impl::Native => register_echo_service(&server, EchoImpl),
        Impl::TonicBridge => server.register_grpc(EchoServiceServer::new(TonicEchoImpl)),
    }
    server
}
