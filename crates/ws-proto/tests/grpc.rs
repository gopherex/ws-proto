//! `Server::register_grpc`: tonic services served over ws-proto unchanged.
#![cfg(feature = "tonic")]

use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{BufMut, Bytes, BytesMut};
use http::{HeaderMap, HeaderValue, Request, Response};
use http_body_util::{combinators::BoxBody, BodyExt, Full, StreamBody};
use prost::Message;
use tonic_health::{
    pb::{
        health_check_response::ServingStatus as PbStatus, HealthCheckRequest, HealthCheckResponse,
    },
    ServingStatus,
};
use tower_service::Service;
use ws_proto::{
    conn::pipe, grpc::RpcStatus, Client, ClientConfig, Code, Metadata, Server, ServerConfig,
};

fn pair(server: Server) -> Client {
    let (a, b) = pipe();
    tokio::spawn(async move { server.serve_conn(a).await });
    Client::from_conn(b, ClientConfig::default())
}

#[tokio::test]
async fn tonic_health_unary_and_server_stream() {
    let (reporter, health) = tonic_health::server::health_reporter();
    reporter
        .set_service_status("echo.v1.EchoService", ServingStatus::Serving)
        .await;
    let server = Server::new(ServerConfig::default());
    server.register_grpc(health);
    let c = pair(server);

    // unary: Check
    let s = c
        .new_stream("/grpc.health.v1.Health/Check", Metadata::new(), None)
        .await
        .unwrap();
    s.send(&HealthCheckRequest {
        service: "echo.v1.EchoService".into(),
    })
    .await
    .unwrap();
    s.close_send().await.unwrap();
    let r: HealthCheckResponse = s.recv().await.unwrap().unwrap();
    assert_eq!(r.status, PbStatus::Serving as i32);
    assert_eq!(s.recv_raw().await.unwrap(), None, "clean END");

    // unary: unknown service -> tonic's own NotFound status via trailers
    let s = c
        .new_stream("/grpc.health.v1.Health/Check", Metadata::new(), None)
        .await
        .unwrap();
    s.send(&HealthCheckRequest {
        service: "nope".into(),
    })
    .await
    .unwrap();
    s.close_send().await.unwrap();
    let err = s.recv_raw().await.unwrap_err();
    assert_eq!(err.code, Code::NotFound);

    // server streaming: Watch, status change pushes a second message
    let s = c
        .new_stream("/grpc.health.v1.Health/Watch", Metadata::new(), None)
        .await
        .unwrap();
    s.send(&HealthCheckRequest {
        service: "echo.v1.EchoService".into(),
    })
    .await
    .unwrap();
    s.close_send().await.unwrap();
    let r: HealthCheckResponse = s.recv().await.unwrap().unwrap();
    assert_eq!(r.status, PbStatus::Serving as i32);
    reporter
        .set_service_status("echo.v1.EchoService", ServingStatus::NotServing)
        .await;
    let r: HealthCheckResponse = s.recv().await.unwrap().unwrap();
    assert_eq!(r.status, PbStatus::NotServing as i32);

    // unknown method under the service prefix: tonic answers Unimplemented itself
    let s = c
        .new_stream("/grpc.health.v1.Health/Nope", Metadata::new(), None)
        .await
        .unwrap();
    s.close_send().await.unwrap();
    assert_eq!(s.recv_raw().await.unwrap_err().code, Code::Unimplemented);
}

#[tokio::test]
async fn tonic_interceptor_sees_open_headers_as_metadata() {
    let (reporter, health) = tonic_health::server::health_reporter();
    reporter
        .set_service_status("x", ServingStatus::Serving)
        .await;
    let guarded =
        tonic::service::interceptor::InterceptedService::new(health, |req: tonic::Request<()>| {
            match req.metadata().get("authorization") {
                Some(v) if v == "Bearer ok" => Ok(req),
                _ => Err(tonic::Status::unauthenticated("no auth")),
            }
        });
    let server = Server::new(ServerConfig::default());
    server.register_grpc(guarded);
    let c = pair(server);

    let call = |md: Metadata| async {
        let s = c
            .new_stream("/grpc.health.v1.Health/Check", md, None)
            .await
            .unwrap();
        s.send(&HealthCheckRequest {
            service: "x".into(),
        })
        .await
        .unwrap();
        s.close_send().await.unwrap();
        s.recv::<HealthCheckResponse>().await
    };
    let err = call(Metadata::new()).await.unwrap_err();
    assert_eq!(
        (err.code, err.message.as_str()),
        (Code::Unauthenticated, "no auth")
    );
    let ok = call([("authorization".to_owned(), "Bearer ok".to_owned())].into())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ok.status, PbStatus::Serving as i32);
}

/// A hand-rolled gRPC service answering with a leading header, one message,
/// an error status with details, and an extra trailer — exercises every
/// mapping in `register_grpc` independently of tonic's codegen.
#[derive(Clone)]
struct Raw;

impl tonic::server::NamedService for Raw {
    const NAME: &'static str = "test.Raw";
}

impl<B: Send + 'static> Service<Request<B>> for Raw {
    type Response = Response<BoxBody<Bytes, Infallible>>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let path = req.uri().path().to_owned();
        let saw_timeout = req.headers().get("grpc-timeout").is_some();
        Box::pin(async move {
            // one framed message: the request path as a prost string field
            let msg = prost_types::Value {
                kind: Some(prost_types::value::Kind::StringValue(path)),
            }
            .encode_to_vec();
            let mut data = BytesMut::new();
            data.put_u8(0);
            data.put_u32(msg.len() as u32);
            data.extend_from_slice(&msg);

            let details = RpcStatus {
                code: 5,
                message: "nf".into(),
                details: vec![prost_types::Any {
                    type_url: "type.googleapis.com/x.Detail".into(),
                    value: vec![1, 2, 3],
                }],
            };
            let st = tonic::Status::with_details(
                tonic::Code::NotFound,
                "not found: path",
                Bytes::from(details.encode_to_vec()),
            );
            let mut trailers = HeaderMap::new();
            st.add_header(&mut trailers).unwrap();
            trailers.insert("x-trail", HeaderValue::from_static("t"));
            trailers.insert(
                "x-saw-timeout",
                HeaderValue::from_static(if saw_timeout { "yes" } else { "no" }),
            );

            let frames = futures_util::stream::iter(vec![
                Ok::<_, Infallible>(http_body::Frame::data(data.freeze())),
                Ok(http_body::Frame::trailers(trailers)),
            ]);
            let body = BoxBody::new(StreamBody::new(frames));
            let mut resp = Response::new(body);
            resp.headers_mut()
                .insert("content-type", HeaderValue::from_static("application/grpc"));
            resp.headers_mut()
                .insert("x-lead", HeaderValue::from_static("l"));
            Ok(resp)
        })
    }
}

#[tokio::test]
async fn raw_grpc_response_maps_header_body_status_details_trailers() {
    let server = Server::new(ServerConfig::default());
    server.register_grpc(Raw);
    let c = pair(server);
    let s = c
        .new_stream(
            "/test.Raw/Any",
            Metadata::new(),
            Some(std::time::Duration::from_secs(3)),
        )
        .await
        .unwrap();
    s.close_send().await.unwrap();

    let msg: prost_types::Value = s.recv().await.unwrap().unwrap();
    assert_eq!(
        msg.kind,
        Some(prost_types::value::Kind::StringValue(
            "/test.Raw/Any".into()
        ))
    );
    assert_eq!(s.header().get("x-lead").map(String::as_str), Some("l"));

    let err = s.recv_raw().await.unwrap_err();
    assert_eq!(err.code, Code::NotFound);
    assert_eq!(err.message, "not found: path");
    assert_eq!(err.details.len(), 1);
    let any = prost_types::Any::decode(err.details[0].clone()).unwrap();
    assert_eq!(any.type_url, "type.googleapis.com/x.Detail");
    assert_eq!(any.value, vec![1, 2, 3]);
    assert_eq!(s.trailer().get("x-trail").map(String::as_str), Some("t"));
    assert_eq!(
        s.trailer().get("x-saw-timeout").map(String::as_str),
        Some("yes"),
        "ws-timeout-ms became grpc-timeout"
    );
    let _ = Full::new(Bytes::new()).boxed(); // keep http-body-util in scope for the test crate
}

// ---- ws-proto layers around a gRPC service, and tower layers inside it ----

#[tokio::test]
async fn ws_middleware_and_stats_wrap_grpc_streams() {
    use std::sync::{Arc, Mutex};
    let (reporter, health) = tonic_health::server::health_reporter();
    reporter
        .set_service_status("x", ServingStatus::Serving)
        .await;
    let server = Server::new(ServerConfig::default());
    server.register_grpc(health);

    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let s2 = Arc::clone(&seen);
    server.add_middleware(move |next| {
        let s2 = Arc::clone(&s2);
        Arc::new(move |st| {
            let next = Arc::clone(&next);
            let s2 = Arc::clone(&s2);
            Box::pin(async move {
                s2.lock().unwrap().push(format!("mw:{}", st.method()));
                if st.header().contains_key("block") {
                    return Err(ws_proto::Status::new(
                        Code::PermissionDenied,
                        "blocked by ws middleware",
                    ));
                }
                next(st).await
            })
        })
    });
    #[derive(Default)]
    struct Rec(Mutex<Vec<String>>);
    impl ws_proto::ServerStats for Rec {
        fn stream_ended(&self, method: &str, code: Code) {
            self.0.lock().unwrap().push(format!("{method}:{code:?}"));
        }
    }
    let rec = Arc::new(Rec::default());
    server.set_stats(rec.clone());
    let c = pair(server);

    let call = |md: Metadata| async {
        let s = c
            .new_stream("/grpc.health.v1.Health/Check", md, None)
            .await
            .unwrap();
        s.send(&HealthCheckRequest {
            service: "x".into(),
        })
        .await
        .unwrap();
        s.close_send().await.unwrap();
        s.recv::<HealthCheckResponse>().await
    };
    assert!(call(Metadata::new()).await.is_ok());
    let err = call([("block".to_owned(), "1".to_owned())].into())
        .await
        .unwrap_err();
    assert_eq!(err.code, Code::PermissionDenied);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert_eq!(
        seen.lock().unwrap().len(),
        2,
        "middleware ran for both gRPC calls"
    );
    assert_eq!(
        *rec.0.lock().unwrap(),
        [
            "/grpc.health.v1.Health/Check:Ok",
            "/grpc.health.v1.Health/Check:PermissionDenied"
        ]
    );
}

#[tokio::test]
async fn tower_layer_over_tonic_service_and_ws_deadline() {
    use tower::ServiceBuilder;
    let (reporter, health) = tonic_health::server::health_reporter();
    reporter
        .set_service_status("x", ServingStatus::Serving)
        .await;

    // A tower layer: inject a response header for every call.
    let layered = ServiceBuilder::new()
        .map_response(|mut r: http::Response<tonic::body::Body>| {
            r.headers_mut()
                .insert("x-layer", HeaderValue::from_static("yes"));
            r
        })
        .service(health.clone());
    // tower layers erase NamedService; give it back.
    #[derive(Clone)]
    struct Named<S>(S);
    impl<S> tonic::server::NamedService for Named<S> {
        const NAME: &'static str = "grpc.health.v1.Health";
    }
    impl<S, R> Service<R> for Named<S>
    where
        S: Service<R>,
    {
        type Response = S::Response;
        type Error = S::Error;
        type Future = S::Future;
        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
            self.0.poll_ready(cx)
        }
        fn call(&mut self, req: R) -> S::Future {
            self.0.call(req)
        }
    }
    let server = Server::new(ServerConfig::default());
    server.register_grpc(Named(layered));
    let c = pair(server);

    let s = c
        .new_stream("/grpc.health.v1.Health/Check", Metadata::new(), None)
        .await
        .unwrap();
    s.send(&HealthCheckRequest {
        service: "x".into(),
    })
    .await
    .unwrap();
    s.close_send().await.unwrap();
    s.recv::<HealthCheckResponse>().await.unwrap().unwrap();
    assert_eq!(s.header().get("x-layer").map(String::as_str), Some("yes"));

    // ws-timeout-ms deadline on a tonic stream that never completes: Watch
    // with no status change blocks; ws-proto ends it with DeadlineExceeded.
    let s = c
        .new_stream(
            "/grpc.health.v1.Health/Watch",
            Metadata::new(),
            Some(std::time::Duration::from_millis(100)),
        )
        .await
        .unwrap();
    s.send(&HealthCheckRequest {
        service: "x".into(),
    })
    .await
    .unwrap();
    s.close_send().await.unwrap();
    s.recv::<HealthCheckResponse>().await.unwrap().unwrap(); // initial status
    let err = tokio::time::timeout(std::time::Duration::from_secs(3), s.recv_raw())
        .await
        .expect("deadline ends the stream")
        .unwrap_err();
    assert_eq!(err.code, Code::DeadlineExceeded);
}
