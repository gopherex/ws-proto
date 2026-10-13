//! Serve gRPC services over ws-proto with no generated glue — the Rust
//! counterpart of Go's `wsrpc.GRPCRegistrar`.
//!
//! A tonic server (`EchoServiceServer::new(impl)`, with or without tower
//! layers / `tonic::service::interceptor`) is a
//! `tower_service::Service<http::Request<B>>` that routes by path and speaks
//! gRPC-over-HTTP/2 framing. [`Server::register_grpc`] registers it under its
//! `NamedService::NAME` prefix and, per ws-proto stream, plays the HTTP side:
//!
//! | ws-proto | gRPC/HTTP |
//! |---|---|
//! | OPEN method `/pkg.Svc/Method` | `POST /pkg.Svc/Method`, `content-type: application/grpc` |
//! | OPEN headers (`ws-timeout-ms`) | request headers / incoming metadata (`grpc-timeout`) |
//! | MSG … HALF_CLOSE | length-prefixed request body … end of body |
//! | HEADER frame | response headers (minus `content-type`, `grpc-*`) |
//! | MSG … | length-prefixed response body |
//! | END status + trailers | `grpc-status`, `grpc-message`, `grpc-status-details-bin`, other trailers |
//!
//! So handler code, interceptors, health, reflection — anything that works
//! under `tonic::transport::Server` — works here unchanged, and status details
//! (`google.rpc.Status.details`, a list of `Any`) cross the wire 1:1 with Go.

use std::{
    convert::Infallible,
    future::poll_fn,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use http::{header::HeaderName, HeaderMap, HeaderValue, Request, Response};
use http_body::{Body, Frame};
use http_body_util::BodyExt;
use prost::Message;
use tonic::server::NamedService;
use tower_service::Service;

use crate::{
    server::Server,
    status::{Code, Status},
    stream::{Metadata, Stream},
};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

impl Server {
    /// Registers a gRPC service (anything tonic can serve) under its
    /// `/<NamedService::NAME>/` route prefix. The service is cloned per
    /// stream, as tower services are.
    pub fn register_grpc<S, B>(&self, svc: S)
    where
        S: Service<Request<RequestBody>, Response = Response<B>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
        B: Body<Data = Bytes> + Send + 'static,
        B::Error: Into<BoxError> + Send,
    {
        self.register_prefix(format!("/{}/", S::NAME), move |stream| {
            let svc = svc.clone();
            async move { serve_grpc(svc, stream).await }
        });
    }
}

async fn serve_grpc<S, B>(mut svc: S, stream: Stream) -> Result<(), Status>
where
    S: Service<Request<RequestBody>, Response = Response<B>, Error = Infallible>,
    B: Body<Data = Bytes>,
    B::Error: Into<BoxError>,
{
    let req = build_request(&stream)?;
    poll_fn(|cx| svc.poll_ready(cx))
        .await
        .unwrap_or_else(|e| match e {});
    let resp = svc.call(req).await.unwrap_or_else(|e| match e {});
    let (parts, body) = resp.into_parts();

    if !parts.status.is_success() {
        return Err(Status::new(
            Code::Internal,
            format!("grpc: HTTP {}", parts.status),
        ));
    }
    // Trailers-only response: the status rides in the headers.
    if parts.headers.contains_key("grpc-status") {
        return finish(&parts.headers, &stream);
    }
    let header = metadata_from_headers(&parts.headers);
    if !header.is_empty() {
        stream.send_header(header).await?;
    }

    let mut body = std::pin::pin!(body);
    let mut buf = BytesMut::new();
    while let Some(frame) = body.frame().await {
        let frame =
            frame.map_err(|e| Status::new(Code::Internal, format!("grpc: body: {}", e.into())))?;
        match frame.into_data() {
            Ok(data) => {
                buf.put(data);
                while let Some(msg) = take_message(&mut buf)? {
                    stream.send_raw(msg).await?;
                }
            }
            Err(frame) => {
                if let Ok(trailers) = frame.into_trailers() {
                    return finish(&trailers, &stream);
                }
            }
        }
    }
    if !buf.is_empty() {
        return Err(Status::new(Code::Internal, "grpc: truncated message frame"));
    }
    Ok(())
}

fn build_request(stream: &Stream) -> Result<Request<RequestBody>, Status> {
    let mut req = Request::builder()
        .method(http::Method::POST)
        .uri(stream.method())
        .version(http::Version::HTTP_2)
        .body(RequestBody::new(stream.clone()))
        .map_err(|e| Status::new(Code::Internal, format!("grpc: request: {e}")))?;
    let h = req.headers_mut();
    h.insert("content-type", HeaderValue::from_static("application/grpc"));
    h.insert("te", HeaderValue::from_static("trailers"));
    for (k, v) in stream.header() {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(k.as_bytes()),
            HeaderValue::from_str(&v),
        ) {
            h.append(name, value);
        }
    }
    if let Some(t) = stream.timeout() {
        let ms = t.as_millis();
        // grpc-timeout: up to 8 digits + unit.
        let value = if ms < 100_000_000 {
            format!("{ms}m")
        } else {
            format!("{}S", t.as_secs().min(99_999_999))
        };
        if let Ok(v) = HeaderValue::from_str(&value) {
            h.insert("grpc-timeout", v);
        }
    }
    Ok(req)
}

/// Response headers → leading metadata, dropping transport-level ones.
fn metadata_from_headers(headers: &HeaderMap) -> Metadata {
    let mut out = Metadata::new();
    for (k, v) in headers {
        let name = k.as_str();
        if matches!(
            name,
            "content-type" | "te" | "trailer" | "date" | "content-length"
        ) || name.starts_with("grpc-")
        {
            continue;
        }
        let Ok(v) = v.to_str() else { continue };
        out.entry(name.to_owned())
            .and_modify(|e| {
                e.push(',');
                e.push_str(v);
            })
            .or_insert_with(|| v.to_owned());
    }
    out
}

/// Pops one gRPC length-prefixed message from `buf`, if complete.
fn take_message(buf: &mut BytesMut) -> Result<Option<Bytes>, Status> {
    if buf.len() < 5 {
        return Ok(None);
    }
    let compressed = buf[0];
    let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    if compressed != 0 {
        return Err(Status::new(
            Code::Internal,
            "grpc: compressed messages are not supported over ws-proto",
        ));
    }
    if buf.len() < 5 + len {
        return Ok(None);
    }
    buf.advance(5);
    Ok(Some(buf.split_to(len).freeze()))
}

/// Trailers → END: `grpc-status`/`grpc-message`/`grpc-status-details-bin`
/// become the status (details as the `Any` list from `google.rpc.Status`);
/// everything else becomes trailer metadata.
fn finish(trailers: &HeaderMap, stream: &Stream) -> Result<(), Status> {
    let md = metadata_from_headers(trailers);
    if !md.is_empty() {
        stream.set_trailer(md);
    }
    let Some(st) = tonic::Status::from_header_map(trailers) else {
        return Ok(());
    };
    let code = Code::from_i32(st.code() as i32);
    if code == Code::Ok {
        return Ok(());
    }
    let details = RpcStatus::decode(st.details())
        .map(|rs| {
            rs.details
                .iter()
                .map(|a| Bytes::from(a.encode_to_vec()))
                .collect()
        })
        .unwrap_or_default();
    Err(Status {
        code,
        message: st.message().to_owned(),
        details,
    })
}

/// `google.rpc.Status`, the payload of `grpc-status-details-bin`.
#[derive(Clone, PartialEq, Message)]
pub struct RpcStatus {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub message: String,
    #[prost(message, repeated, tag = "3")]
    pub details: Vec<prost_types::Any>,
}

type RecvFuture = Pin<Box<dyn std::future::Future<Output = Result<Option<Bytes>, Status>> + Send>>;

/// The gRPC request body: ws-proto MSG payloads re-framed with the 5-byte
/// gRPC prefix, ending at the client's half-close.
pub struct RequestBody {
    stream: Stream,
    pending: Option<RecvFuture>,
    done: bool,
}

impl RequestBody {
    fn new(stream: Stream) -> Self {
        Self {
            stream,
            pending: None,
            done: false,
        }
    }
}

impl Body for RequestBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        if self.done {
            return Poll::Ready(None);
        }
        if self.pending.is_none() {
            let s = self.stream.clone();
            self.pending = Some(Box::pin(async move { s.recv_raw().await }));
        }
        let res = match self.pending.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(r) => r,
        };
        self.pending = None;
        match res {
            Ok(Some(payload)) => {
                let mut buf = BytesMut::with_capacity(5 + payload.len());
                buf.put_u8(0);
                buf.put_u32(payload.len() as u32);
                buf.extend_from_slice(&payload);
                Poll::Ready(Some(Ok(Frame::data(buf.freeze()))))
            }
            Ok(None) => {
                self.done = true;
                Poll::Ready(None)
            }
            Err(st) => {
                self.done = true;
                Poll::Ready(Some(Err(st)))
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.done
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn take_message_handles_partial_and_multiple() {
        let mut buf = BytesMut::new();
        buf.put_u8(0);
        buf.put_u32(3);
        buf.extend_from_slice(b"abc");
        buf.put_u8(0);
        buf.put_u32(2);
        buf.extend_from_slice(b"d");
        assert_eq!(
            take_message(&mut buf).unwrap(),
            Some(Bytes::from_static(b"abc"))
        );
        assert_eq!(
            take_message(&mut buf).unwrap(),
            None,
            "second message incomplete"
        );
        buf.extend_from_slice(b"e");
        assert_eq!(
            take_message(&mut buf).unwrap(),
            Some(Bytes::from_static(b"de"))
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn compressed_flag_rejected() {
        let mut buf = BytesMut::new();
        buf.put_u8(1);
        buf.put_u32(0);
        assert_eq!(take_message(&mut buf).unwrap_err().code, Code::Internal);
    }

    #[test]
    fn metadata_filters_transport_headers() {
        let mut h = HeaderMap::new();
        h.insert("content-type", "application/grpc".parse().unwrap());
        h.insert("grpc-encoding", "identity".parse().unwrap());
        h.append("x-a", "1".parse().unwrap());
        h.append("x-a", "2".parse().unwrap());
        let md = metadata_from_headers(&h);
        assert_eq!(md.len(), 1);
        assert_eq!(md.get("x-a").map(String::as_str), Some("1,2"));
    }
}
