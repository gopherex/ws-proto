//! WebSocket transport for protobuf RPC services — the Rust runtime.
//!
//! Wire-compatible with the Go `wsrpc` package and the TypeScript
//! `@gopherex/ws-proto-transport` package: one WebSocket connection carries
//! binary frames, each a marshaled [`transport::Frame`], multiplexing any
//! number of concurrent RPC streams.
//!
//! Layering, bottom up:
//!
//! 1. [`conn::WsConn`] — a WebSocket connection seen as a `Stream` + `Sink` of
//!    binary messages. The runtime never touches a WebSocket library
//!    directly; adapters live behind cargo features ([`conn::tungstenite`]).
//! 2. [`mux::Mux`] / [`stream::Stream`] — frame multiplexing, per-stream
//!    flow control, metadata, deadlines and status; wire-identical to Go.
//! 3. [`client::Client`] / [`server::Server`] — dial / upgrade + handler
//!    registry; the typed bindings emitted by `protoc-gen-ws-rs` sit on top.

pub mod call;
pub mod client;
pub mod conn;
pub mod error;
#[cfg(feature = "tonic")]
pub mod grpc;
pub mod interceptor;
pub mod mux;
pub mod server;
pub mod stats;
pub mod status;
pub mod stream;

/// The wire envelope (`wsproto.transport.v1`), generated into the
/// `ws-proto-frame` crate from `transport/transport.proto`.
pub use ws_proto_frame::wsproto::transport::v1 as transport;

pub use call::{CallOptions, Request, Response};
pub use client::{Client, ClientConfig, Reconnect};
pub use error::Error;
pub use interceptor::{Interceptor, MethodSpec, StreamKind, StreamingConn};
pub use mux::{Keepalive, Mux, MuxConfig};
pub use server::{Server, ServerConfig};
pub use stats::ServerStats;
pub use status::{Code, Status};
pub use stream::{Metadata, Stream};

/// WebSocket subprotocol negotiated on upgrade (`Sec-WebSocket-Protocol`).
pub const SUBPROTOCOL: &str = "wsrpc.v1";

/// Default per-stream flow-control window in bytes, matching Go and TS.
pub const DEFAULT_INITIAL_WINDOW: u32 = 256 * 1024;

#[cfg(test)]
mod tests {
    use super::transport::{Frame, Kind, Status};
    use prost::Message;

    fn sample() -> Frame {
        Frame {
            stream_id: 1,
            kind: Kind::Open as i32,
            headers: [("a".to_owned(), "b".to_owned())].into_iter().collect(),
            payload: bytes::Bytes::from_static(&[1, 2, 3]),
            status: Some(Status {
                code: 5,
                message: "nf".to_owned(),
                details: vec![],
            }),
            method: "/echo.v1.EchoService/Unary".to_owned(),
            window: 7,
        }
    }

    #[test]
    fn frame_roundtrip() {
        let f = sample();
        assert_eq!(Frame::decode(f.encode_to_vec().as_slice()).unwrap(), f);
    }

    /// Bytes produced by the Go `transport` package (deterministic marshal)
    /// for the same frame. Guards wire compatibility of the envelope.
    #[test]
    fn frame_matches_go_golden() {
        let golden = "080110011a060a016112016222030102032a06080512026e66321a2f6563686f2e76312e4563686f536572766963652f556e6172794007";
        assert_eq!(hex(&sample().encode_to_vec()), golden);
        assert_eq!(
            Frame::decode(hex_decode(golden).as_slice()).unwrap(),
            sample()
        );
    }

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    fn hex_decode(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }
}
