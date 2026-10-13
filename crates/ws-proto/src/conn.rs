//! The WebSocket connection abstraction the runtime is built on.
//!
//! `wsrpc.v1` needs little from a WebSocket: deliver the peer's **binary**
//! messages as whole byte blobs, send ours, and exchange ping/pong for
//! keepalive. Close handshake, masking, TLS stay the library's business. So
//! the runtime consumes a [`WsConn`]: a `Stream` of incoming [`Message`]s and
//! a `Sink` for outgoing ones, and never names a WebSocket crate.
//!
//! Adapters for concrete libraries live in submodules behind cargo features;
//! [`tungstenite`] (default) covers `tokio-tungstenite` and therefore
//! `axum::extract::ws`, `warp::ws`, `actix-ws` and friends. Anything that
//! already implements both traits with the right item types is a `WsConn`
//! for free (blanket impl).

use bytes::Bytes;
use futures_core::Stream;
use futures_sink::Sink;

use crate::Error;

/// What crosses a [`WsConn`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// One complete binary WebSocket message: one marshaled
    /// [`Frame`](crate::transport::Frame).
    Binary(Bytes),
    /// Keepalive probe. Adapters whose library answers pings itself never
    /// surface inbound pings; the mux answers any that do reach it.
    Ping(Bytes),
    /// Answer to a ping; drives the keepalive timeout.
    Pong(Bytes),
}

/// A WebSocket connection as the runtime sees it.
///
/// Contract for implementors:
/// - Text data messages from the peer are a protocol violation; yield
///   [`Error::Protocol`].
/// - A clean close from the peer ends the stream (`None`); an abnormal one
///   yields [`Error::Transport`].
pub trait WsConn:
    Stream<Item = Result<Message, Error>> + Sink<Message, Error = Error> + Send + Unpin
{
}

impl<T> WsConn for T where
    T: Stream<Item = Result<Message, Error>> + Sink<Message, Error = Error> + Send + Unpin
{
}

/// An in-memory [`WsConn`] pair: what one end sends, the other receives.
/// The in-process analog of a WebSocket, for tests and embedding a server and
/// client in one binary (Go `wsrpc.NewPipe`). Dropping one end closes the
/// other's stream.
pub fn pipe() -> (Pipe, Pipe) {
    let (a_tx, a_rx) = tokio::sync::mpsc::channel(PIPE_BUFFER);
    let (b_tx, b_rx) = tokio::sync::mpsc::channel(PIPE_BUFFER);
    (
        Pipe {
            tx: tokio_util::sync::PollSender::new(a_tx),
            rx: b_rx,
        },
        Pipe {
            tx: tokio_util::sync::PollSender::new(b_tx),
            rx: a_rx,
        },
    )
}

const PIPE_BUFFER: usize = 16;

/// One end of [`pipe`].
pub struct Pipe {
    tx: tokio_util::sync::PollSender<Message>,
    rx: tokio::sync::mpsc::Receiver<Message>,
}

impl Stream for Pipe {
    type Item = Result<Message, Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx).map(|o| o.map(Ok))
    }
}

impl Sink<Message> for Pipe {
    type Error = Error;

    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Error>> {
        std::pin::Pin::new(&mut self.tx)
            .poll_ready(cx)
            .map_err(|_| pipe_closed())
    }

    fn start_send(mut self: std::pin::Pin<&mut Self>, item: Message) -> Result<(), Error> {
        std::pin::Pin::new(&mut self.tx)
            .start_send(item)
            .map_err(|_| pipe_closed())
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Error>> {
        std::pin::Pin::new(&mut self.tx)
            .poll_flush(cx)
            .map_err(|_| pipe_closed())
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Error>> {
        std::pin::Pin::new(&mut self.tx)
            .poll_close(cx)
            .map_err(|_| pipe_closed())
    }
}

fn pipe_closed() -> Error {
    Error::transport(std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        "pipe closed",
    ))
}

#[cfg(feature = "tungstenite")]
pub mod tungstenite {
    //! [`WsConn`](super::WsConn) adapter for `tokio-tungstenite`.

    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    use futures_core::Stream;
    use futures_sink::Sink;
    use tokio::io::{AsyncRead, AsyncWrite};
    use tokio_tungstenite::{
        tungstenite::{self, Message},
        WebSocketStream,
    };

    use super::Message as WsMessage;
    use crate::Error;

    /// Wraps a [`WebSocketStream`] into a [`WsConn`](super::WsConn).
    ///
    /// Binary messages pass through; inbound pings are answered by tungstenite
    /// itself and skipped here, pongs surface for keepalive; a `Close` ends
    /// the stream; `Text` is reported as [`Error::Protocol`].
    pub struct TungsteniteConn<S> {
        inner: WebSocketStream<S>,
    }

    impl<S> TungsteniteConn<S> {
        pub fn new(inner: WebSocketStream<S>) -> Self {
            Self { inner }
        }

        pub fn into_inner(self) -> WebSocketStream<S> {
            self.inner
        }
    }

    impl<S> From<WebSocketStream<S>> for TungsteniteConn<S> {
        fn from(inner: WebSocketStream<S>) -> Self {
            Self::new(inner)
        }
    }

    impl<S> Stream for TungsteniteConn<S>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        type Item = Result<WsMessage, Error>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            loop {
                let msg = match Pin::new(&mut self.inner).poll_next(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(None) => return Poll::Ready(None),
                    Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(map_err(e)))),
                    Poll::Ready(Some(Ok(msg))) => msg,
                };
                match msg {
                    Message::Binary(b) => return Poll::Ready(Some(Ok(WsMessage::Binary(b)))),
                    Message::Pong(b) => return Poll::Ready(Some(Ok(WsMessage::Pong(b)))),
                    Message::Close(_) => return Poll::Ready(None),
                    Message::Ping(_) | Message::Frame(_) => continue,
                    Message::Text(_) => {
                        return Poll::Ready(Some(Err(Error::Protocol(
                            "text message on a wsrpc.v1 connection".to_owned(),
                        ))))
                    }
                }
            }
        }
    }

    impl<S> Sink<WsMessage> for TungsteniteConn<S>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        type Error = Error;

        fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
            Pin::new(&mut self.inner).poll_ready(cx).map_err(map_err)
        }

        fn start_send(mut self: Pin<&mut Self>, item: WsMessage) -> Result<(), Error> {
            let msg = match item {
                WsMessage::Binary(b) => Message::Binary(b),
                WsMessage::Ping(b) => Message::Ping(b),
                WsMessage::Pong(b) => Message::Pong(b),
            };
            Pin::new(&mut self.inner).start_send(msg).map_err(map_err)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
            Pin::new(&mut self.inner).poll_flush(cx).map_err(map_err)
        }

        fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
            Pin::new(&mut self.inner).poll_close(cx).map_err(map_err)
        }
    }

    fn map_err(e: tungstenite::Error) -> Error {
        match e {
            // Peer went away without a close handshake: still "the stream
            // ended", callers decide whether that's abnormal.
            tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed => {
                Error::transport(e)
            }
            tungstenite::Error::Protocol(_) | tungstenite::Error::Utf8(_) => {
                Error::Protocol(e.to_string())
            }
            other => Error::transport(other),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::{
            conn::WsConn,
            transport::{Frame, Kind},
        };
        use bytes::Bytes;
        use futures_util::{SinkExt, StreamExt};
        use prost::Message as _;
        use tokio_tungstenite::tungstenite::protocol::Role;

        type Duplex = tokio::io::DuplexStream;

        /// Two ends of an in-memory WebSocket, no handshake, no network.
        async fn loopback() -> (TungsteniteConn<Duplex>, TungsteniteConn<Duplex>) {
            let (a, b) = tokio::io::duplex(64 * 1024);
            let client = WebSocketStream::from_raw_socket(a, Role::Client, None).await;
            let server = WebSocketStream::from_raw_socket(b, Role::Server, None).await;
            (TungsteniteConn::new(client), TungsteniteConn::new(server))
        }

        #[tokio::test]
        async fn binary_frame_roundtrips_through_adapter() {
            let (mut client, mut server) = loopback().await;
            fn assert_ws_conn(_: &impl WsConn) {}
            assert_ws_conn(&client);
            let frame = Frame {
                stream_id: 3,
                kind: Kind::Msg as i32,
                payload: Bytes::from_static(b"hello"),
                ..Default::default()
            };

            client
                .send(WsMessage::Binary(Bytes::from(frame.encode_to_vec())))
                .await
                .unwrap();
            let got = match server.next().await.unwrap().unwrap() {
                WsMessage::Binary(b) => b,
                other => panic!("expected binary, got {other:?}"),
            };
            assert_eq!(Frame::decode(got).unwrap(), frame);
        }

        #[tokio::test]
        async fn text_message_is_protocol_error() {
            let (client, mut server) = loopback().await;
            // Bypass the adapter on the sending side to inject a text frame.
            client
                .into_inner()
                .send(Message::Text("nope".into()))
                .await
                .unwrap();
            match server.next().await.unwrap() {
                Err(Error::Protocol(_)) => {}
                other => panic!("expected protocol error, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn ping_is_answered_by_library_and_pong_surfaces() {
            let (mut client, mut server) = loopback().await;
            client
                .send(WsMessage::Ping(Bytes::from_static(b"k")))
                .await
                .unwrap();
            // The server side must be polled for tungstenite to queue the pong;
            // it has nothing else to deliver, so poll with a timeout.
            let _ = tokio::time::timeout(std::time::Duration::from_millis(50), server.next()).await;
            server.flush().await.unwrap();
            let got = tokio::time::timeout(std::time::Duration::from_secs(1), client.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(got, WsMessage::Pong(Bytes::from_static(b"k")));
        }

        #[tokio::test]
        async fn close_ends_stream() {
            let (mut client, mut server) = loopback().await;
            client.close().await.unwrap();
            assert!(server.next().await.is_none());
        }
    }
}
