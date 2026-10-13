//! Standalone ws-proto server used by the cross-language tests.
//!
//! Readiness contract (same as the Go `example/server`):
//! - listen address from `WS_PROTO_TEST_ADDR` (default `127.0.0.1:8910`);
//! - once bound, prints `LISTENING <addr>` to stdout;
//! - `WS_PROTO_IMPL=tonic` serves the tonic implementation through the bridge.

use std::io::Write;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let addr = std::env::var("WS_PROTO_TEST_ADDR").unwrap_or_else(|_| "127.0.0.1:8910".to_owned());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("LISTENING {}", listener.local_addr()?);
    std::io::stdout().flush()?;
    let which = ws_proto_example_server::Impl::from_env();
    ws_proto_example_server::new_server_with(which)
        .serve_listener(listener)
        .await
}
