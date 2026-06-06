# Plan 1 — Wire Contract + Go Runtime (`transport.proto` + `wsrpc/`)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the shared WebSocket wire contract (`transport.proto` → `Frame` envelope) and a complete, tested Go runtime (`wsrpc/`) that multiplexes all four RPC stream kinds over one connection.

**Architecture:** A `Frame` protobuf message is the wire unit. A `Mux` reads/writes frames over an abstract `frameConn` (implemented by both an in-memory pipe for tests and a `coder/websocket` connection). Per-RPC state lives in an untyped `Stream` (works with `proto.Message`). A `Server` is an `http.Handler` that upgrades + dispatches `OPEN` frames to registered handlers by method name; a `ClientConn` dials and allocates client streams. Typed wrappers are deferred to the generator (Plan 2) — this runtime is intentionally untyped.

**Tech Stack:** Go 1.25, `google.golang.org/protobuf`, `github.com/coder/websocket`, `google.golang.org/grpc` (codes only, for the bridge in Plan 2). Proto codegen via `easyp` (config mirrors `protoc-gen-go-ogen/easyp.yaml`). No git branches/worktrees — commit on the current branch. Commits carry no co-author trailer.

**Constraints from user:** Single current git branch only (never create branches/worktrees). Commit messages must NOT contain a `Co-Authored-By` trailer.

---

## File Structure

| Path | Responsibility |
|---|---|
| `go.mod` | Module `github.com/gopherex/ws-proto`, go 1.25, deps |
| `easyp.yaml` | easyp lint + generate config for `transport/` |
| `transport/transport.proto` | `Frame` envelope + `Kind`/`Status` |
| `transport/options.proto` | `(ws.service)` / `(ws.method)` options (minimal) |
| `transport/transport.pb.go` | Generated (committed) |
| `transport/options.pb.go` | Generated (committed) |
| `wsrpc/status.go` | `Status` type, codes mapping, `Errorf`/`FromError` |
| `wsrpc/codec.go` | `frameConn` interface + `Marshal`/`Unmarshal` of `Frame` to bytes |
| `wsrpc/pipe.go` | In-memory `frameConn` pair for tests |
| `wsrpc/wsconn.go` | `coder/websocket`-backed `frameConn` |
| `wsrpc/mux.go` | `Mux`: frame router, stream registry, read/write pumps |
| `wsrpc/stream.go` | `Stream`: untyped per-RPC API (`Send`/`Recv`/`CloseSend`/headers) |
| `wsrpc/server.go` | `Server` (`http.Handler`): upgrade + dispatch by method |
| `wsrpc/client.go` | `Dial` + `ClientConn.NewStream` |
| `wsrpc/Makefile` or root `Makefile` | `gen`, `test` targets |

---

## Task 1: Bootstrap module + tooling

**Files:**
- Create: `go.mod`, `Makefile`, `.gitignore` (append)

- [ ] **Step 1: Init module**

Run:
```bash
cd /home/yaroher/devel/github/gopherex/ws-proto
go mod init github.com/gopherex/ws-proto
```
Then edit `go.mod` so the go directive reads `go 1.25`.

- [ ] **Step 2: Add runtime deps**

Run:
```bash
go get google.golang.org/protobuf@latest
go get github.com/coder/websocket@latest
go get google.golang.org/grpc@latest
go get github.com/stretchr/testify@latest
```

- [ ] **Step 3: Create root `Makefile`**

```makefile
.PHONY: gen test fmt

gen:
	easyp generate

fmt:
	gofmt -w .

test:
	gofmt -l .
	go vet ./...
	go test ./...
```

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum Makefile
git commit -m "chore: bootstrap go module and make targets"
```

---

## Task 2: Wire contract proto + generation

**Files:**
- Create: `transport/transport.proto`, `transport/options.proto`, `easyp.yaml`
- Generated: `transport/transport.pb.go`, `transport/options.pb.go`

- [ ] **Step 1: Write `transport/transport.proto`**

```proto
syntax = "proto3";

package wsproto.transport.v1;

option go_package = "github.com/gopherex/ws-proto/transport;transport";

// Frame is the single wire unit multiplexed over one WebSocket connection.
message Frame {
  uint32 stream_id = 1;             // client-allocated, monotonic per connection
  Kind kind = 2;
  map<string, string> headers = 3;  // OPEN: metadata + deadline; END: trailers
  bytes payload = 4;                // MSG: one marshaled request/response message
  Status status = 5;                // END only
  string method = 6;                // OPEN only: "/pkg.Service/Method"
}

enum Kind {
  KIND_UNSPECIFIED = 0;
  KIND_OPEN = 1;
  KIND_MSG = 2;
  KIND_HALF_CLOSE = 3;
  KIND_END = 4;
  KIND_RST = 5;
}

message Status {
  int32 code = 1;                   // gRPC-style status code
  string message = 2;
  repeated bytes details = 3;       // marshaled google.protobuf.Any payloads
}
```

- [ ] **Step 2: Write `transport/options.proto`**

```proto
syntax = "proto3";

package wsproto.transport.v1;

import "google/protobuf/descriptor.proto";

option go_package = "github.com/gopherex/ws-proto/transport;transport";

message ServiceOptions {
  string path_prefix = 1;  // route prefix override; default ""
}

message MethodOptions {
  string route = 1;            // route override; default "/pkg.Service/Method"
  uint32 max_message_bytes = 2; // 0 = unlimited
}

extend google.protobuf.ServiceOptions {
  ServiceOptions service = 70123;
}

extend google.protobuf.MethodOptions {
  MethodOptions method = 70123;
}
```

- [ ] **Step 3: Write `easyp.yaml`**

```yaml
lint:
  use:
    - DEFAULT
  enum_zero_value_suffix: _UNSPECIFIED
  service_suffix: API
  ignore: []
  except:
    - PACKAGE_VERSION_SUFFIX
  allow_comment_ignores: false
deps:
generate:
  inputs:
    - directory: transport
  plugins:
    - name: go
      out: .
      opts:
        paths: source_relative
```

- [ ] **Step 4: Generate**

Run:
```bash
easyp generate
```
Expected: creates `transport/transport.pb.go` and `transport/options.pb.go`.
If `protoc-gen-go` is missing, install: `go install google.golang.org/protobuf/cmd/protoc-gen-go@latest` and re-run.

- [ ] **Step 5: Verify it builds**

Run: `go build ./transport/...`
Expected: no output (success).

- [ ] **Step 6: Commit**

```bash
git add transport/ easyp.yaml
git commit -m "feat: add transport.proto Frame wire contract and options"
```

---

## Task 3: Status type

**Files:**
- Create: `wsrpc/status.go`
- Test: `wsrpc/status_test.go`

- [ ] **Step 1: Write the failing test**

`wsrpc/status_test.go`:
```go
package wsrpc

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestErrorfRoundTrip(t *testing.T) {
	err := Errorf(codes.NotFound, "user %d missing", 7)
	st := FromError(err)
	require.Equal(t, codes.NotFound, st.Code)
	require.Equal(t, "user 7 missing", st.Message)
}

func TestFromErrorNil(t *testing.T) {
	require.Nil(t, FromError(nil))
}

func TestFromErrorPlain(t *testing.T) {
	st := FromError(errors.New("boom"))
	require.Equal(t, codes.Unknown, st.Code)
	require.Equal(t, "boom", st.Message)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./wsrpc/ -run TestErrorf -v`
Expected: FAIL (undefined: Errorf).

- [ ] **Step 3: Implement `wsrpc/status.go`**

```go
package wsrpc

import (
	"fmt"

	"google.golang.org/grpc/codes"
)

// Status is a transport-level RPC status, decoupled from any wire encoding.
type Status struct {
	Code    codes.Code
	Message string
	Details [][]byte
}

func (s *Status) Error() string {
	return fmt.Sprintf("wsrpc: code = %s desc = %s", s.Code, s.Message)
}

// Errorf builds an error carrying a Status.
func Errorf(c codes.Code, format string, args ...any) error {
	return &Status{Code: c, Message: fmt.Sprintf(format, args...)}
}

// FromError extracts a *Status from err. Returns nil for nil. Non-status
// errors map to codes.Unknown.
func FromError(err error) *Status {
	if err == nil {
		return nil
	}
	if s, ok := err.(*Status); ok {
		return s
	}
	return &Status{Code: codes.Unknown, Message: err.Error()}
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./wsrpc/ -run "TestErrorf|TestFromError" -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add wsrpc/status.go wsrpc/status_test.go
git commit -m "feat: add wsrpc Status type and error helpers"
```

---

## Task 4: Frame codec + frameConn interface

**Files:**
- Create: `wsrpc/codec.go`
- Test: `wsrpc/codec_test.go`

- [ ] **Step 1: Write the failing test**

`wsrpc/codec_test.go`:
```go
package wsrpc

import (
	"testing"

	"github.com/gopherex/ws-proto/transport"
	"github.com/stretchr/testify/require"
)

func TestFrameCodecRoundTrip(t *testing.T) {
	in := &transport.Frame{
		StreamId: 3,
		Kind:     transport.Kind_KIND_OPEN,
		Method:   "/pkg.Svc/Do",
		Headers:  map[string]string{"k": "v"},
		Payload:  []byte{1, 2, 3},
	}
	b, err := marshalFrame(in)
	require.NoError(t, err)

	out, err := unmarshalFrame(b)
	require.NoError(t, err)
	require.Equal(t, in.StreamId, out.StreamId)
	require.Equal(t, in.Kind, out.Kind)
	require.Equal(t, in.Method, out.Method)
	require.Equal(t, "v", out.Headers["k"])
	require.Equal(t, []byte{1, 2, 3}, out.Payload)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./wsrpc/ -run TestFrameCodec -v`
Expected: FAIL (undefined: marshalFrame).

- [ ] **Step 3: Implement `wsrpc/codec.go`**

```go
package wsrpc

import (
	"context"

	"github.com/gopherex/ws-proto/transport"
	"google.golang.org/protobuf/proto"
)

// frameConn is the abstract transport the Mux runs over. Implemented by
// pipeConn (tests) and wsConn (coder/websocket).
type frameConn interface {
	WriteFrame(ctx context.Context, f *transport.Frame) error
	ReadFrame(ctx context.Context) (*transport.Frame, error)
	Close() error
}

func marshalFrame(f *transport.Frame) ([]byte, error) {
	return proto.Marshal(f)
}

func unmarshalFrame(b []byte) (*transport.Frame, error) {
	f := &transport.Frame{}
	if err := proto.Unmarshal(b, f); err != nil {
		return nil, err
	}
	return f, nil
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./wsrpc/ -run TestFrameCodec -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add wsrpc/codec.go wsrpc/codec_test.go
git commit -m "feat: add Frame codec and frameConn interface"
```

---

## Task 5: In-memory pipe frameConn

**Files:**
- Create: `wsrpc/pipe.go`
- Test: `wsrpc/pipe_test.go`

- [ ] **Step 1: Write the failing test**

`wsrpc/pipe_test.go`:
```go
package wsrpc

import (
	"context"
	"testing"

	"github.com/gopherex/ws-proto/transport"
	"github.com/stretchr/testify/require"
)

func TestPipeDelivers(t *testing.T) {
	ctx := context.Background()
	a, b := newPipe()
	defer a.Close()
	defer b.Close()

	go func() {
		_ = a.WriteFrame(ctx, &transport.Frame{StreamId: 1, Kind: transport.Kind_KIND_MSG})
	}()

	f, err := b.ReadFrame(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(1), f.StreamId)
	require.Equal(t, transport.Kind_KIND_MSG, f.Kind)
}

func TestPipeCloseUnblocksRead(t *testing.T) {
	ctx := context.Background()
	a, b := newPipe()
	a.Close()
	_, err := b.ReadFrame(ctx)
	require.Error(t, err)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./wsrpc/ -run TestPipe -v`
Expected: FAIL (undefined: newPipe).

- [ ] **Step 3: Implement `wsrpc/pipe.go`**

```go
package wsrpc

import (
	"context"
	"errors"

	"github.com/gopherex/ws-proto/transport"
)

// pipeConn is an in-memory frameConn. newPipe returns the two connected ends.
type pipeConn struct {
	in   chan *transport.Frame // frames readable on this end
	out  chan *transport.Frame // frames written from this end
	done chan struct{}
}

func newPipe() (*pipeConn, *pipeConn) {
	ab := make(chan *transport.Frame, 16)
	ba := make(chan *transport.Frame, 16)
	done := make(chan struct{})
	a := &pipeConn{in: ba, out: ab, done: done}
	b := &pipeConn{in: ab, out: ba, done: done}
	return a, b
}

var errPipeClosed = errors.New("wsrpc: pipe closed")

func (p *pipeConn) WriteFrame(ctx context.Context, f *transport.Frame) error {
	select {
	case p.out <- f:
		return nil
	case <-p.done:
		return errPipeClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *pipeConn) ReadFrame(ctx context.Context) (*transport.Frame, error) {
	select {
	case f := <-p.in:
		return f, nil
	case <-p.done:
		return nil, errPipeClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *pipeConn) Close() error {
	select {
	case <-p.done:
	default:
		close(p.done)
	}
	return nil
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./wsrpc/ -run TestPipe -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add wsrpc/pipe.go wsrpc/pipe_test.go
git commit -m "feat: add in-memory pipe frameConn for tests"
```

---

## Task 6: Stream (untyped per-RPC API)

**Files:**
- Create: `wsrpc/stream.go`

This task defines the `Stream` type used by mux/server/client. It is exercised
by the integration tests in Task 9 (a `Stream` cannot function without a `Mux`),
so there is no isolated unit test here — the failing test is Task 9 Step 1.

- [ ] **Step 1: Implement `wsrpc/stream.go`**

```go
package wsrpc

import (
	"context"
	"io"
	"sync"

	"github.com/gopherex/ws-proto/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// Stream is one multiplexed RPC. It is untyped: callers pass proto.Message.
// Generated code (Plan 2) wraps this in typed Send/Recv methods.
type Stream struct {
	id     uint32
	method string
	mux    *Mux
	ctx    context.Context
	cancel context.CancelFunc

	header map[string]string // headers received on OPEN (server) or END (client trailers)

	recvCh chan *transport.Frame // MSG/END/RST frames routed here by the mux

	mu       sync.Mutex
	sendDone bool
	closed   bool
	endSt    *Status // set when END/RST observed
}

func newStream(ctx context.Context, mux *Mux, id uint32, method string) *Stream {
	c, cancel := context.WithCancel(ctx)
	return &Stream{
		id:     id,
		method: method,
		mux:    mux,
		ctx:    c,
		cancel: cancel,
		recvCh: make(chan *transport.Frame, 16),
	}
}

// Context returns the stream context, cancelled on end/RST.
func (s *Stream) Context() context.Context { return s.ctx }

// Method returns the fully-qualified RPC method.
func (s *Stream) Method() string { return s.method }

// Header returns headers/trailers observed for this stream.
func (s *Stream) Header() map[string]string { return s.header }

// Send marshals msg and writes a MSG frame.
func (s *Stream) Send(msg proto.Message) error {
	b, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return s.mux.write(s.ctx, &transport.Frame{
		StreamId: s.id,
		Kind:     transport.Kind_KIND_MSG,
		Payload:  b,
	})
}

// Recv waits for the next MSG and unmarshals into msg. Returns io.EOF on a
// clean END, or the *Status as error on a non-OK END / RST.
func (s *Stream) Recv(msg proto.Message) error {
	select {
	case f, ok := <-s.recvCh:
		if !ok {
			return io.EOF
		}
		switch f.Kind {
		case transport.Kind_KIND_MSG:
			return proto.Unmarshal(f.Payload, msg)
		case transport.Kind_KIND_END:
			s.applyEnd(f)
			if s.endSt != nil && s.endSt.Code != codes.OK {
				return s.endSt
			}
			return io.EOF
		case transport.Kind_KIND_RST:
			s.applyEnd(f)
			if s.endSt == nil {
				s.endSt = &Status{Code: codes.Canceled, Message: "stream reset"}
			}
			return s.endSt
		default:
			return io.EOF
		}
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// CloseSend signals the client is done sending (HALF_CLOSE).
func (s *Stream) CloseSend() error {
	s.mu.Lock()
	if s.sendDone {
		s.mu.Unlock()
		return nil
	}
	s.sendDone = true
	s.mu.Unlock()
	return s.mux.write(s.ctx, &transport.Frame{
		StreamId: s.id,
		Kind:     transport.Kind_KIND_HALF_CLOSE,
	})
}

// end is called by the server side to finish a stream with a status + trailers.
func (s *Stream) end(st *Status, trailers map[string]string) error {
	f := &transport.Frame{
		StreamId: s.id,
		Kind:     transport.Kind_KIND_END,
		Headers:  trailers,
		Status:   statusToProto(st),
	}
	return s.mux.write(s.ctx, f)
}

func (s *Stream) applyEnd(f *transport.Frame) {
	if f.Headers != nil {
		s.header = f.Headers
	}
	s.endSt = statusFromProto(f.Status)
	s.cancel()
}

// deliver routes an inbound frame into the stream (called by the mux).
func (s *Stream) deliver(f *transport.Frame) {
	select {
	case s.recvCh <- f:
	case <-s.ctx.Done():
	}
}

func statusToProto(st *Status) *transport.Status {
	if st == nil {
		st = &Status{Code: codes.OK}
	}
	return &transport.Status{
		Code:    int32(st.Code),
		Message: st.Message,
		Details: st.Details,
	}
}

func statusFromProto(p *transport.Status) *Status {
	if p == nil {
		return &Status{Code: codes.OK}
	}
	return &Status{
		Code:    codes.Code(p.Code),
		Message: p.Message,
		Details: p.Details,
	}
}
```

- [ ] **Step 2: Verify it builds**

Run: `go build ./wsrpc/`
Expected: build fails only on undefined `Mux.write` (added in Task 7) — that is expected; do NOT commit yet. Proceed to Task 7.

---

## Task 7: Mux (frame router + stream registry)

**Files:**
- Create: `wsrpc/mux.go`

The mux is exercised by the Task 9 integration tests. Build verification only here.

- [ ] **Step 1: Implement `wsrpc/mux.go`**

```go
package wsrpc

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/gopherex/ws-proto/transport"
)

// Mux multiplexes streams over one frameConn. Used by both client and server;
// onOpen is nil on the client and set on the server to dispatch new streams.
type Mux struct {
	conn   frameConn
	ctx    context.Context
	cancel context.CancelFunc

	nextID uint32 // client stream-id allocator (odd ids), atomic

	mu      sync.Mutex
	streams map[uint32]*Stream

	onOpen func(*Stream) // server-side dispatch; nil on client

	writeMu sync.Mutex // serialize frame writes
}

func newMux(ctx context.Context, conn frameConn, onOpen func(*Stream)) *Mux {
	c, cancel := context.WithCancel(ctx)
	m := &Mux{
		conn:    conn,
		ctx:     c,
		cancel:  cancel,
		nextID:  1,
		streams: make(map[uint32]*Stream),
		onOpen:  onOpen,
	}
	go m.readLoop()
	return m
}

func (m *Mux) write(ctx context.Context, f *transport.Frame) error {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	return m.conn.WriteFrame(ctx, f)
}

// newClientStream allocates an id, registers the stream, and sends OPEN.
func (m *Mux) newClientStream(ctx context.Context, method string, headers map[string]string) (*Stream, error) {
	id := atomic.AddUint32(&m.nextID, 2) - 2 // 1,3,5,...
	s := newStream(ctx, m, id, method)
	m.mu.Lock()
	m.streams[id] = s
	m.mu.Unlock()
	if err := m.write(ctx, &transport.Frame{
		StreamId: id,
		Kind:     transport.Kind_KIND_OPEN,
		Method:   method,
		Headers:  headers,
	}); err != nil {
		m.remove(id)
		return nil, err
	}
	return s, nil
}

func (m *Mux) remove(id uint32) {
	m.mu.Lock()
	delete(m.streams, id)
	m.mu.Unlock()
}

func (m *Mux) readLoop() {
	defer m.cancel()
	for {
		f, err := m.conn.ReadFrame(m.ctx)
		if err != nil {
			m.failAll(err)
			return
		}
		m.route(f)
	}
}

func (m *Mux) route(f *transport.Frame) {
	if f.Kind == transport.Kind_KIND_OPEN {
		if m.onOpen == nil {
			return // clients ignore OPEN
		}
		s := newStream(m.ctx, m, f.StreamId, f.Method)
		s.header = f.Headers
		m.mu.Lock()
		m.streams[f.StreamId] = s
		m.mu.Unlock()
		m.onOpen(s)
		return
	}

	m.mu.Lock()
	s := m.streams[f.StreamId]
	m.mu.Unlock()
	if s == nil {
		return
	}
	s.deliver(f)
	if f.Kind == transport.Kind_KIND_END || f.Kind == transport.Kind_KIND_RST {
		m.remove(f.StreamId)
	}
}

func (m *Mux) failAll(err error) {
	m.mu.Lock()
	for id, s := range m.streams {
		s.endSt = FromError(err)
		s.cancel()
		delete(m.streams, id)
	}
	m.mu.Unlock()
}

// Close shuts the mux and underlying conn.
func (m *Mux) Close() error {
	m.cancel()
	return m.conn.Close()
}
```

- [ ] **Step 2: Verify it builds**

Run: `go build ./wsrpc/`
Expected: success (Task 6 `stream.go` now resolves `Mux.write`).

- [ ] **Step 3: Commit Tasks 6+7 together**

```bash
git add wsrpc/stream.go wsrpc/mux.go
git commit -m "feat: add wsrpc Stream and Mux"
```

---

## Task 8: Server + Client

**Files:**
- Create: `wsrpc/server.go`, `wsrpc/client.go`

- [ ] **Step 1: Implement `wsrpc/server.go`**

```go
package wsrpc

import (
	"context"
	"net/http"
	"sync"

	"github.com/coder/websocket"
	"google.golang.org/grpc/codes"
)

// Handler runs one server-side RPC stream. Returning an error ends the stream
// with the corresponding status; returning nil ends with codes.OK.
type Handler func(ctx context.Context, stream *Stream) error

// Server is an http.Handler that upgrades to WebSocket and dispatches streams
// to registered handlers keyed by fully-qualified method.
type Server struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func NewServer() *Server {
	return &Server{handlers: make(map[string]Handler)}
}

// Register binds a handler to a method like "/pkg.Service/Method".
func (s *Server) Register(method string, h Handler) {
	s.mu.Lock()
	s.handlers[method] = h
	s.mu.Unlock()
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	conn := newWSConn(c)
	mux := newMux(r.Context(), conn, nil)
	mux.onOpen = func(stream *Stream) { go s.serveStream(stream) }
	<-mux.ctx.Done()
	_ = c.CloseNow()
}

func (s *Server) serveStream(stream *Stream) {
	s.mu.RLock()
	h := s.handlers[stream.method]
	s.mu.RUnlock()
	if h == nil {
		_ = stream.end(&Status{Code: codes.Unimplemented, Message: "unknown method " + stream.method}, nil)
		return
	}
	err := h(stream.ctx, stream)
	st := FromError(err)
	if st == nil {
		st = &Status{Code: codes.OK}
	}
	_ = stream.end(st, nil)
}
```

- [ ] **Step 2: Implement `wsrpc/client.go`**

```go
package wsrpc

import (
	"context"

	"github.com/coder/websocket"
)

// ClientConn is a multiplexed client connection.
type ClientConn struct {
	mux *Mux
}

// Dial opens a WebSocket to url (ws:// or wss://) and returns a ClientConn.
func Dial(ctx context.Context, url string) (*ClientConn, error) {
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return nil, err
	}
	conn := newWSConn(c)
	return &ClientConn{mux: newMux(ctx, conn, nil)}, nil
}

// newClientConn wraps an existing frameConn (used by tests over a pipe).
func newClientConn(ctx context.Context, conn frameConn) *ClientConn {
	return &ClientConn{mux: newMux(ctx, conn, nil)}
}

// NewStream opens a new client stream for method with request headers.
func (cc *ClientConn) NewStream(ctx context.Context, method string, headers map[string]string) (*Stream, error) {
	return cc.mux.newClientStream(ctx, method, headers)
}

func (cc *ClientConn) Close() error { return cc.mux.Close() }
```

- [ ] **Step 3: Implement `wsrpc/wsconn.go`**

```go
package wsrpc

import (
	"context"

	"github.com/coder/websocket"
	"github.com/gopherex/ws-proto/transport"
)

// wsConn adapts a coder/websocket connection to frameConn.
type wsConn struct {
	c *websocket.Conn
}

func newWSConn(c *websocket.Conn) *wsConn {
	c.SetReadLimit(-1)
	return &wsConn{c: c}
}

func (w *wsConn) WriteFrame(ctx context.Context, f *transport.Frame) error {
	b, err := marshalFrame(f)
	if err != nil {
		return err
	}
	return w.c.Write(ctx, websocket.MessageBinary, b)
}

func (w *wsConn) ReadFrame(ctx context.Context) (*transport.Frame, error) {
	_, b, err := w.c.Read(ctx)
	if err != nil {
		return nil, err
	}
	return unmarshalFrame(b)
}

func (w *wsConn) Close() error {
	return w.c.Close(websocket.StatusNormalClosure, "")
}
```

- [ ] **Step 4: Verify it builds**

Run: `go build ./wsrpc/`
Expected: success.

- [ ] **Step 5: Commit**

```bash
git add wsrpc/server.go wsrpc/client.go wsrpc/wsconn.go
git commit -m "feat: add wsrpc Server, ClientConn and WebSocket frameConn"
```

---

## Task 9: Integration tests — all four stream kinds over a pipe

**Files:**
- Create: `wsrpc/integration_test.go`

This test wires a server-side mux and a client `ClientConn` over an in-memory
pipe, with no real WebSocket, exercising the full `Stream`/`Mux` machinery.

- [ ] **Step 1: Write the failing test**

`wsrpc/integration_test.go`:
```go
package wsrpc

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/gopherex/ws-proto/transport"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// harness wires a server mux + client conn over a pipe. Handlers keyed by method.
func harness(t *testing.T, handlers map[string]Handler) *ClientConn {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	srvEnd, cliEnd := newPipe()

	srvMux := newMux(ctx, srvEnd, nil)
	srvMux.onOpen = func(s *Stream) {
		go func() {
			h := handlers[s.method]
			if h == nil {
				_ = s.end(&Status{Code: codes.Unimplemented}, nil)
				return
			}
			err := h(s.ctx, s)
			st := FromError(err)
			if st == nil {
				st = &Status{Code: codes.OK}
			}
			_ = s.end(st, nil)
		}()
	}

	return newClientConn(ctx, cliEnd)
}

func TestUnary(t *testing.T) {
	ctx := context.Background()
	cc := harness(t, map[string]Handler{
		"/t/Unary": func(ctx context.Context, s *Stream) error {
			var req wrapperspb.StringValue
			require.NoError(t, s.Recv(&req))
			require.Equal(t, io.EOF, s.Recv(&wrapperspb.StringValue{})) // HALF_CLOSE -> next recv after send loop
			return s.Send(&wrapperspb.StringValue{Value: "hi " + req.Value})
		},
	})

	s, err := cc.NewStream(ctx, "/t/Unary", nil)
	require.NoError(t, err)
	require.NoError(t, s.Send(&wrapperspb.StringValue{Value: "bob"}))
	require.NoError(t, s.CloseSend())

	var res wrapperspb.StringValue
	require.NoError(t, s.Recv(&res))
	require.Equal(t, "hi bob", res.Value)
	require.Equal(t, io.EOF, s.Recv(&wrapperspb.StringValue{}))
}

func TestServerStream(t *testing.T) {
	ctx := context.Background()
	cc := harness(t, map[string]Handler{
		"/t/SS": func(ctx context.Context, s *Stream) error {
			for i := 0; i < 3; i++ {
				if err := s.Send(&wrapperspb.Int32Value{Value: int32(i)}); err != nil {
					return err
				}
			}
			return nil
		},
	})

	s, err := cc.NewStream(ctx, "/t/SS", nil)
	require.NoError(t, err)
	require.NoError(t, s.CloseSend())

	var got []int32
	for {
		var v wrapperspb.Int32Value
		err := s.Recv(&v)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		got = append(got, v.Value)
	}
	require.Equal(t, []int32{0, 1, 2}, got)
}

func TestClientStream(t *testing.T) {
	ctx := context.Background()
	cc := harness(t, map[string]Handler{
		"/t/CS": func(ctx context.Context, s *Stream) error {
			var sum int32
			for {
				var v wrapperspb.Int32Value
				err := s.Recv(&v)
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				sum += v.Value
			}
			return s.Send(&wrapperspb.Int32Value{Value: sum})
		},
	})

	s, err := cc.NewStream(ctx, "/t/CS", nil)
	require.NoError(t, err)
	for i := 1; i <= 4; i++ {
		require.NoError(t, s.Send(&wrapperspb.Int32Value{Value: int32(i)}))
	}
	require.NoError(t, s.CloseSend())

	var res wrapperspb.Int32Value
	require.NoError(t, s.Recv(&res))
	require.Equal(t, int32(10), res.Value)
}

func TestBidi(t *testing.T) {
	ctx := context.Background()
	cc := harness(t, map[string]Handler{
		"/t/BD": func(ctx context.Context, s *Stream) error {
			for {
				var v wrapperspb.StringValue
				err := s.Recv(&v)
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				if err := s.Send(&wrapperspb.StringValue{Value: "echo:" + v.Value}); err != nil {
					return err
				}
			}
			return nil
		},
	})

	s, err := cc.NewStream(ctx, "/t/BD", nil)
	require.NoError(t, err)
	require.NoError(t, s.Send(&wrapperspb.StringValue{Value: "a"}))
	var r1 wrapperspb.StringValue
	require.NoError(t, s.Recv(&r1))
	require.Equal(t, "echo:a", r1.Value)
	require.NoError(t, s.CloseSend())
	require.Equal(t, io.EOF, s.Recv(&wrapperspb.StringValue{}))
}

func TestErrorStatus(t *testing.T) {
	ctx := context.Background()
	cc := harness(t, map[string]Handler{
		"/t/Err": func(ctx context.Context, s *Stream) error {
			return Errorf(codes.PermissionDenied, "nope")
		},
	})
	s, err := cc.NewStream(ctx, "/t/Err", nil)
	require.NoError(t, err)
	require.NoError(t, s.CloseSend())
	err = s.Recv(&wrapperspb.StringValue{})
	st := FromError(err)
	require.Equal(t, codes.PermissionDenied, st.Code)
	require.Equal(t, "nope", st.Message)
}

func TestUnknownMethod(t *testing.T) {
	ctx := context.Background()
	cc := harness(t, map[string]Handler{})
	s, err := cc.NewStream(ctx, "/t/Missing", nil)
	require.NoError(t, err)
	require.NoError(t, s.CloseSend())
	err = s.Recv(&wrapperspb.StringValue{})
	require.Equal(t, codes.Unimplemented, FromError(err).Code)
}

func TestDeadlineCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	cc := harness(t, map[string]Handler{
		"/t/Hang": func(hctx context.Context, s *Stream) error {
			<-hctx.Done()
			return Errorf(codes.Canceled, "cancelled")
		},
	})
	s, err := cc.NewStream(ctx, "/t/Hang", nil)
	require.NoError(t, err)
	require.NoError(t, s.CloseSend())
	err = s.Recv(&wrapperspb.StringValue{})
	require.Error(t, err)
}
```

> Note on `TestUnary`: the handler's second `Recv` returning `io.EOF` depends on
> the client sending `HALF_CLOSE`. If the timing of the extra in-handler `Recv`
> proves flaky, simplify the handler to a single `Recv` + `Send` and drop the
> mid-handler EOF assertion — the client-side assertions are the contract that
> matters.

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./wsrpc/ -run "TestUnary|TestServerStream|TestClientStream|TestBidi" -v`
Expected: FAIL initially if any wiring is incomplete; otherwise iterate until green.

- [ ] **Step 3: Make tests pass**

Iterate on `stream.go`/`mux.go` until all integration tests pass. Likely
adjustment points: `Recv` handling of END vs RST, and `HALF_CLOSE` delivery
to the server stream (the server handler's `Recv` must observe `io.EOF` after
the client's `CloseSend`). To deliver `HALF_CLOSE` as EOF, extend
`mux.route` so a `KIND_HALF_CLOSE` frame closes the server stream's `recvCh`:

```go
// in mux.route, after s := m.streams[f.StreamId]; s != nil:
if f.Kind == transport.Kind_KIND_HALF_CLOSE {
	s.halfClose()
	return
}
```

and add to `stream.go`:
```go
// halfClose closes recvCh so a server-side Recv returns io.EOF.
func (s *Stream) halfClose() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.recvCh)
}
```

Note: `Send` after `recvCh` close is still valid (server keeps sending). Only
the inbound direction is closed by `HALF_CLOSE`.

- [ ] **Step 4: Run full package**

Run: `go test ./wsrpc/ -v`
Expected: PASS (all tests).

- [ ] **Step 5: Commit**

```bash
git add wsrpc/integration_test.go wsrpc/stream.go wsrpc/mux.go
git commit -m "test: add wsrpc integration tests for all four stream kinds"
```

---

## Task 10: Real WebSocket loopback test

**Files:**
- Create: `wsrpc/wsconn_test.go`

- [ ] **Step 1: Write the test**

`wsrpc/wsconn_test.go`:
```go
package wsrpc

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestWebSocketLoopback(t *testing.T) {
	srv := NewServer()
	srv.Register("/t/Echo", func(ctx context.Context, s *Stream) error {
		var v wrapperspb.StringValue
		if err := s.Recv(&v); err != nil && err != io.EOF {
			return err
		}
		return s.Send(&wrapperspb.StringValue{Value: "echo:" + v.Value})
	})

	hs := httptest.NewServer(srv)
	defer hs.Close()
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	ctx := context.Background()
	cc, err := Dial(ctx, wsURL)
	require.NoError(t, err)
	defer cc.Close()

	s, err := cc.NewStream(ctx, "/t/Echo", nil)
	require.NoError(t, err)
	require.NoError(t, s.Send(&wrapperspb.StringValue{Value: "hello"}))
	require.NoError(t, s.CloseSend())

	var res wrapperspb.StringValue
	require.NoError(t, s.Recv(&res))
	require.Equal(t, "echo:hello", res.Value)
}
```

- [ ] **Step 2: Run**

Run: `go test ./wsrpc/ -run TestWebSocketLoopback -v`
Expected: PASS. If `websocket.Accept` rejects the request on origin grounds,
scope the allowed origins explicitly via
`&websocket.AcceptOptions{OriginPatterns: []string{"127.0.0.1:*", "localhost:*"}}`
in `server.go`'s `websocket.Accept` call. Do NOT disable TLS verification —
this is WebSocket origin (CSRF) handling, not transport security. Production
servers should set `OriginPatterns` to their real origins.

- [ ] **Step 3: Commit**

```bash
git add wsrpc/wsconn_test.go
git commit -m "test: add real WebSocket loopback integration test"
```

---

## Task 11: Finalize

- [ ] **Step 1: Full verification**

Run:
```bash
make test
```
Expected: `gofmt -l .` prints nothing, `go vet ./...` clean, `go test ./...` PASS.

- [ ] **Step 2: Commit any fmt/vet fixes**

```bash
git add -A
git commit -m "chore: gofmt and vet clean for wsrpc"
```

---

## Self-Review (completed during planning)

- **Spec coverage:** `transport.proto`/`Frame` (Task 2) ✓; all four stream kinds (Task 9) ✓; cancel/deadline/error-status (Task 9) ✓; mux/stream/server/client/status runtime files (Tasks 3–8) ✓; `coder/websocket` (Task 8/10) ✓; options proto (Task 2) ✓; in-memory pipe test harness (Task 5/9) ✓. The gRPC bridge adapter and typed wrappers are explicitly Plan 2 (generator) scope, not this plan.
- **Deferred to later plans:** generator (`protoc-gen-go-ws`), TS runtime, `protoc-gen-ws-es`, golden `example/`. Plan 1 delivers a working, tested untyped runtime.
- **Type consistency:** `frameConn` (codec.go) implemented by `pipeConn` (pipe.go) + `wsConn` (wsconn.go); `Mux.write`/`newClientStream`/`route`/`failAll` used consistently by stream.go/server.go/client.go; `Status`/`statusToProto`/`statusFromProto` names consistent; `Handler` signature consistent across server.go and the Task 9 harness.
- **Known risk flagged inline:** `TestUnary` mid-handler EOF timing (Task 9 note) and `websocket.Accept` origin check (Task 10 Step 2).
```
