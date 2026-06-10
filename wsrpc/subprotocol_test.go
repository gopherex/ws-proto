package wsrpc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

// TestDialRejectsWrongSubprotocol reproduces M2: if the server completes the
// WebSocket handshake WITHOUT selecting the wsrpc.v1 subprotocol (a misbehaving
// server or a proxy that strips Sec-WebSocket-Protocol), the client would
// otherwise speak a framing protocol the peer never agreed to. Dial must reject
// such a connection instead of silently mis-framing.
func TestDialRejectsWrongSubprotocol(t *testing.T) {
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Accept the upgrade but negotiate NO subprotocol.
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer c.CloseNow()
		<-r.Context().Done()
	}))
	defer hs.Close()
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := Dial(ctx, wsURL)
	require.Error(t, err, "Dial must reject a server that did not negotiate the wsrpc.v1 subprotocol")
}
