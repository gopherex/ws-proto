package wsrpc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

// dialOrigin performs a raw WebSocket upgrade against srv with the given
// Origin header ("" sends no Origin, like a non-browser client) and returns
// the HTTP status of the handshake response (101 on success).
func dialOrigin(t *testing.T, srv *Server, origin string) int {
	t.Helper()
	hs := httptest.NewServer(srv)
	defer hs.Close()
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	h := http.Header{}
	if origin != "" {
		if origin == "self" {
			origin = hs.URL
		}
		h.Set("Origin", origin)
	}
	c, resp, err := websocket.Dial(context.Background(), wsURL, &websocket.DialOptions{
		Subprotocols: []string{Subprotocol},
		HTTPHeader:   h,
	})
	if err == nil {
		defer c.CloseNow()
	}
	require.NotNil(t, resp, "handshake produced no HTTP response: %v", err)
	return resp.StatusCode
}

// TestOriginPolicyFailClosed verifies H3: a server constructed with no origin
// policy (neither WithOriginPatterns, WithSameOriginOnly nor
// WithInsecureSkipOriginCheck) rejects every WebSocket upgrade with 403 rather
// than silently accepting cross-origin clients.
func TestOriginPolicyFailClosed(t *testing.T) {
	srv := NewServer()
	hs := httptest.NewServer(srv)
	defer hs.Close()
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	_, err := Dial(context.Background(), wsURL)
	require.Error(t, err, "unconfigured server must reject the upgrade (fail closed)")

	require.Equal(t, http.StatusForbidden, dialOrigin(t, NewServer(), ""))
	require.Equal(t, http.StatusForbidden, dialOrigin(t, NewServer(), "self"))
}

// TestOriginInsecureSkipAllows verifies the explicit opt-out: a server built
// with WithInsecureSkipOriginCheck accepts upgrades from any origin.
func TestOriginInsecureSkipAllows(t *testing.T) {
	srv := NewServer(WithInsecureSkipOriginCheck())
	hs := httptest.NewServer(srv)
	defer hs.Close()
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	cc, err := Dial(context.Background(), wsURL)
	require.NoError(t, err)
	defer cc.Close()

	require.Equal(t, http.StatusSwitchingProtocols,
		dialOrigin(t, NewServer(WithInsecureSkipOriginCheck()), "https://evil.example"))
}

// TestOriginPatternsAllows verifies that configuring WithOriginPatterns also
// satisfies the fail-closed gate (a non-empty policy was chosen).
func TestOriginPatternsAllows(t *testing.T) {
	srv := NewServer(WithOriginPatterns("*"))
	hs := httptest.NewServer(srv)
	defer hs.Close()
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	cc, err := Dial(context.Background(), wsURL)
	require.NoError(t, err)
	defer cc.Close()
}

// TestOriginPatternsRestrict verifies that a non-empty pattern list admits
// matching cross-origin pages, same-origin pages and non-browser clients, and
// rejects every other origin.
func TestOriginPatternsRestrict(t *testing.T) {
	newSrv := func() *Server { return NewServer(WithOriginPatterns("app.example.com")) }
	require.Equal(t, http.StatusSwitchingProtocols, dialOrigin(t, newSrv(), "https://app.example.com"))
	require.Equal(t, http.StatusSwitchingProtocols, dialOrigin(t, newSrv(), "self"))
	require.Equal(t, http.StatusSwitchingProtocols, dialOrigin(t, newSrv(), ""))
	require.Equal(t, http.StatusForbidden, dialOrigin(t, newSrv(), "https://evil.example"))
}

// TestOriginSameOnly verifies the same-origin-only policy, expressed both via
// WithSameOriginOnly and via WithOriginPatterns with an explicit empty list:
// non-browser clients (no Origin header) and same-origin pages upgrade, while
// cross-origin pages are rejected.
func TestOriginSameOnly(t *testing.T) {
	cases := map[string]func() *Server{
		"WithSameOriginOnly":      func() *Server { return NewServer(WithSameOriginOnly()) },
		"WithOriginPatterns()":    func() *Server { return NewServer(WithOriginPatterns()) },
		"patterns then same-only": func() *Server { return NewServer(WithOriginPatterns("evil.example"), WithSameOriginOnly()) },
	}
	for name, newSrv := range cases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, http.StatusSwitchingProtocols, dialOrigin(t, newSrv(), ""), "no Origin header")
			require.Equal(t, http.StatusSwitchingProtocols, dialOrigin(t, newSrv(), "self"), "same origin")
			require.Equal(t, http.StatusForbidden, dialOrigin(t, newSrv(), "https://evil.example"), "cross origin")

			hs := httptest.NewServer(newSrv())
			defer hs.Close()
			cc, err := Dial(context.Background(), "ws"+strings.TrimPrefix(hs.URL, "http"))
			require.NoError(t, err, "wsrpc client (no Origin) must connect")
			defer cc.Close()
		})
	}
}
