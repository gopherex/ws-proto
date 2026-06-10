package wsrpc

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOriginPolicyFailClosed verifies H3: a server constructed with no origin
// policy (neither WithOriginPatterns nor WithInsecureSkipOriginCheck) rejects
// every WebSocket upgrade rather than silently accepting cross-origin clients.
func TestOriginPolicyFailClosed(t *testing.T) {
	srv := NewServer()
	hs := httptest.NewServer(srv)
	defer hs.Close()
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	_, err := Dial(context.Background(), wsURL)
	require.Error(t, err, "unconfigured server must reject the upgrade (fail closed)")
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
