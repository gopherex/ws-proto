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
