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
