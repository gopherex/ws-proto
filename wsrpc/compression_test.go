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

func TestCompressionRoundTrip(t *testing.T) {
	srv := NewServer(WithCompression(CompressionContextTakeover))
	srv.Register("/t/Echo", func(ctx context.Context, s *Stream) error {
		var v wrapperspb.StringValue
		if err := s.Recv(&v); err != nil && err != io.EOF {
			return err
		}
		return s.Send(&wrapperspb.StringValue{Value: "z:" + v.Value})
	})

	hs := httptest.NewServer(srv)
	defer hs.Close()

	cc, err := Dial(context.Background(), "ws"+strings.TrimPrefix(hs.URL, "http"),
		WithDialCompression(CompressionContextTakeover))
	require.NoError(t, err)
	defer cc.Close()

	s, err := cc.NewStream(context.Background(), "/t/Echo", nil)
	require.NoError(t, err)
	// A large, compressible payload so permessage-deflate is exercised.
	payload := strings.Repeat("a", 4096)
	require.NoError(t, s.Send(&wrapperspb.StringValue{Value: payload}))
	require.NoError(t, s.CloseSend())

	var res wrapperspb.StringValue
	require.NoError(t, s.Recv(&res))
	require.Equal(t, "z:"+payload, res.Value)
}
