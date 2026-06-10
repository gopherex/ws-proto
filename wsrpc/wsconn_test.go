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
	srv := NewServer(WithInsecureSkipOriginCheck())
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
