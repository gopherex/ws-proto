package example_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	echov1 "github.com/gopherex/ws-proto/example/proto/echo/v1"
	"github.com/gopherex/ws-proto/wsrpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// mdImpl is a gRPC EchoServiceServer whose streaming method emits leading header
// and trailer response metadata via the grpc.ServerStream, the realistic path
// the bridge now propagates.
type mdImpl struct {
	echov1.UnimplementedEchoServiceServer
}

func (mdImpl) ServerStream(req *echov1.ServerStreamRequest, stream grpc.ServerStreamingServer[echov1.ServerStreamResponse]) error {
	if err := stream.SendHeader(metadata.Pairs("x-md", "v")); err != nil {
		return err
	}
	stream.SetTrailer(metadata.Pairs("x-trl", "t"))
	for i := int32(0); i < req.Count; i++ {
		if err := stream.Send(&echov1.ServerStreamResponse{Index: i}); err != nil {
			return err
		}
	}
	return nil
}

// TestBridgeStreamingMetadataPropagates verifies that streaming response
// metadata set on the grpc.ServerStream (SendHeader/SetTrailer) reaches the
// wsrpc client as a leading header and trailer respectively.
func TestBridgeStreamingMetadataPropagates(t *testing.T) {
	srv := wsrpc.NewServer()
	echov1.RegisterEchoServiceHandler(srv, echov1.EchoServiceFromGRPC(mdImpl{}))

	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)
	wsURL := "ws" + strings.TrimPrefix(hs.URL, "http")

	cc, err := wsrpc.Dial(context.Background(), wsURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })
	client := echov1.NewEchoServiceWSClient(cc)

	stream, err := client.ServerStream(context.Background(), &echov1.ServerStreamRequest{Count: 2})
	require.NoError(t, err)

	// Drain the responses so END (and trailers) arrive.
	var got int
	for {
		_, err := stream.Recv()
		if err != nil {
			break
		}
		got++
	}
	require.Equal(t, 2, got)

	require.Equal(t, "v", stream.Header()["x-md"])
	require.Equal(t, "t", stream.Trailer()["x-trl"])
}
