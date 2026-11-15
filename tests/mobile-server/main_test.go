package main

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	echov1 "github.com/gopherex/ws-proto/example/proto/echo/v1"
	"github.com/gopherex/ws-proto/wsrpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestMobileFixture(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"Ada", "!error", "!empty", "!conn-end"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(newHandler())
			defer server.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			connection, err := wsrpc.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"))
			require.NoError(t, err)
			defer connection.Close()
			stream, err := connection.NewStream(ctx, "/echo.v1.EchoService/Unary", map[string]string{"x-test": "request"})
			require.NoError(t, err)
			require.NoError(t, stream.Send(&echov1.UnaryRequest{Name: name}))
			require.NoError(t, stream.CloseSend())
			response := new(echov1.UnaryResponse)
			err = stream.Recv(response)
			switch name {
			case "Ada":
				require.NoError(t, err)
				require.Equal(t, "hello Ada", response.GetGreeting())
				require.ErrorIs(t, stream.Recv(response), io.EOF)
			case "!empty":
				require.ErrorIs(t, err, io.EOF)
			case "!error":
				require.Error(t, err)
				status := wsrpc.FromError(err)
				require.Equal(t, codes.InvalidArgument, status.Code)
				require.Equal(t, "mobile-error", status.Message)
				require.Len(t, status.Details, 1)
				detail := new(anypb.Any)
				require.NoError(t, proto.Unmarshal(status.Details[0], detail))
				value := new(wrapperspb.StringValue)
				require.NoError(t, detail.UnmarshalTo(value))
				require.Equal(t, "mobile-detail", value.GetValue())
			case "!conn-end":
				require.Error(t, err)
				require.Equal(t, codes.PermissionDenied, wsrpc.FromError(err).Code)
			}
			require.Equal(t, "ready", stream.Header()["x-header"])
			require.Equal(t, "request", stream.Header()["x-echo"])
			if name != "!conn-end" {
				require.Equal(t, "done", stream.Trailer()["x-trailer"])
			}
		})
	}
}
