package wsrpc

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/gopherex/ws-proto/transport"
	"google.golang.org/grpc/codes"
)

// A send on a terminated stream reports the outcome Recv reports — io.EOF for
// a clean END, the *Status otherwise — whether or not a receive has applied
// the terminal frame yet. A handler that refuses at once must never surface
// to the caller's Send as a bare io.EOF.
func TestSendAfterTerminalReportsRecvOutcome(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		kind transport.Kind
		st   *Status
		want codes.Code // codes.OK means io.EOF
	}{
		{"end ok", transport.Kind_KIND_END, nil, codes.OK},
		{"end refused", transport.Kind_KIND_END, &Status{Code: codes.PermissionDenied, Message: "undeclared"}, codes.PermissionDenied},
		{"rst ok", transport.Kind_KIND_RST, nil, codes.Canceled},
		{"rst unavailable", transport.Kind_KIND_RST, &Status{Code: codes.Unavailable, Message: "no instance"}, codes.Unavailable},
	} {
		for _, applied := range []bool{false, true} {
			name := tc.name + "/recorded"
			if applied {
				name = tc.name + "/received"
			}

			t.Run(name, func(t *testing.T) {
				t.Parallel()

				stream := newStream(context.Background(), &Mux{}, 1, "/t/SendAfterEnd", 1)
				stream.signalEnd(&transport.Frame{Kind: tc.kind, Status: statusToProto(tc.st)})

				var recvErr error
				if applied {
					_, recvErr = stream.RecvRaw()
				}

				err := stream.SendRaw([]byte{1})
				if tc.want == codes.OK {
					if !errors.Is(err, io.EOF) {
						t.Fatalf("send: got %v, want io.EOF", err)
					}
				} else if st := FromError(err); st.Code != tc.want {
					t.Fatalf("send: got %v, want %v", err, tc.want)
				}

				if applied && FromError(recvErr).Code != FromError(err).Code {
					t.Fatalf("send %v and recv %v disagree", err, recvErr)
				}
			})
		}
	}
}
