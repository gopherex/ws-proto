package wsrpc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestStatusDetailsRoundTrip(t *testing.T) {
	// A wsrpc Status carrying one detail (Any-wrapped StringValue), as it travels
	// on the wire (Frame.Status.details = marshaled Any bytes).
	detail, err := anypb.New(&wrapperspb.StringValue{Value: "boom-detail"})
	require.NoError(t, err)
	raw, err := proto.Marshal(detail)
	require.NoError(t, err)

	st := &Status{Code: codes.FailedPrecondition, Message: "bad", Details: [][]byte{raw}}

	// *Status -> gRPC status keeps code, message and details.
	gs := st.GRPCStatus()
	require.Equal(t, codes.FailedPrecondition, gs.Code())
	require.Equal(t, "bad", gs.Message())
	require.Len(t, gs.Proto().GetDetails(), 1)

	// gRPC status error -> back to *Status with details preserved.
	back := FromError(gs.Err())
	require.Equal(t, codes.FailedPrecondition, back.Code)
	require.Equal(t, "bad", back.Message)
	require.Len(t, back.Details, 1)

	var a anypb.Any
	require.NoError(t, proto.Unmarshal(back.Details[0], &a))
	msg, err := a.UnmarshalNew()
	require.NoError(t, err)
	require.Equal(t, "boom-detail", msg.(*wrapperspb.StringValue).Value)
}
