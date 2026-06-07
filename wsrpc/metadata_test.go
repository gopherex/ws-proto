package wsrpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// TestUnaryMetadataSink verifies SetHeader/SetTrailer on a context carrying a
// unary metadata sink accumulate into the UnaryMD handle.
func TestUnaryMetadataSink(t *testing.T) {
	ctx, umd := WithUnaryMetadata(context.Background())

	SetHeader(ctx, map[string]string{"x-h": "1"})
	SetHeader(ctx, map[string]string{"x-h2": "a"})
	SetTrailer(ctx, map[string]string{"x-t": "2"})

	require.Equal(t, map[string]string{"x-h": "1", "x-h2": "a"}, umd.Header())
	require.Equal(t, map[string]string{"x-t": "2"}, umd.Trailer())
}

// TestSetHeaderOutsideUnaryIsNoop verifies SetHeader/SetTrailer are no-ops when
// no sink is installed.
func TestSetHeaderOutsideUnaryIsNoop(t *testing.T) {
	require.NotPanics(t, func() {
		SetHeader(context.Background(), map[string]string{"x": "y"})
		SetTrailer(context.Background(), map[string]string{"x": "y"})
	})
}

// TestEmptyUnaryMetadata verifies an untouched sink yields nil header/trailer.
func TestEmptyUnaryMetadata(t *testing.T) {
	_, umd := WithUnaryMetadata(context.Background())
	require.Nil(t, umd.Header())
	require.Nil(t, umd.Trailer())
}

// TestUnaryServerTransportStream verifies the grpc.ServerTransportStream shim
// forwards header/trailer metadata into the sink, so grpc.SetHeader /
// grpc.SetTrailer (which find the transport stream on the ctx) propagate.
func TestUnaryServerTransportStream(t *testing.T) {
	ctx, umd := WithUnaryMetadata(context.Background())
	ctx = grpc.NewContextWithServerTransportStream(ctx, UnaryServerTransportStream(ctx, "/svc/M"))

	require.NoError(t, grpc.SetHeader(ctx, metadata.Pairs("x-md", "v")))
	require.NoError(t, grpc.SetTrailer(ctx, metadata.Pairs("x-tr", "w")))

	require.Equal(t, "v", umd.Header()["x-md"])
	require.Equal(t, "w", umd.Trailer()["x-tr"])
}
