package wsrpc

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFromErrorContextStatus(t *testing.T) {
	t.Parallel()
	explicit := &Status{Code: codes.PermissionDenied, Message: "denied"}
	grpcError := status.Error(codes.ResourceExhausted, "busy")
	for _, test := range []struct {
		name string
		err  error
		code codes.Code
	}{
		{"cancelled", context.Canceled, codes.Canceled},
		{"deadline", context.DeadlineExceeded, codes.DeadlineExceeded},
		{"wrapped cancelled", fmt.Errorf("read request: %w", context.Canceled), codes.Canceled},
		{"wrapped deadline", fmt.Errorf("read request: %w", context.DeadlineExceeded), codes.DeadlineExceeded},
		{"nested deadline", fmt.Errorf("handler: %w", fmt.Errorf("database: %w", context.DeadlineExceeded)), codes.DeadlineExceeded},
		{"joined cancelled", errors.Join(errors.New("upstream"), context.Canceled), codes.Canceled},
		{"joined deadline", errors.Join(errors.New("upstream"), context.DeadlineExceeded), codes.DeadlineExceeded},
		{"ws status before context", errors.Join(explicit, context.DeadlineExceeded), codes.PermissionDenied},
		{"ws status after context", errors.Join(context.DeadlineExceeded, explicit), codes.PermissionDenied},
		{"grpc status before context", errors.Join(grpcError, context.Canceled), codes.ResourceExhausted},
		{"grpc status after context", errors.Join(context.Canceled, grpcError), codes.ResourceExhausted},
		{"plain error", errors.New("context deadline exceeded"), codes.Unknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			result := FromError(test.err)
			require.Equal(t, test.code, result.Code)
			require.Equal(t, test.err.Error(), result.Message)
		})
	}
	require.Same(t, explicit, FromError(explicit))
}

func BenchmarkFromErrorContextStatus(b *testing.B) {
	err := fmt.Errorf("read request: %w", context.DeadlineExceeded)
	for b.Loop() {
		FromError(err)
	}
}

func TestErrorfRoundTrip(t *testing.T) {
	err := Errorf(codes.NotFound, "user %d missing", 7)
	st := FromError(err)
	require.Equal(t, codes.NotFound, st.Code)
	require.Equal(t, "user 7 missing", st.Message)
}

func TestFromErrorNil(t *testing.T) {
	require.Nil(t, FromError(nil))
}

func TestFromErrorPlain(t *testing.T) {
	st := FromError(errors.New("boom"))
	require.Equal(t, codes.Unknown, st.Code)
	require.Equal(t, "boom", st.Message)
}
