package wsrpc

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

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
