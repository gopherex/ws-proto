package wsrpc

import (
	"fmt"

	"google.golang.org/grpc/codes"
)

// Status is a transport-level RPC status, decoupled from any wire encoding.
type Status struct {
	Code    codes.Code
	Message string
	Details [][]byte
}

func (s *Status) Error() string {
	return fmt.Sprintf("wsrpc: code = %s desc = %s", s.Code, s.Message)
}

// Errorf builds an error carrying a Status.
func Errorf(c codes.Code, format string, args ...any) error {
	return &Status{Code: c, Message: fmt.Sprintf(format, args...)}
}

// FromError extracts a *Status from err. Returns nil for nil. Non-status
// errors map to codes.Unknown.
func FromError(err error) *Status {
	if err == nil {
		return nil
	}
	if s, ok := err.(*Status); ok {
		return s
	}
	return &Status{Code: codes.Unknown, Message: err.Error()}
}
