package wsrpc

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// GRPCStatus makes *Status interoperable with the google.golang.org/grpc/status
// package, so status.Code(err) / status.FromError(err) recognize a wsrpc error
// (e.g. the code surfaced to a client when a gRPC interceptor on the FromGRPC
// bridge rejects an RPC). Note: Details are not carried into the gRPC status
// (nor read back in FromError) — only code and message round-trip today.
func (s *Status) GRPCStatus() *status.Status {
	return status.New(s.Code, s.Message)
}

// Errorf builds an error carrying a Status.
func Errorf(c codes.Code, format string, args ...any) error {
	return &Status{Code: c, Message: fmt.Sprintf(format, args...)}
}

// FromError extracts a *Status from err. Returns nil for nil. A *Status is
// returned as-is; a gRPC *status.Status error (e.g. status.Error, as produced
// by gRPC server interceptors on the FromGRPC bridge) maps to its code and
// message. Other errors map to codes.Unknown.
func FromError(err error) *Status {
	if err == nil {
		return nil
	}
	if s, ok := err.(*Status); ok {
		return s
	}
	if gs, ok := status.FromError(err); ok {
		return &Status{Code: gs.Code(), Message: gs.Message()}
	}
	return &Status{Code: codes.Unknown, Message: err.Error()}
}
