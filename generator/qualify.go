package generator

import (
	"strconv"
	"unicode"
	"unicode/utf8"

	"google.golang.org/protobuf/compiler/protogen"
)

// Import paths used across emitted code. Centralized so every emit site
// qualifies identifiers through GeneratedFile.QualifiedGoIdent.
const (
	wsrpcImport    = protogen.GoImportPath("github.com/gopherex/ws-proto/wsrpc")
	protoImport    = protogen.GoImportPath("google.golang.org/protobuf/proto")
	contextImport  = protogen.GoImportPath("context")
	ioImport       = protogen.GoImportPath("io")
	grpcImport     = protogen.GoImportPath("google.golang.org/grpc")
	metadataImport = protogen.GoImportPath("google.golang.org/grpc/metadata")
	codesImport    = protogen.GoImportPath("google.golang.org/grpc/codes")
)

// methodRoute returns the wire method name "/pkg.Service/Method".
func methodRoute(svc *protogen.Service, m *protogen.Method) string {
	return "/" + string(svc.Desc.FullName()) + "/" + string(m.Desc.Name())
}

// kind classifies an RPC method by its streaming flags.
type rpcKind int

const (
	kindUnary rpcKind = iota
	kindServerStream
	kindClientStream
	kindBidi
)

func methodKind(m *protogen.Method) rpcKind {
	c, s := m.Desc.IsStreamingClient(), m.Desc.IsStreamingServer()
	switch {
	case !c && !s:
		return kindUnary
	case !c && s:
		return kindServerStream
	case c && !s:
		return kindClientStream
	default:
		return kindBidi
	}
}

// strconvQuote returns s as a Go double-quoted string literal for emission.
func strconvQuote(s string) string { return strconv.Quote(s) }

// unexport lowercases the first rune of s (EchoService -> echoService).
func unexport(s string) string {
	if s == "" {
		return s
	}
	r, n := utf8.DecodeRuneInString(s)
	return string(unicode.ToLower(r)) + s[n:]
}
