package generator

import "google.golang.org/protobuf/compiler/protogen"

func (g *Generator) genService(gf *protogen.GeneratedFile, svc *protogen.Service) {
	// --- handler interface ---
	gf.P("// ", svc.GoName, "Handler is the server API for the ", svc.GoName, " service.")
	gf.P("type ", svc.GoName, "Handler interface {")
	for _, m := range svc.Methods {
		req := gf.QualifiedGoIdent(m.Input.GoIdent)
		res := gf.QualifiedGoIdent(m.Output.GoIdent)
		ctx := gf.QualifiedGoIdent(contextImport.Ident("Context"))
		switch methodKind(m) {
		case kindUnary:
			gf.P("\t", m.GoName, "(ctx ", ctx, ", req *", req, ") (*", res, ", error)")
		case kindServerStream:
			gf.P("\t", m.GoName, "(ctx ", ctx, ", req *", req, ", stream *", svc.GoName, "_", m.GoName, "Server) error")
		case kindClientStream:
			gf.P("\t", m.GoName, "(ctx ", ctx, ", stream *", svc.GoName, "_", m.GoName, "Server) (*", res, ", error)")
		case kindBidi:
			gf.P("\t", m.GoName, "(ctx ", ctx, ", stream *", svc.GoName, "_", m.GoName, "Server) error")
		}
	}
	gf.P("}")
	gf.P()

	// --- typed server stream wrappers (one per streaming method) ---
	for _, m := range svc.Methods {
		if methodKind(m) == kindUnary {
			continue
		}
		g.genServerStreamWrapper(gf, svc, m)
	}

	// --- registrar ---
	g.genRegistrar(gf, svc)
}

// genServerStreamWrapper emits <Svc>_<Method>Server wrapping *wsrpc.Stream with
// typed Send/Recv. Recv decodes a request message; Send encodes a response.
func (g *Generator) genServerStreamWrapper(gf *protogen.GeneratedFile, svc *protogen.Service, m *protogen.Method) {
	name := svc.GoName + "_" + m.GoName + "Server"
	req := gf.QualifiedGoIdent(m.Input.GoIdent)
	res := gf.QualifiedGoIdent(m.Output.GoIdent)
	stream := gf.QualifiedGoIdent(wsrpcImport.Ident("Stream"))

	gf.P("// ", name, " is the typed server stream for ", svc.GoName, ".", m.GoName, ".")
	gf.P("type ", name, " struct {")
	gf.P("\tstream *", stream)
	gf.P("}")
	gf.P()

	// Send (server -> client response)
	gf.P("func (x *", name, ") Send(msg *", res, ") error {")
	gf.P("\treturn x.stream.Send(msg)")
	gf.P("}")
	gf.P()

	// Recv (client -> server request); only emitted for client-stream & bidi,
	// but emitting it for server-stream too is harmless and uniform.
	gf.P("func (x *", name, ") Recv() (*", req, ", error) {")
	gf.P("\tmsg := new(", req, ")")
	gf.P("\tif err := x.stream.Recv(msg); err != nil {")
	gf.P("\t\treturn nil, err")
	gf.P("\t}")
	gf.P("\treturn msg, nil")
	gf.P("}")
	gf.P()

	// Context passthrough.
	ctx := gf.QualifiedGoIdent(contextImport.Ident("Context"))
	gf.P("func (x *", name, ") Context() ", ctx, " {")
	gf.P("\treturn x.stream.Context()")
	gf.P("}")
	gf.P()
}

// genRegistrar emits Register<Svc>Handler with a wsrpc.Handler adapter per method.
func (g *Generator) genRegistrar(gf *protogen.GeneratedFile, svc *protogen.Service) {
	srvType := gf.QualifiedGoIdent(wsrpcImport.Ident("Server"))
	streamType := gf.QualifiedGoIdent(wsrpcImport.Ident("Stream"))
	ctx := gf.QualifiedGoIdent(contextImport.Ident("Context"))
	ioEOF := gf.QualifiedGoIdent(ioImport.Ident("EOF"))

	gf.P("// Register", svc.GoName, "Handler registers impl on srv for all ", svc.GoName, " methods.")
	gf.P("func Register", svc.GoName, "Handler(srv *", srvType, ", impl ", svc.GoName, "Handler) {")
	for _, m := range svc.Methods {
		route := methodRoute(svc, m)
		wrapper := svc.GoName + "_" + m.GoName + "Server"
		req := gf.QualifiedGoIdent(m.Input.GoIdent)
		gf.P("\tsrv.Register(", strconvQuote(route), ", func(ctx ", ctx, ", s *", streamType, ") error {")
		switch methodKind(m) {
		case kindUnary:
			// Decode the single request, dispatch, send the single response.
			gf.P("\t\treq := new(", req, ")")
			gf.P("\t\tif err := s.Recv(req); err != nil && err != ", ioEOF, " {")
			gf.P("\t\t\treturn err")
			gf.P("\t\t}")
			gf.P("\t\tres, err := impl.", m.GoName, "(ctx, req)")
			gf.P("\t\tif err != nil {")
			gf.P("\t\t\treturn err")
			gf.P("\t\t}")
			gf.P("\t\treturn s.Send(res)")
		case kindServerStream:
			gf.P("\t\treq := new(", req, ")")
			gf.P("\t\tif err := s.Recv(req); err != nil && err != ", ioEOF, " {")
			gf.P("\t\t\treturn err")
			gf.P("\t\t}")
			gf.P("\t\treturn impl.", m.GoName, "(ctx, req, &", wrapper, "{stream: s})")
		case kindClientStream:
			gf.P("\t\tres, err := impl.", m.GoName, "(ctx, &", wrapper, "{stream: s})")
			gf.P("\t\tif err != nil {")
			gf.P("\t\t\treturn err")
			gf.P("\t\t}")
			gf.P("\t\treturn s.Send(res)")
		case kindBidi:
			gf.P("\t\treturn impl.", m.GoName, "(ctx, &", wrapper, "{stream: s})")
		}
		gf.P("\t})")
	}
	gf.P("}")
	gf.P()
}
