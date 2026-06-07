package generator

import "google.golang.org/protobuf/compiler/protogen"

func (g *Generator) genClient(gf *protogen.GeneratedFile, svc *protogen.Service) {
	ctx := gf.QualifiedGoIdent(contextImport.Ident("Context"))
	ccType := gf.QualifiedGoIdent(wsrpcImport.Ident("ClientConn"))
	callOpt := gf.QualifiedGoIdent(wsrpcImport.Ident("CallOption"))

	// --- client interface ---
	iface := clientIfaceName(svc)
	gf.P("// ", iface, " is the client API for the ", svc.GoName, " service.")
	gf.P("type ", iface, " interface {")
	for _, m := range svc.Methods {
		req := gf.QualifiedGoIdent(m.Input.GoIdent)
		res := gf.QualifiedGoIdent(m.Output.GoIdent)
		switch methodKind(m) {
		case kindUnary:
			gf.P("\t", m.GoName, "(ctx ", ctx, ", req *", req, ", opts ...", callOpt, ") (*", res, ", error)")
		case kindServerStream:
			gf.P("\t", m.GoName, "(ctx ", ctx, ", req *", req, ", opts ...", callOpt, ") (*", clientWrapperName(svc, m), ", error)")
		case kindClientStream, kindBidi:
			gf.P("\t", m.GoName, "(ctx ", ctx, ", opts ...", callOpt, ") (*", clientWrapperName(svc, m), ", error)")
		}
	}
	gf.P("}")
	gf.P()

	// --- impl struct + ctor ---
	impl := clientImplName(svc)
	gf.P("type ", impl, " struct {")
	gf.P("\tcc *", ccType)
	gf.P("}")
	gf.P()
	gf.P("// ", clientCtorName(svc), " returns a ", iface, " backed by cc.")
	gf.P("func ", clientCtorName(svc), "(cc *", ccType, ") ", iface, " {")
	gf.P("\treturn &", impl, "{cc: cc}")
	gf.P("}")
	gf.P()

	// --- typed client stream wrappers ---
	for _, m := range svc.Methods {
		if methodKind(m) == kindUnary {
			continue
		}
		g.genClientStreamWrapper(gf, svc, m)
	}

	// --- per-method client funcs ---
	for _, m := range svc.Methods {
		g.genClientMethod(gf, svc, m, impl)
	}
}

// genClientStreamWrapper emits <Svc>_<Method>Client wrapping *wsrpc.Stream.
// Send encodes a request; Recv decodes a response; CloseSend forwards.
func (g *Generator) genClientStreamWrapper(gf *protogen.GeneratedFile, svc *protogen.Service, m *protogen.Method) {
	name := clientWrapperName(svc, m)
	req := gf.QualifiedGoIdent(m.Input.GoIdent)
	res := gf.QualifiedGoIdent(m.Output.GoIdent)
	stream := gf.QualifiedGoIdent(wsrpcImport.Ident("Stream"))

	gf.P("// ", name, " is the typed client stream for ", svc.GoName, ".", m.GoName, ".")
	gf.P("type ", name, " struct {")
	gf.P("\tstream *", stream)
	gf.P("}")
	gf.P()

	gf.P("func (x *", name, ") Send(msg *", req, ") error {")
	gf.P("\treturn x.stream.Send(msg)")
	gf.P("}")
	gf.P()

	gf.P("func (x *", name, ") Recv() (*", res, ", error) {")
	gf.P("\tmsg := new(", res, ")")
	gf.P("\tif err := x.stream.Recv(msg); err != nil {")
	gf.P("\t\treturn nil, err")
	gf.P("\t}")
	gf.P("\treturn msg, nil")
	gf.P("}")
	gf.P()

	gf.P("func (x *", name, ") CloseSend() error {")
	gf.P("\treturn x.stream.CloseSend()")
	gf.P("}")
	gf.P()

	gf.P("// Header returns the leading response headers (KIND_HEADER), if the server sent any.")
	gf.P("func (x *", name, ") Header() map[string]string {")
	gf.P("\treturn x.stream.Header()")
	gf.P("}")
	gf.P()

	gf.P("// Trailer returns the response trailers carried on the END frame.")
	gf.P("func (x *", name, ") Trailer() map[string]string {")
	gf.P("\treturn x.stream.Trailer()")
	gf.P("}")
	gf.P()

	// For client-stream, the response is collected by CloseAndRecv.
	if methodKind(m) == kindClientStream {
		gf.P("func (x *", name, ") CloseAndRecv() (*", res, ", error) {")
		gf.P("\tif err := x.stream.CloseSend(); err != nil {")
		gf.P("\t\treturn nil, err")
		gf.P("\t}")
		gf.P("\treturn x.Recv()")
		gf.P("}")
		gf.P()
	}
}

// genClientMethod emits one client func per method on the impl struct.
func (g *Generator) genClientMethod(gf *protogen.GeneratedFile, svc *protogen.Service, m *protogen.Method, impl string) {
	ctx := gf.QualifiedGoIdent(contextImport.Ident("Context"))
	ioEOF := gf.QualifiedGoIdent(ioImport.Ident("EOF"))
	callOpt := gf.QualifiedGoIdent(wsrpcImport.Ident("CallOption"))
	callHeaders := gf.QualifiedGoIdent(wsrpcImport.Ident("CallHeaders"))
	req := gf.QualifiedGoIdent(m.Input.GoIdent)
	res := gf.QualifiedGoIdent(m.Output.GoIdent)
	route := methodRoute(svc, m)
	wrapper := clientWrapperName(svc, m)

	switch methodKind(m) {
	case kindUnary:
		gf.P("func (c *", impl, ") ", m.GoName, "(ctx ", ctx, ", req *", req, ", opts ...", callOpt, ") (*", res, ", error) {")
		gf.P("\theaders := ", callHeaders, "(opts...)")
		gf.P("\ts, err := c.cc.NewStream(ctx, ", strconvQuote(route), ", headers)")
		gf.P("\tif err != nil {")
		gf.P("\t\treturn nil, err")
		gf.P("\t}")
		gf.P("\tif err := s.Send(req); err != nil {")
		gf.P("\t\treturn nil, err")
		gf.P("\t}")
		gf.P("\tif err := s.CloseSend(); err != nil {")
		gf.P("\t\treturn nil, err")
		gf.P("\t}")
		gf.P("\tres := new(", res, ")")
		gf.P("\tif err := s.Recv(res); err != nil {")
		gf.P("\t\treturn nil, err")
		gf.P("\t}")
		// Drain the trailing END (io.EOF) so the stream closes cleanly.
		gf.P("\tif err := s.Recv(new(", res, ")); err != nil && err != ", ioEOF, " {")
		gf.P("\t\treturn nil, err")
		gf.P("\t}")
		gf.P("\treturn res, nil")
		gf.P("}")
		gf.P()
	case kindServerStream:
		gf.P("func (c *", impl, ") ", m.GoName, "(ctx ", ctx, ", req *", req, ", opts ...", callOpt, ") (*", wrapper, ", error) {")
		gf.P("\theaders := ", callHeaders, "(opts...)")
		gf.P("\ts, err := c.cc.NewStream(ctx, ", strconvQuote(route), ", headers)")
		gf.P("\tif err != nil {")
		gf.P("\t\treturn nil, err")
		gf.P("\t}")
		gf.P("\tif err := s.Send(req); err != nil {")
		gf.P("\t\treturn nil, err")
		gf.P("\t}")
		gf.P("\tif err := s.CloseSend(); err != nil {")
		gf.P("\t\treturn nil, err")
		gf.P("\t}")
		gf.P("\treturn &", wrapper, "{stream: s}, nil")
		gf.P("}")
		gf.P()
	case kindClientStream, kindBidi:
		gf.P("func (c *", impl, ") ", m.GoName, "(ctx ", ctx, ", opts ...", callOpt, ") (*", wrapper, ", error) {")
		gf.P("\theaders := ", callHeaders, "(opts...)")
		gf.P("\ts, err := c.cc.NewStream(ctx, ", strconvQuote(route), ", headers)")
		gf.P("\tif err != nil {")
		gf.P("\t\treturn nil, err")
		gf.P("\t}")
		gf.P("\treturn &", wrapper, "{stream: s}, nil")
		gf.P("}")
		gf.P()
	}
}
