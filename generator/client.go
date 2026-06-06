package generator

import "google.golang.org/protobuf/compiler/protogen"

func (g *Generator) genClient(gf *protogen.GeneratedFile, svc *protogen.Service) {
	ctx := gf.QualifiedGoIdent(contextImport.Ident("Context"))
	ccType := gf.QualifiedGoIdent(wsrpcImport.Ident("ClientConn"))

	// --- client interface ---
	gf.P("// ", svc.GoName, "Client is the client API for the ", svc.GoName, " service.")
	gf.P("type ", svc.GoName, "Client interface {")
	for _, m := range svc.Methods {
		req := gf.QualifiedGoIdent(m.Input.GoIdent)
		res := gf.QualifiedGoIdent(m.Output.GoIdent)
		switch methodKind(m) {
		case kindUnary:
			gf.P("\t", m.GoName, "(ctx ", ctx, ", req *", req, ") (*", res, ", error)")
		case kindServerStream:
			gf.P("\t", m.GoName, "(ctx ", ctx, ", req *", req, ") (*", svc.GoName, "_", m.GoName, "Client, error)")
		case kindClientStream, kindBidi:
			gf.P("\t", m.GoName, "(ctx ", ctx, ") (*", svc.GoName, "_", m.GoName, "Client, error)")
		}
	}
	gf.P("}")
	gf.P()

	// --- impl struct + ctor ---
	impl := unexport(svc.GoName) + "Client"
	gf.P("type ", impl, " struct {")
	gf.P("\tcc *", ccType)
	gf.P("}")
	gf.P()
	gf.P("// New", svc.GoName, "Client returns a ", svc.GoName, "Client backed by cc.")
	gf.P("func New", svc.GoName, "Client(cc *", ccType, ") ", svc.GoName, "Client {")
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
	name := svc.GoName + "_" + m.GoName + "Client"
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
	req := gf.QualifiedGoIdent(m.Input.GoIdent)
	res := gf.QualifiedGoIdent(m.Output.GoIdent)
	route := methodRoute(svc, m)
	wrapper := svc.GoName + "_" + m.GoName + "Client"

	switch methodKind(m) {
	case kindUnary:
		gf.P("func (c *", impl, ") ", m.GoName, "(ctx ", ctx, ", req *", req, ") (*", res, ", error) {")
		gf.P("\ts, err := c.cc.NewStream(ctx, ", strconvQuote(route), ", nil)")
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
		gf.P("func (c *", impl, ") ", m.GoName, "(ctx ", ctx, ", req *", req, ") (*", wrapper, ", error) {")
		gf.P("\ts, err := c.cc.NewStream(ctx, ", strconvQuote(route), ", nil)")
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
		gf.P("func (c *", impl, ") ", m.GoName, "(ctx ", ctx, ") (*", wrapper, ", error) {")
		gf.P("\ts, err := c.cc.NewStream(ctx, ", strconvQuote(route), ", nil)")
		gf.P("\tif err != nil {")
		gf.P("\t\treturn nil, err")
		gf.P("\t}")
		gf.P("\treturn &", wrapper, "{stream: s}, nil")
		gf.P("}")
		gf.P()
	}
}
