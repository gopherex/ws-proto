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

// streamKindIdent returns the wsrpc.StreamKind* constant for a streaming method.
func streamKindIdent(gf *protogen.GeneratedFile, m *protogen.Method) string {
	switch methodKind(m) {
	case kindServerStream:
		return gf.QualifiedGoIdent(wsrpcImport.Ident("StreamKindServerStream"))
	case kindClientStream:
		return gf.QualifiedGoIdent(wsrpcImport.Ident("StreamKindClientStream"))
	default: // bidi
		return gf.QualifiedGoIdent(wsrpcImport.Ident("StreamKindBidiStream"))
	}
}

// genClientStreamWrapper emits <Svc>_<Method>Client wrapping a
// wsrpc.StreamingClientConn (so client interceptors can wrap Send/Receive).
func (g *Generator) genClientStreamWrapper(gf *protogen.GeneratedFile, svc *protogen.Service, m *protogen.Method) {
	name := clientWrapperName(svc, m)
	req := gf.QualifiedGoIdent(m.Input.GoIdent)
	res := gf.QualifiedGoIdent(m.Output.GoIdent)
	connType := gf.QualifiedGoIdent(wsrpcImport.Ident("StreamingClientConn"))

	gf.P("// ", name, " is the typed client stream for ", svc.GoName, ".", m.GoName, ".")
	gf.P("type ", name, " struct {")
	gf.P("\tconn ", connType)
	gf.P("}")
	gf.P()

	gf.P("func (x *", name, ") Send(msg *", req, ") error {")
	gf.P("\treturn x.conn.Send(msg)")
	gf.P("}")
	gf.P()

	gf.P("func (x *", name, ") Recv() (*", res, ", error) {")
	gf.P("\tmsg := new(", res, ")")
	gf.P("\tif err := x.conn.Receive(msg); err != nil {")
	gf.P("\t\treturn nil, err")
	gf.P("\t}")
	gf.P("\treturn msg, nil")
	gf.P("}")
	gf.P()

	gf.P("func (x *", name, ") CloseSend() error {")
	gf.P("\treturn x.conn.CloseRequest()")
	gf.P("}")
	gf.P()

	gf.P("// Header returns the leading response headers (KIND_HEADER), if the server sent any.")
	gf.P("func (x *", name, ") Header() map[string]string {")
	gf.P("\treturn x.conn.ResponseHeader()")
	gf.P("}")
	gf.P()

	gf.P("// Trailer returns the response trailers carried on the END frame.")
	gf.P("func (x *", name, ") Trailer() map[string]string {")
	gf.P("\treturn x.conn.ResponseTrailer()")
	gf.P("}")
	gf.P()

	// For client-stream, the response is collected by CloseAndRecv.
	if methodKind(m) == kindClientStream {
		gf.P("func (x *", name, ") CloseAndRecv() (*", res, ", error) {")
		gf.P("\tif err := x.conn.CloseRequest(); err != nil {")
		gf.P("\t\treturn nil, err")
		gf.P("\t}")
		gf.P("\treturn x.Recv()")
		gf.P("}")
		gf.P()
	}
}

// genClientMethod emits one client func per method on the impl struct, routing
// through the wsrpc dispatch helpers so client interceptors run.
func (g *Generator) genClientMethod(gf *protogen.GeneratedFile, svc *protogen.Service, m *protogen.Method, impl string) {
	ctx := gf.QualifiedGoIdent(contextImport.Ident("Context"))
	callOpt := gf.QualifiedGoIdent(wsrpcImport.Ident("CallOption"))
	callHeaders := gf.QualifiedGoIdent(wsrpcImport.Ident("CallHeaders"))
	methodSpec := gf.QualifiedGoIdent(wsrpcImport.Ident("MethodSpec"))
	req := gf.QualifiedGoIdent(m.Input.GoIdent)
	res := gf.QualifiedGoIdent(m.Output.GoIdent)
	route := methodRoute(svc, m)
	wrapper := clientWrapperName(svc, m)

	switch methodKind(m) {
	case kindUnary:
		invokeUnary := gf.QualifiedGoIdent(wsrpcImport.Ident("InvokeUnary"))
		protoMsg := gf.QualifiedGoIdent(protoImport.Ident("Message"))
		unaryKind := gf.QualifiedGoIdent(wsrpcImport.Ident("StreamKindUnary"))
		gf.P("func (c *", impl, ") ", m.GoName, "(ctx ", ctx, ", req *", req, ", opts ...", callOpt, ") (*", res, ", error) {")
		gf.P("\tout, err := ", invokeUnary, "(ctx, c.cc, ", methodSpec, "{Route: ", strconvQuote(route), ", Kind: ", unaryKind, "}, req, func() ", protoMsg, " { return new(", res, ") }, ", callHeaders, "(opts...))")
		gf.P("\tif err != nil {")
		gf.P("\t\treturn nil, err")
		gf.P("\t}")
		gf.P("\treturn out.(*", res, "), nil")
		gf.P("}")
		gf.P()
	case kindServerStream:
		openStream := gf.QualifiedGoIdent(wsrpcImport.Ident("OpenStreamingClient"))
		gf.P("func (c *", impl, ") ", m.GoName, "(ctx ", ctx, ", req *", req, ", opts ...", callOpt, ") (*", wrapper, ", error) {")
		gf.P("\tconn, err := ", openStream, "(ctx, c.cc, ", methodSpec, "{Route: ", strconvQuote(route), ", Kind: ", streamKindIdent(gf, m), "}, ", callHeaders, "(opts...))")
		gf.P("\tif err != nil {")
		gf.P("\t\treturn nil, err")
		gf.P("\t}")
		gf.P("\tif err := conn.Send(req); err != nil {")
		gf.P("\t\treturn nil, err")
		gf.P("\t}")
		gf.P("\tif err := conn.CloseRequest(); err != nil {")
		gf.P("\t\treturn nil, err")
		gf.P("\t}")
		gf.P("\treturn &", wrapper, "{conn: conn}, nil")
		gf.P("}")
		gf.P()
	case kindClientStream, kindBidi:
		openStream := gf.QualifiedGoIdent(wsrpcImport.Ident("OpenStreamingClient"))
		gf.P("func (c *", impl, ") ", m.GoName, "(ctx ", ctx, ", opts ...", callOpt, ") (*", wrapper, ", error) {")
		gf.P("\tconn, err := ", openStream, "(ctx, c.cc, ", methodSpec, "{Route: ", strconvQuote(route), ", Kind: ", streamKindIdent(gf, m), "}, ", callHeaders, "(opts...))")
		gf.P("\tif err != nil {")
		gf.P("\t\treturn nil, err")
		gf.P("\t}")
		gf.P("\treturn &", wrapper, "{conn: conn}, nil")
		gf.P("}")
		gf.P()
	}
}
