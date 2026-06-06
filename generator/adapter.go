package generator

import "google.golang.org/protobuf/compiler/protogen"

// genGRPCBridge emits <Svc>FromGRPC(impl <Svc>Server) <Svc>Handler plus, for
// each streaming method, a concrete grpc.ServerStream shim over *wsrpc.Stream.
func (g *Generator) genGRPCBridge(gf *protogen.GeneratedFile, svc *protogen.Service) {
	bridge := unexport(svc.GoName) + "GRPCBridge"

	// Bridge struct holds the grpc server impl.
	gf.P("// ", bridge, " adapts a gRPC ", svc.GoName, "Server to a ", svc.GoName, "Handler.")
	gf.P("type ", bridge, " struct {")
	gf.P("\timpl ", svc.GoName, "Server")
	gf.P("}")
	gf.P()

	gf.P("// ", svc.GoName, "FromGRPC wraps a protoc-gen-go-grpc server so it can be")
	gf.P("// registered on a wsrpc.Server via Register", svc.GoName, "Handler.")
	gf.P("func ", svc.GoName, "FromGRPC(impl ", svc.GoName, "Server) ", svc.GoName, "Handler {")
	gf.P("\treturn &", bridge, "{impl: impl}")
	gf.P("}")
	gf.P()

	for _, m := range svc.Methods {
		g.genBridgeMethod(gf, svc, m, bridge)
	}
	for _, m := range svc.Methods {
		if methodKind(m) == kindUnary {
			continue
		}
		g.genGRPCShim(gf, svc, m)
	}
}

func (g *Generator) genBridgeMethod(gf *protogen.GeneratedFile, svc *protogen.Service, m *protogen.Method, bridge string) {
	ctx := gf.QualifiedGoIdent(contextImport.Ident("Context"))
	req := gf.QualifiedGoIdent(m.Input.GoIdent)
	res := gf.QualifiedGoIdent(m.Output.GoIdent)
	wrapper := svc.GoName + "_" + m.GoName + "Server"
	shim := unexport(svc.GoName) + m.GoName + "GRPCShim"

	switch methodKind(m) {
	case kindUnary:
		gf.P("func (b *", bridge, ") ", m.GoName, "(ctx ", ctx, ", req *", req, ") (*", res, ", error) {")
		gf.P("\treturn b.impl.", m.GoName, "(ctx, req)")
		gf.P("}")
		gf.P()
	case kindServerStream:
		gf.P("func (b *", bridge, ") ", m.GoName, "(ctx ", ctx, ", req *", req, ", stream *", wrapper, ") error {")
		gf.P("\treturn b.impl.", m.GoName, "(req, &", shim, "{stream: stream.stream, ctx: ctx})")
		gf.P("}")
		gf.P()
	case kindClientStream:
		gf.P("func (b *", bridge, ") ", m.GoName, "(ctx ", ctx, ", stream *", wrapper, ") (*", res, ", error) {")
		gf.P("\tsh := &", shim, "{stream: stream.stream, ctx: ctx}")
		gf.P("\tif err := b.impl.", m.GoName, "(sh); err != nil {")
		gf.P("\t\treturn nil, err")
		gf.P("\t}")
		gf.P("\tif sh.resp == nil {")
		gf.P("\t\treturn new(", res, "), nil")
		gf.P("\t}")
		gf.P("\treturn sh.resp, nil")
		gf.P("}")
		gf.P()
	case kindBidi:
		gf.P("func (b *", bridge, ") ", m.GoName, "(ctx ", ctx, ", stream *", wrapper, ") error {")
		gf.P("\treturn b.impl.", m.GoName, "(&", shim, "{stream: stream.stream, ctx: ctx})")
		gf.P("}")
		gf.P()
	}
}

// genGRPCShim emits a concrete type implementing the grpc.ServerStream
// interface (plus typed Send/Recv/SendAndClose) over a *wsrpc.Stream so a grpc
// streaming handler can run unchanged.
func (g *Generator) genGRPCShim(gf *protogen.GeneratedFile, svc *protogen.Service, m *protogen.Method) {
	shim := unexport(svc.GoName) + m.GoName + "GRPCShim"
	req := gf.QualifiedGoIdent(m.Input.GoIdent)
	res := gf.QualifiedGoIdent(m.Output.GoIdent)
	streamType := gf.QualifiedGoIdent(wsrpcImport.Ident("Stream"))
	ctx := gf.QualifiedGoIdent(contextImport.Ident("Context"))
	mdType := gf.QualifiedGoIdent(metadataImport.Ident("MD"))
	protoMsg := gf.QualifiedGoIdent(protoImport.Ident("Message"))

	gf.P("// ", shim, " adapts *wsrpc.Stream to the gRPC server stream for ", svc.GoName, ".", m.GoName, ".")
	gf.P("type ", shim, " struct {")
	gf.P("\tstream *", streamType)
	gf.P("\tctx    ", ctx)
	if methodKind(m) == kindClientStream {
		gf.P("\tresp   *", res, " // captured by SendAndClose")
	}
	gf.P("}")
	gf.P()

	// Typed Send (server -> client). client-stream uses SendAndClose instead.
	if methodKind(m) != kindClientStream {
		gf.P("func (x *", shim, ") Send(msg *", res, ") error {")
		gf.P("\treturn x.stream.Send(msg)")
		gf.P("}")
		gf.P()
	}

	// Typed Recv (client -> server). server-stream has no Recv on its grpc iface
	// but emitting it is harmless and keeps a uniform shape.
	gf.P("func (x *", shim, ") Recv() (*", req, ", error) {")
	gf.P("\tmsg := new(", req, ")")
	gf.P("\tif err := x.stream.Recv(msg); err != nil {")
	gf.P("\t\treturn nil, err")
	gf.P("\t}")
	gf.P("\treturn msg, nil")
	gf.P("}")
	gf.P()

	if methodKind(m) == kindClientStream {
		gf.P("func (x *", shim, ") SendAndClose(msg *", res, ") error {")
		gf.P("\tx.resp = msg")
		gf.P("\treturn nil")
		gf.P("}")
		gf.P()
	}

	// grpc.ServerStream surface.
	gf.P("func (x *", shim, ") Context() ", ctx, " { return x.ctx }")
	gf.P()
	gf.P("func (x *", shim, ") SetHeader(", mdType, ") error { return nil }")
	gf.P()
	gf.P("func (x *", shim, ") SendHeader(", mdType, ") error { return nil }")
	gf.P()
	gf.P("func (x *", shim, ") SetTrailer(", mdType, ") {}")
	gf.P()
	// SendMsg/RecvMsg operate on any proto.Message by type-asserting.
	gf.P("func (x *", shim, ") SendMsg(m any) error {")
	gf.P("\tpm, ok := m.(", protoMsg, ")")
	gf.P("\tif !ok {")
	gf.P("\t\treturn ", g.errorfRef(gf), "(", codesUnimplemented(gf), ", \"wsrpc: SendMsg expected proto.Message\")")
	gf.P("\t}")
	gf.P("\treturn x.stream.Send(pm)")
	gf.P("}")
	gf.P()
	gf.P("func (x *", shim, ") RecvMsg(m any) error {")
	gf.P("\tpm, ok := m.(", protoMsg, ")")
	gf.P("\tif !ok {")
	gf.P("\t\treturn ", g.errorfRef(gf), "(", codesUnimplemented(gf), ", \"wsrpc: RecvMsg expected proto.Message\")")
	gf.P("\t}")
	gf.P("\treturn x.stream.Recv(pm)")
	gf.P("}")
	gf.P()
}

// errorfRef returns the qualified wsrpc.Errorf identifier.
func (g *Generator) errorfRef(gf *protogen.GeneratedFile) string {
	return gf.QualifiedGoIdent(wsrpcImport.Ident("Errorf"))
}

// codesUnimplemented returns the qualified codes.Unimplemented identifier.
func codesUnimplemented(gf *protogen.GeneratedFile) string {
	return gf.QualifiedGoIdent(codesImport.Ident("Unimplemented"))
}
