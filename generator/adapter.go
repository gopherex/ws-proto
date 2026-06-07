package generator

import "google.golang.org/protobuf/compiler/protogen"

// genGRPCBridge emits <Svc>FromGRPC(impl <Svc>Server, opts ...wsrpc.BridgeOption)
// <Svc>Handler. The bridge runs optional gRPC server interceptors around each
// RPC, mirroring grpc-go's own generated handler/interceptor flow. A single
// generic grpc.ServerStream shim per service adapts *wsrpc.Stream so an
// interceptor may wrap it transparently.
func (g *Generator) genGRPCBridge(gf *protogen.GeneratedFile, svc *protogen.Service) {
	bridge := unexport(svc.GoName) + "GRPCBridge"

	cfgType := gf.QualifiedGoIdent(wsrpcImport.Ident("BridgeConfig"))
	optType := gf.QualifiedGoIdent(wsrpcImport.Ident("BridgeOption"))
	applyOpts := gf.QualifiedGoIdent(wsrpcImport.Ident("ApplyBridgeOptions"))

	// Bridge struct holds the grpc server impl plus the resolved config.
	gf.P("// ", bridge, " adapts a gRPC ", svc.GoName, "Server to a ", svc.GoName, "Handler.")
	gf.P("type ", bridge, " struct {")
	gf.P("\timpl ", svc.GoName, "Server")
	gf.P("\tcfg  ", cfgType)
	gf.P("}")
	gf.P()

	gf.P("// ", svc.GoName, "FromGRPC wraps a protoc-gen-go-grpc server so it can be")
	gf.P("// registered on a wsrpc.Server via Register", svc.GoName, "Handler. Optional")
	gf.P("// wsrpc.BridgeOption values install gRPC server interceptors run around RPCs.")
	gf.P("func ", svc.GoName, "FromGRPC(impl ", svc.GoName, "Server, opts ...", optType, ") ", svc.GoName, "Handler {")
	gf.P("\treturn &", bridge, "{impl: impl, cfg: ", applyOpts, "(opts...)}")
	gf.P("}")
	gf.P()

	// One generic ServerStream shim per service.
	g.genGRPCStreamShim(gf, svc)

	for _, m := range svc.Methods {
		g.genBridgeMethod(gf, svc, m, bridge)
	}
}

// grpcStreamName returns the per-service generic grpc.ServerStream shim type name.
func grpcStreamName(svc *protogen.Service) string {
	return unexport(svc.GoName) + "GRPCStream"
}

// genGRPCStreamShim emits a single type per service implementing grpc.ServerStream
// over *wsrpc.Stream. When capturing is set (client-streaming), SendMsg captures
// the response instead of sending it, so the bridge can return it through the
// wsrpc handler contract.
func (g *Generator) genGRPCStreamShim(gf *protogen.GeneratedFile, svc *protogen.Service) {
	shim := grpcStreamName(svc)
	streamType := gf.QualifiedGoIdent(wsrpcImport.Ident("Stream"))
	ctx := gf.QualifiedGoIdent(contextImport.Ident("Context"))
	mdType := gf.QualifiedGoIdent(metadataImport.Ident("MD"))
	protoMsg := gf.QualifiedGoIdent(protoImport.Ident("Message"))
	codeInternal := gf.QualifiedGoIdent(codesImport.Ident("Internal"))

	gf.P("// ", shim, " adapts *wsrpc.Stream to grpc.ServerStream. When capturing is set")
	gf.P("// (client-streaming), SendMsg captures the response instead of sending it, so")
	gf.P("// the bridge can return it through the wsrpc handler contract.")
	flattenMD := gf.QualifiedGoIdent(wsrpcImport.Ident("FlattenMD"))

	gf.P("type ", shim, " struct {")
	gf.P("\tstream        *", streamType)
	gf.P("\tctx           ", ctx)
	gf.P("\tcapturing     bool")
	gf.P("\tcaptured      ", protoMsg)
	gf.P("\tpendingHeader ", mdType, " // buffered leading headers, flushed on SendHeader or first SendMsg")
	gf.P("}")
	gf.P()

	gf.P("func (x *", shim, ") Context() ", ctx, " { return x.ctx }")
	gf.P()
	gf.P("// SetHeader buffers leading header metadata; it is sent on the first")
	gf.P("// SendMsg or an explicit SendHeader (gRPC ServerStream semantics).")
	gf.P("func (x *", shim, ") SetHeader(md ", mdType, ") error {")
	gf.P("\tif x.pendingHeader == nil {")
	gf.P("\t\tx.pendingHeader = ", mdType, "{}")
	gf.P("\t}")
	gf.P("\tfor k, v := range md {")
	gf.P("\t\tx.pendingHeader[k] = append(x.pendingHeader[k], v...)")
	gf.P("\t}")
	gf.P("\treturn nil")
	gf.P("}")
	gf.P()
	gf.P("// SendHeader merges md into the pending headers and flushes them as a")
	gf.P("// leading KIND_HEADER frame on the underlying wsrpc stream.")
	gf.P("func (x *", shim, ") SendHeader(md ", mdType, ") error {")
	gf.P("\tif err := x.SetHeader(md); err != nil {")
	gf.P("\t\treturn err")
	gf.P("\t}")
	gf.P("\terr := x.stream.SendHeader(", flattenMD, "(x.pendingHeader))")
	gf.P("\tx.pendingHeader = nil")
	gf.P("\treturn err")
	gf.P("}")
	gf.P()
	gf.P("// SetTrailer merges md into the trailers flushed with the END frame.")
	gf.P("func (x *", shim, ") SetTrailer(md ", mdType, ") {")
	gf.P("\tx.stream.SetTrailer(", flattenMD, "(md))")
	gf.P("}")
	gf.P()

	gf.P("func (x *", shim, ") SendMsg(m any) error {")
	gf.P("\tpm, ok := m.(", protoMsg, ")")
	gf.P("\tif !ok {")
	gf.P("\t\treturn ", g.errorfRef(gf), "(", codeInternal, ", \"wsrpc: SendMsg expected proto.Message\")")
	gf.P("\t}")
	gf.P("\tif x.capturing {")
	gf.P("\t\tx.captured = pm")
	gf.P("\t\treturn nil")
	gf.P("\t}")
	gf.P("\t// Flush any buffered leading headers before the first response message")
	gf.P("\t// (best-effort: ignore FailedPrecondition if headers were already sent).")
	gf.P("\tif x.pendingHeader != nil {")
	gf.P("\t\t_ = x.stream.SendHeader(", flattenMD, "(x.pendingHeader))")
	gf.P("\t\tx.pendingHeader = nil")
	gf.P("\t}")
	gf.P("\treturn x.stream.Send(pm)")
	gf.P("}")
	gf.P()

	gf.P("func (x *", shim, ") RecvMsg(m any) error {")
	gf.P("\tpm, ok := m.(", protoMsg, ")")
	gf.P("\tif !ok {")
	gf.P("\t\treturn ", g.errorfRef(gf), "(", codeInternal, ", \"wsrpc: RecvMsg expected proto.Message\")")
	gf.P("\t}")
	gf.P("\treturn x.stream.Recv(pm)")
	gf.P("}")
	gf.P()
}

func (g *Generator) genBridgeMethod(gf *protogen.GeneratedFile, svc *protogen.Service, m *protogen.Method, bridge string) {
	ctx := gf.QualifiedGoIdent(contextImport.Ident("Context"))
	req := gf.QualifiedGoIdent(m.Input.GoIdent)
	res := gf.QualifiedGoIdent(m.Output.GoIdent)
	wrapper := serverWrapperName(svc, m)
	shim := grpcStreamName(svc)
	route := strconvQuote(methodRoute(svc, m))

	grpcServerStream := gf.QualifiedGoIdent(grpcImport.Ident("ServerStream"))
	genericStream := gf.QualifiedGoIdent(grpcImport.Ident("GenericServerStream"))
	unaryInfo := gf.QualifiedGoIdent(grpcImport.Ident("UnaryServerInfo"))
	streamInfo := gf.QualifiedGoIdent(grpcImport.Ident("StreamServerInfo"))
	codeInternal := gf.QualifiedGoIdent(codesImport.Ident("Internal"))

	switch methodKind(m) {
	case kindUnary:
		gf.P("func (b *", bridge, ") ", m.GoName, "(ctx ", ctx, ", req *", req, ") (*", res, ", error) {")
		gf.P("\tif b.cfg.Unary == nil {")
		gf.P("\t\treturn b.impl.", m.GoName, "(ctx, req)")
		gf.P("\t}")
		gf.P("\tinfo := &", unaryInfo, "{Server: b.impl, FullMethod: ", route, "}")
		gf.P("\tresp, err := b.cfg.Unary(ctx, req, info, func(ctx ", ctx, ", r any) (any, error) {")
		gf.P("\t\treturn b.impl.", m.GoName, "(ctx, r.(*", req, "))")
		gf.P("\t})")
		gf.P("\tif err != nil {")
		gf.P("\t\treturn nil, err")
		gf.P("\t}")
		gf.P("\tout, ok := resp.(*", res, ")")
		gf.P("\tif !ok {")
		gf.P("\t\treturn nil, ", g.errorfRef(gf), "(", codeInternal, ", \"wsrpc: ", svc.GoName, ".", m.GoName, " interceptor returned unexpected response type\")")
		gf.P("\t}")
		gf.P("\treturn out, nil")
		gf.P("}")
		gf.P()

	case kindServerStream:
		gf.P("func (b *", bridge, ") ", m.GoName, "(ctx ", ctx, ", req *", req, ", stream *", wrapper, ") error {")
		gf.P("\tbase := &", shim, "{stream: stream.stream, ctx: ctx}")
		gf.P("\th := func(_ any, ss ", grpcServerStream, ") error {")
		gf.P("\t\treturn b.impl.", m.GoName, "(req, &", genericStream, "[", req, ", ", res, "]{ServerStream: ss})")
		gf.P("\t}")
		gf.P("\tif b.cfg.Stream == nil {")
		gf.P("\t\treturn h(b.impl, base)")
		gf.P("\t}")
		gf.P("\tinfo := &", streamInfo, "{FullMethod: ", route, ", IsServerStream: true}")
		gf.P("\treturn b.cfg.Stream(b.impl, base, info, h)")
		gf.P("}")
		gf.P()

	case kindClientStream:
		gf.P("func (b *", bridge, ") ", m.GoName, "(ctx ", ctx, ", stream *", wrapper, ") (*", res, ", error) {")
		gf.P("\tbase := &", shim, "{stream: stream.stream, ctx: ctx, capturing: true}")
		gf.P("\th := func(_ any, ss ", grpcServerStream, ") error {")
		gf.P("\t\treturn b.impl.", m.GoName, "(&", genericStream, "[", req, ", ", res, "]{ServerStream: ss})")
		gf.P("\t}")
		gf.P("\tvar err error")
		gf.P("\tif b.cfg.Stream == nil {")
		gf.P("\t\terr = h(b.impl, base)")
		gf.P("\t} else {")
		gf.P("\t\tinfo := &", streamInfo, "{FullMethod: ", route, ", IsClientStream: true}")
		gf.P("\t\terr = b.cfg.Stream(b.impl, base, info, h)")
		gf.P("\t}")
		gf.P("\tif err != nil {")
		gf.P("\t\treturn nil, err")
		gf.P("\t}")
		gf.P("\tif base.captured == nil {")
		gf.P("\t\treturn nil, ", g.errorfRef(gf), "(", codeInternal, ", \"wsrpc: ", svc.GoName, ".", m.GoName, " handler returned without SendAndClose\")")
		gf.P("\t}")
		gf.P("\treturn base.captured.(*", res, "), nil")
		gf.P("}")
		gf.P()

	case kindBidi:
		gf.P("func (b *", bridge, ") ", m.GoName, "(ctx ", ctx, ", stream *", wrapper, ") error {")
		gf.P("\tbase := &", shim, "{stream: stream.stream, ctx: ctx}")
		gf.P("\th := func(_ any, ss ", grpcServerStream, ") error {")
		gf.P("\t\treturn b.impl.", m.GoName, "(&", genericStream, "[", req, ", ", res, "]{ServerStream: ss})")
		gf.P("\t}")
		gf.P("\tif b.cfg.Stream == nil {")
		gf.P("\t\treturn h(b.impl, base)")
		gf.P("\t}")
		gf.P("\tinfo := &", streamInfo, "{FullMethod: ", route, ", IsClientStream: true, IsServerStream: true}")
		gf.P("\treturn b.cfg.Stream(b.impl, base, info, h)")
		gf.P("}")
		gf.P()
	}
}

// errorfRef returns the qualified wsrpc.Errorf identifier.
func (g *Generator) errorfRef(gf *protogen.GeneratedFile) string {
	return gf.QualifiedGoIdent(wsrpcImport.Ident("Errorf"))
}
