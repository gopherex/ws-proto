import type { Schema, GeneratedFile } from "@bufbuild/protoplugin";
import type { DescService, DescMethod } from "@bufbuild/protobuf";

// generateTs is invoked once per plugin run with the full schema. For each
// proto file that declares services we emit a sibling "<name>_ws_pb.ts" next to
// protoc-gen-es's "<name>_pb.ts".
export function generateTs(schema: Schema): void {
  for (const file of schema.files) {
    if (file.services.length === 0) {
      continue; // nothing to generate for service-less files
    }
    const f = schema.generateFile(file.name + "_ws_pb.ts");
    f.preamble(file);

    // Runtime import: WsTransport from @gopherex/ws-proto-transport.
    const WsTransport = f.import("WsTransport", "@gopherex/ws-proto-transport");

    printCallOptions(f);

    for (const service of file.services) {
      printClient(f, service, WsTransport);
    }
  }
}

type ImportSymbol = ReturnType<GeneratedFile["import"]>;

// printCallOptions emits the shared CallOptions interface once per generated
// file: request metadata (headers), AbortSignal-based cancellation, and a
// response-trailer callback.
function printCallOptions(f: GeneratedFile): void {
  f.print(f.export("interface", "CallOptions"), " {");
  f.print("  /** Request metadata sent as headers on the opening frame. */");
  f.print("  headers?: Record<string, string>;");
  f.print("  /** Aborts the RPC (sends RST); recv()/iteration reject. */");
  f.print("  signal?: AbortSignal;");
  f.print("  /** Invoked with the response trailers once the server ends the stream. */");
  f.print("  onTrailer?: (trailer: Record<string, string>) => void;");
  f.print("}");
  f.print();
}

// Fully-qualified wire path "/pkg.Service/Method".
function wirePath(service: DescService, method: DescMethod): string {
  return `/${service.typeName}/${method.name}`;
}

function printClient(
  f: GeneratedFile,
  service: DescService,
  WsTransport: ImportSymbol,
): void {
  const className = service.name + "Client";
  f.print(f.jsDoc(service));
  f.print(f.export("class", className), " {");
  f.print("  constructor(private readonly transport: ", WsTransport, ") {}");
  f.print();

  for (const method of service.methods) {
    switch (method.methodKind) {
      case "unary":
        printUnary(f, service, method);
        break;
      case "server_streaming":
        printServerStreaming(f, service, method);
        break;
      case "client_streaming":
        printClientStreaming(f, service, method);
        break;
      case "bidi_streaming":
        printBidiStreaming(f, service, method);
        break;
    }
    f.print();
  }
  f.print("}");
  f.print();
}

// unary: async getUser(req, options?): Promise<Res>
function printUnary(
  f: GeneratedFile,
  service: DescService,
  method: DescMethod,
): void {
  const inT = f.importShape(method.input);
  const outT = f.importShape(method.output);
  const inSchema = f.importSchema(method.input);
  const outSchema = f.importSchema(method.output);
  const { toBinary, fromBinary } = f.runtime;
  f.print(f.jsDoc(method, "  "));
  f.print("  async ", method.localName, "(req: ", inT, ", options?: CallOptions): Promise<", outT, "> {");
  f.print("    const stream = this.transport.openStream(", f.string(wirePath(service, method)), ", { headers: options?.headers, signal: options?.signal });");
  f.print("    stream.send(", toBinary, "(", inSchema, ", req));");
  f.print("    stream.closeSend();");
  f.print("    const bytes = await stream.recv();");
  f.print("    if (bytes === null) {");
  f.print("      throw new Error(", f.string(`${service.typeName}.${method.name}: server closed stream without a response`), ");");
  f.print("    }");
  f.print("    const message = ", fromBinary, "(", outSchema, ", bytes);");
  f.print("    if (options?.onTrailer) { options.onTrailer(await stream.responseHeaders()); }");
  f.print("    return message;");
  f.print("  }");
}

// server_streaming: watch(req, options?): AsyncIterable<Res>
function printServerStreaming(
  f: GeneratedFile,
  service: DescService,
  method: DescMethod,
): void {
  const inT = f.importShape(method.input);
  const outT = f.importShape(method.output);
  const inSchema = f.importSchema(method.input);
  const outSchema = f.importSchema(method.output);
  const { toBinary, fromBinary } = f.runtime;
  f.print(f.jsDoc(method, "  "));
  f.print("  async *", method.localName, "(req: ", inT, ", options?: CallOptions): AsyncIterable<", outT, "> {");
  f.print("    const stream = this.transport.openStream(", f.string(wirePath(service, method)), ", { headers: options?.headers, signal: options?.signal });");
  f.print("    stream.send(", toBinary, "(", inSchema, ", req));");
  f.print("    stream.closeSend();");
  f.print("    for await (const bytes of stream) {");
  f.print("      yield ", fromBinary, "(", outSchema, ", bytes);");
  f.print("    }");
  f.print("    if (options?.onTrailer) { options.onTrailer(await stream.responseHeaders()); }");
  f.print("  }");
}

// client_streaming: upload(reqs, options?): Promise<Res>
function printClientStreaming(
  f: GeneratedFile,
  service: DescService,
  method: DescMethod,
): void {
  const inT = f.importShape(method.input);
  const outT = f.importShape(method.output);
  const inSchema = f.importSchema(method.input);
  const outSchema = f.importSchema(method.output);
  const { toBinary, fromBinary } = f.runtime;
  f.print(f.jsDoc(method, "  "));
  f.print("  async ", method.localName, "(reqs: AsyncIterable<", inT, ">, options?: CallOptions): Promise<", outT, "> {");
  f.print("    const stream = this.transport.openStream(", f.string(wirePath(service, method)), ", { headers: options?.headers, signal: options?.signal });");
  f.print("    try {");
  f.print("      for await (const req of reqs) {");
  f.print("        stream.send(", toBinary, "(", inSchema, ", req));");
  f.print("      }");
  f.print("    } catch (err) {");
  f.print("      stream.cancel(); // abort the RPC so the server-side stream does not leak");
  f.print("      throw err;");
  f.print("    }");
  f.print("    stream.closeSend();");
  f.print("    const bytes = await stream.recv();");
  f.print("    if (bytes === null) {");
  f.print("      throw new Error(", f.string(`${service.typeName}.${method.name}: server closed stream without a response`), ");");
  f.print("    }");
  f.print("    const message = ", fromBinary, "(", outSchema, ", bytes);");
  f.print("    if (options?.onTrailer) { options.onTrailer(await stream.responseHeaders()); }");
  f.print("    return message;");
  f.print("  }");
}

// bidi_streaming: chat(reqs, options?): AsyncIterable<Res>
function printBidiStreaming(
  f: GeneratedFile,
  service: DescService,
  method: DescMethod,
): void {
  const inT = f.importShape(method.input);
  const outT = f.importShape(method.output);
  const inSchema = f.importSchema(method.input);
  const outSchema = f.importSchema(method.output);
  const { toBinary, fromBinary } = f.runtime;
  f.print(f.jsDoc(method, "  "));
  f.print("  async *", method.localName, "(reqs: AsyncIterable<", inT, ">, options?: CallOptions): AsyncIterable<", outT, "> {");
  f.print("    const stream = this.transport.openStream(", f.string(wirePath(service, method)), ", { headers: options?.headers, signal: options?.signal });");
  // Pump request messages concurrently so the read loop below can interleave.
  // On pump failure cancel the stream: that ends the read loop promptly (no
  // hang) and we re-raise the captured error after the loop unwinds.
  f.print("    let pumpError: unknown;");
  f.print("    const pump = (async () => {");
  f.print("      try {");
  f.print("        for await (const req of reqs) {");
  f.print("          stream.send(", toBinary, "(", inSchema, ", req));");
  f.print("        }");
  f.print("        stream.closeSend();");
  f.print("      } catch (err) {");
  f.print("        pumpError = err;");
  f.print("        stream.cancel();");
  f.print("      }");
  f.print("    })();");
  f.print("    try {");
  f.print("      for await (const bytes of stream) {");
  f.print("        yield ", fromBinary, "(", outSchema, ", bytes);");
  f.print("      }");
  f.print("    } finally {");
  f.print("      await pump;");
  f.print("    }");
  f.print("    if (options?.onTrailer) { options.onTrailer(await stream.responseHeaders()); }");
  f.print("    if (pumpError !== undefined) {");
  f.print("      throw pumpError;");
  f.print("    }");
  f.print("  }");
}
