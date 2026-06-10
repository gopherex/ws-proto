import type { Schema, GeneratedFile } from "@bufbuild/protoplugin";
import type { DescService, DescMethod } from "@bufbuild/protobuf";

const RUNTIME = "@gopherex/ws-proto-transport";

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

    // Runtime imports from @gopherex/ws-proto-transport. The typed client is a
    // thin wrapper over the transport's typed dispatch (unary/stream), which runs
    // the interceptor chain; per-call callback timing lives in the runtime
    // helpers so the generated code stays minimal.
    const rt = {
      WsTransport: f.import("WsTransport", RUNTIME),
      MethodInfo: f.import("MethodInfo", RUNTIME),
      applyUnaryCallbacks: f.import("applyUnaryCallbacks", RUNTIME),
      iterateStream: f.import("iterateStream", RUNTIME),
      firstResponse: f.import("firstResponse", RUNTIME),
    };

    printCallOptions(f);

    for (const service of file.services) {
      printClient(f, service, rt);
    }
  }
}

type ImportSymbol = ReturnType<GeneratedFile["import"]>;

interface Runtime {
  WsTransport: ImportSymbol;
  MethodInfo: ImportSymbol;
  applyUnaryCallbacks: ImportSymbol;
  iterateStream: ImportSymbol;
  firstResponse: ImportSymbol;
}

// printCallOptions emits the shared CallOptions interface once per generated
// file: request metadata (headers), AbortSignal-based cancellation, deadline,
// and the leading-header / trailer callbacks.
function printCallOptions(f: GeneratedFile): void {
  f.print(f.export("interface", "CallOptions"), " {");
  f.print("  /** Request metadata sent as headers on the opening frame. */");
  f.print("  headers?: Record<string, string>;");
  f.print("  /** Aborts the RPC (sends RST); recv()/iteration reject. */");
  f.print("  signal?: AbortSignal;");
  f.print("  /** Per-call deadline in milliseconds; propagated to the server and aborts locally on expiry. */");
  f.print("  timeoutMs?: number;");
  f.print("  /** Invoked with the optional leading response headers (KIND_HEADER) sent before the first message. */");
  f.print("  onHeader?: (header: Record<string, string>) => void;");
  f.print("  /** Invoked with the response trailers once the server ends the stream. */");
  f.print("  onTrailer?: (trailer: Record<string, string>) => void;");
  f.print("}");
  f.print();
}

// methodInfoName returns the module-level identifier for a method's MethodInfo.
function methodInfoName(service: DescService, method: DescMethod): string {
  return `${service.name}_${method.name}`;
}

// printMethodInfo emits the module-level MethodInfo descriptor the client passes
// to the transport dispatch (service name, method name, kind, I/O schemas).
function printMethodInfo(f: GeneratedFile, service: DescService, method: DescMethod, rt: Runtime): void {
  const inT = f.importShape(method.input);
  const outT = f.importShape(method.output);
  const inSchema = f.importSchema(method.input);
  const outSchema = f.importSchema(method.output);
  f.print("const ", methodInfoName(service, method), ": ", rt.MethodInfo, "<", inT, ", ", outT, "> = {");
  f.print("  typeName: ", f.string(service.typeName), ",");
  f.print("  name: ", f.string(method.name), ",");
  f.print("  kind: ", f.string(method.methodKind), ",");
  f.print("  input: ", inSchema, ",");
  f.print("  output: ", outSchema, ",");
  f.print("};");
  f.print();
}

function printClient(f: GeneratedFile, service: DescService, rt: Runtime): void {
  for (const method of service.methods) {
    printMethodInfo(f, service, method, rt);
  }

  const className = service.name + "Client";
  f.print(f.jsDoc(service));
  f.print(f.export("class", className), " {");
  f.print("  constructor(private readonly transport: ", rt.WsTransport, ") {}");
  f.print();

  for (const method of service.methods) {
    switch (method.methodKind) {
      case "unary":
        printUnary(f, service, method, rt);
        break;
      case "server_streaming":
        printServerStreaming(f, service, method, rt);
        break;
      case "client_streaming":
        printClientStreaming(f, service, method, rt);
        break;
      case "bidi_streaming":
        printBidiStreaming(f, service, method, rt);
        break;
    }
    f.print();
  }
  f.print("}");
  f.print();
}

// CALL_OPTS is the per-call options object forwarded to the transport dispatch.
const CALL_OPTS = "{ headers: options?.headers, signal: options?.signal, timeoutMs: options?.timeoutMs }";

// unary: async getUser(req, options?): Promise<Res>
function printUnary(f: GeneratedFile, service: DescService, method: DescMethod, rt: Runtime): void {
  const inT = f.importShape(method.input);
  const outT = f.importShape(method.output);
  f.print(f.jsDoc(method, "  "));
  f.print("  async ", method.localName, "(req: ", inT, ", options?: CallOptions): Promise<", outT, "> {");
  f.print("    const res = await this.transport.unary(", methodInfoName(service, method), ", req, ", CALL_OPTS, ");");
  f.print("    return ", rt.applyUnaryCallbacks, "(res, options);");
  f.print("  }");
}

// server_streaming: watch(req, options?): AsyncIterable<Res>
function printServerStreaming(f: GeneratedFile, service: DescService, method: DescMethod, rt: Runtime): void {
  const inT = f.importShape(method.input);
  const outT = f.importShape(method.output);
  f.print(f.jsDoc(method, "  "));
  f.print("  async *", method.localName, "(req: ", inT, ", options?: CallOptions): AsyncIterable<", outT, "> {");
  f.print("    const res = await this.transport.stream(", methodInfoName(service, method), ", (async function* () { yield req; })(), ", CALL_OPTS, ");");
  f.print("    yield* ", rt.iterateStream, "(res, options);");
  f.print("  }");
}

// client_streaming: upload(reqs, options?): Promise<Res>
function printClientStreaming(f: GeneratedFile, service: DescService, method: DescMethod, rt: Runtime): void {
  const inT = f.importShape(method.input);
  const outT = f.importShape(method.output);
  f.print(f.jsDoc(method, "  "));
  f.print("  async ", method.localName, "(reqs: AsyncIterable<", inT, ">, options?: CallOptions): Promise<", outT, "> {");
  f.print("    const res = await this.transport.stream(", methodInfoName(service, method), ", reqs, ", CALL_OPTS, ");");
  f.print("    return ", rt.firstResponse, "(res, ", f.string(`${service.typeName}.${method.name}`), ", options);");
  f.print("  }");
}

// bidi_streaming: chat(reqs, options?): AsyncIterable<Res>
function printBidiStreaming(f: GeneratedFile, service: DescService, method: DescMethod, rt: Runtime): void {
  const inT = f.importShape(method.input);
  const outT = f.importShape(method.output);
  f.print(f.jsDoc(method, "  "));
  f.print("  async *", method.localName, "(reqs: AsyncIterable<", inT, ">, options?: CallOptions): AsyncIterable<", outT, "> {");
  f.print("    const res = await this.transport.stream(", methodInfoName(service, method), ", reqs, ", CALL_OPTS, ");");
  f.print("    yield* ", rt.iterateStream, "(res, options);");
  f.print("  }");
}
