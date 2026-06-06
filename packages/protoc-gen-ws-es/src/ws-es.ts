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

    // Runtime import: WsTransport from @gopherex/ws-transport.
    const WsTransport = f.import("WsTransport", "@gopherex/ws-transport");

    for (const service of file.services) {
      printDescriptor(f, service);
      printClient(f, service, WsTransport);
    }
  }
}

type ImportSymbol = ReturnType<GeneratedFile["import"]>;

// Fully-qualified wire path "/pkg.Service/Method".
function wirePath(service: DescService, method: DescMethod): string {
  return `/${service.typeName}/${method.name}`;
}

// Map protobuf-es methodKind to a stable string we emit into the descriptor.
function kindLiteral(method: DescMethod): string {
  switch (method.methodKind) {
    case "unary":
      return "unary";
    case "server_streaming":
      return "server_streaming";
    case "client_streaming":
      return "client_streaming";
    case "bidi_streaming":
      return "bidi_streaming";
  }
}

// Emit `export const FooServiceDescriptor = { ... } as const;`
// A data-driven map: localName -> { kind, path, input schema, output schema }.
function printDescriptor(f: GeneratedFile, service: DescService): void {
  const descName = service.name + "Descriptor";
  f.print(f.jsDoc(service));
  f.print(f.export("const", descName), " = {");
  f.print("  typeName: ", f.string(service.typeName), ",");
  f.print("  methods: {");
  for (const method of service.methods) {
    const inputSchema = f.importSchema(method.input);
    const outputSchema = f.importSchema(method.output);
    f.print("    ", method.localName, ": {");
    f.print("      kind: ", f.string(kindLiteral(method)), ",");
    f.print("      path: ", f.string(wirePath(service, method)), ",");
    f.print("      input: ", inputSchema, ",");
    f.print("      output: ", outputSchema, ",");
    f.print("    },");
  }
  f.print("  },");
  f.print("} as const;");
  f.print();
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

// unary: async getUser(req, headers?): Promise<Res>
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
  f.print("  async ", method.localName, "(req: ", inT, ", headers?: Record<string, string>): Promise<", outT, "> {");
  f.print("    const stream = this.transport.openStream(", f.string(wirePath(service, method)), ", headers);");
  f.print("    stream.send(", toBinary, "(", inSchema, ", req));");
  f.print("    stream.closeSend();");
  f.print("    const bytes = await stream.recv();");
  f.print("    if (bytes === null) {");
  f.print("      throw new Error(", f.string(`${service.typeName}.${method.name}: server closed stream without a response`), ");");
  f.print("    }");
  f.print("    return ", fromBinary, "(", outSchema, ", bytes);");
  f.print("  }");
}

// server_streaming: watch(req, headers?): AsyncIterable<Res>
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
  f.print("  async *", method.localName, "(req: ", inT, ", headers?: Record<string, string>): AsyncIterable<", outT, "> {");
  f.print("    const stream = this.transport.openStream(", f.string(wirePath(service, method)), ", headers);");
  f.print("    stream.send(", toBinary, "(", inSchema, ", req));");
  f.print("    stream.closeSend();");
  f.print("    for await (const bytes of stream) {");
  f.print("      yield ", fromBinary, "(", outSchema, ", bytes);");
  f.print("    }");
  f.print("  }");
}

// client_streaming: upload(reqs, headers?): Promise<Res>
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
  f.print("  async ", method.localName, "(reqs: AsyncIterable<", inT, ">, headers?: Record<string, string>): Promise<", outT, "> {");
  f.print("    const stream = this.transport.openStream(", f.string(wirePath(service, method)), ", headers);");
  f.print("    for await (const req of reqs) {");
  f.print("      stream.send(", toBinary, "(", inSchema, ", req));");
  f.print("    }");
  f.print("    stream.closeSend();");
  f.print("    const bytes = await stream.recv();");
  f.print("    if (bytes === null) {");
  f.print("      throw new Error(", f.string(`${service.typeName}.${method.name}: server closed stream without a response`), ");");
  f.print("    }");
  f.print("    return ", fromBinary, "(", outSchema, ", bytes);");
  f.print("  }");
}

// bidi_streaming: chat(reqs, headers?): AsyncIterable<Res>
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
  f.print("  async *", method.localName, "(reqs: AsyncIterable<", inT, ">, headers?: Record<string, string>): AsyncIterable<", outT, "> {");
  f.print("    const stream = this.transport.openStream(", f.string(wirePath(service, method)), ", headers);");
  // Pump request messages concurrently so the read loop below can interleave.
  f.print("    const pump = (async () => {");
  f.print("      for await (const req of reqs) {");
  f.print("        stream.send(", toBinary, "(", inSchema, ", req));");
  f.print("      }");
  f.print("      stream.closeSend();");
  f.print("    })();");
  f.print("    try {");
  f.print("      for await (const bytes of stream) {");
  f.print("        yield ", fromBinary, "(", outSchema, ", bytes);");
  f.print("      }");
  f.print("    } finally {");
  f.print("      await pump;");
  f.print("    }");
  f.print("  }");
}
