import { describe, it, expect } from "vitest";
import { execFileSync } from "node:child_process";
import { readFileSync, existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));
const pkgRoot = resolve(here, "..");
const genFile = resolve(here, "gen/echo_ws_pb.ts");

function run(cmd: string, args: string[]): void {
  execFileSync(cmd, args, { cwd: pkgRoot, stdio: "inherit" });
}

describe("protoc-gen-ws-es generation", () => {
  // The SDK is generated once by the vitest globalSetup; these tests assert it.

  it("emits the sibling _ws_pb.ts", () => {
    expect(existsSync(genFile)).toBe(true);
    expect(existsSync(resolve(here, "gen/echo_pb.ts"))).toBe(true);
  });

  it("emits a client class wired through MethodInfo descriptors", () => {
    const src = readFileSync(genFile, "utf8");
    expect(src).toContain("export class EchoServiceClient");
    // The wire route now lives in a MethodInfo descriptor (typeName + name)
    // passed to the transport's typed dispatch, not as an inline string.
    expect(src).toContain("EchoService_Unary");
    expect(src).toContain('typeName: "echo.v1.EchoService"');
    expect(src).toContain('name: "Unary"');
  });

  it("emits the correct signature for each method kind", () => {
    const src = readFileSync(genFile, "utf8");
    // unary -> Promise
    expect(src).toMatch(/async unary\(req: UnaryRequest[^)]*\): Promise<UnaryResponse>/);
    // server streaming -> async generator
    expect(src).toMatch(/async \*serverStream\(req: ServerStreamRequest[^)]*\): AsyncIterable<ServerStreamResponse>/);
    // client streaming -> Promise from AsyncIterable input
    expect(src).toMatch(/async clientStream\(reqs: AsyncIterable<ClientStreamRequest>[^)]*\): Promise<ClientStreamResponse>/);
    // bidi -> async generator from AsyncIterable input
    expect(src).toMatch(/async \*bidi\(reqs: AsyncIterable<BidiRequest>[^)]*\): AsyncIterable<BidiResponse>/);
  });

  it("imports schemas from the protoc-gen-es sibling", () => {
    const src = readFileSync(genFile, "utf8");
    expect(src).toMatch(/from "\.\/echo_pb\.js"/);
    expect(src).toContain("UnaryRequestSchema");
    expect(src).toContain("UnaryResponseSchema");
  });

  it("type-checks against the real runtime (tsc)", () => {
    // Throws (non-zero exit) if the generated code does not compile.
    run("npx", ["tsc", "-p", "test/tsc-check.tsconfig.json"]);
  });
});
