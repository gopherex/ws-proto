import { execFileSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const pkgRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

// Generates the test client SDK exactly once, before any test file runs, so
// files that import test/gen never depend on vitest's file execution order
// (which is alphabetical, not the order generation happens to live in).
export default function setup(): void {
  execFileSync("npm", ["run", "build"], { cwd: pkgRoot, stdio: "inherit" });
  execFileSync("npx", ["buf", "generate", "--template", "test/buf.gen.test.yaml"], {
    cwd: pkgRoot,
    stdio: "inherit",
  });
}
