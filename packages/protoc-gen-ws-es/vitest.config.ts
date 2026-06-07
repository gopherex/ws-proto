import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    testTimeout: 30000,
    hookTimeout: 60000,
    // Generate the test client SDK once, before any test file runs, so files
    // that import test/gen don't depend on alphabetical file order.
    globalSetup: ["./test/global-setup.ts"],
    // Keep files sequential: the integration test binds a fixed port.
    fileParallelism: false,
  },
});
