import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    testTimeout: 30000,
    hookTimeout: 60000,
    // Run test files sequentially: both the generation unit test and the
    // integration test regenerate into test/gen, so they must not race.
    fileParallelism: false,
  },
});
