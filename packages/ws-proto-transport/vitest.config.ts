import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // Every test gets a hard timeout: the defect class under test is "hangs
    // forever", so a runaway test must fail fast rather than stall the runner.
    testTimeout: 2000,
    hookTimeout: 2000,
    // Unhandled rejections (default vitest behavior) fail the run; keep the
    // explicit default here so nobody flips it silently.
    dangerouslyIgnoreUnhandledErrors: false,
  },
});
