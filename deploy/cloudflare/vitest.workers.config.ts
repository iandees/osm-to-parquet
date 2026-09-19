import { defineConfig } from "vitest/config";
import { cloudflareTest } from "@cloudflare/vitest-pool-workers";

// Worker-level test running inside the actual Workers runtime
// (Miniflare), against `wrangler.test.jsonc` — a narrower config than
// the real `wrangler.jsonc` (no `containers` block; see that file's
// comment for why). Run with `npm run test:worker`.
export default defineConfig({
  plugins: [
    cloudflareTest({
      wrangler: { configPath: "./wrangler.test.jsonc" },
    }),
  ],
  test: {
    include: ["test/**/*.test.ts"],
  },
});
