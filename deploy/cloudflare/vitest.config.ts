import { defineConfig } from "vitest/config";

// Plain-node vitest config for the pure logic in `src/logic.ts` (contract
// section 7: "the Worker logic ... is factored into pure functions with
// plain vitest tests"). This runs with no Cloudflare runtime at all —
// `logic.ts` only relies on WebCrypto and `Headers`, both standard and
// available under Node's own globals. The Worker-level test against the
// actual Workers runtime is separate (`test/worker.test.ts`, run with
// `npm run test:worker`; see the README for why it needs its own config).
export default defineConfig({
  test: {
    include: ["src/**/*.test.ts"],
  },
});
