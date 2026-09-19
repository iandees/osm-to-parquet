/**
 * `EngineContainer`: one Durable-Object-managed instance of the osmpq
 * container image running `osmpq serve` on port 8080 (contract section 6,
 * `docs/m3-contracts.md` section 6.1 / 6.4). The Worker load-balances
 * across `ENGINE_INSTANCES` of these with `getRandom` (see `index.ts`);
 * each one sleeps two minutes after its last request, per
 * `docs/design.md` section 6.1's "short sleepAfter while traffic is low"
 * policy.
 */
import { Container } from "@cloudflare/containers";

export interface EngineEnv {
  OSMPQ_ROOT: string;
  OSMPQ_ANNOUNCED_ENDPOINT: string;
  OSMPQ_S3_KEY_ID?: string;
  OSMPQ_S3_SECRET?: string;
  OSMPQ_S3_ENDPOINT?: string;
  OSMPQ_S3_REGION?: string;
}

export class EngineContainer extends Container<EngineEnv> {
  defaultPort = 8080;
  // Short by default (see the runbook for lengthening this once there is
  // steady traffic worth keeping an instance warm for).
  sleepAfter = "2m";

  // Typed as a plain string map (matching `Container['envVars']`) rather
  // than left to inference, since the object literal below has optional
  // keys that TypeScript would otherwise not treat as assignable to the
  // base class's `Record<string, string>` field.
  envVars: Record<string, string> = {
    OSMPQ_ROOT: this.env.OSMPQ_ROOT,
    OSMPQ_ANNOUNCED_ENDPOINT: this.env.OSMPQ_ANNOUNCED_ENDPOINT,
    // The container sits behind the Worker, which is the only path in;
    // CF-Connecting-IP is trustworthy there (contract section 6.1).
    OSMPQ_TRUST_PROXY: "1",
    ...(this.env.OSMPQ_S3_KEY_ID ? { OSMPQ_S3_KEY_ID: this.env.OSMPQ_S3_KEY_ID } : {}),
    ...(this.env.OSMPQ_S3_SECRET ? { OSMPQ_S3_SECRET: this.env.OSMPQ_S3_SECRET } : {}),
    ...(this.env.OSMPQ_S3_ENDPOINT ? { OSMPQ_S3_ENDPOINT: this.env.OSMPQ_S3_ENDPOINT } : {}),
    ...(this.env.OSMPQ_S3_REGION ? { OSMPQ_S3_REGION: this.env.OSMPQ_S3_REGION } : {}),
  };
}
