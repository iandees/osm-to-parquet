/**
 * `UpdaterContainer` (the osmpq image running `osmpq updater-server` on
 * port 8081) and `UpdaterScheduler`, the single Durable Object that owns
 * the "cron with a lock" for it (`docs/design.md` section 4.2, contract
 * section 7).
 */
import { Container, getContainer } from "@cloudflare/containers";
import { DurableObject } from "cloudflare:workers";
import { canStartRun, nextAlarmAfterRun } from "./logic";

export interface UpdaterEnv {
  OSMPQ_ROOT: string;
  OSMPQ_REPLICATION_SOURCE: string;
  OSMPQ_S3_KEY_ID?: string;
  OSMPQ_S3_SECRET?: string;
  OSMPQ_S3_ENDPOINT?: string;
  OSMPQ_S3_REGION?: string;
  UPDATER: DurableObjectNamespace<UpdaterContainer>;
}

export class UpdaterContainer extends Container<UpdaterEnv> {
  defaultPort = 8081;
  // Long: the updater container's local disk is a warm read-through
  // cache for the id-sorted copy (`docs/design.md` section 4.1), which
  // only pays off if the instance survives between runs. Ten minutes
  // comfortably spans the one-run-per-minute schedule.
  sleepAfter = "10m";
  // See the matching note in `engine.ts`: the base class's default
  // startup readiness probe (`/ping`) doesn't exist on this app.
  pingEndpoint = "healthz";

  // See the matching note in `engine.ts`: typed explicitly as a plain
  // string map so it structurally matches the base class's `envVars`.
  envVars: Record<string, string> = {
    OSMPQ_ROOT: this.env.OSMPQ_ROOT,
    OSMPQ_REPLICATION_SOURCE: this.env.OSMPQ_REPLICATION_SOURCE,
    ...(this.env.OSMPQ_S3_KEY_ID ? { OSMPQ_S3_KEY_ID: this.env.OSMPQ_S3_KEY_ID } : {}),
    ...(this.env.OSMPQ_S3_SECRET ? { OSMPQ_S3_SECRET: this.env.OSMPQ_S3_SECRET } : {}),
    ...(this.env.OSMPQ_S3_ENDPOINT ? { OSMPQ_S3_ENDPOINT: this.env.OSMPQ_S3_ENDPOINT } : {}),
    ...(this.env.OSMPQ_S3_REGION ? { OSMPQ_S3_REGION: this.env.OSMPQ_S3_REGION } : {}),
  };
}

/** The name the scheduler always uses for the single updater container
 * instance (contract section 7: "the single updater container instance"
 * — the updater is stateless per `docs/design.md` section 4.1 but must
 * not run concurrently with itself, so there is exactly one). */
const UPDATER_INSTANCE_NAME = "updater";

const RUN_INTERVAL_MS = 60_000;

export interface RunSummary {
  // Shape mirrors `update/server.py`'s `RunSummary` JSON (contract
  // section 6.3); kept as `unknown`-ish here since the Worker only
  // stores and returns it, never interprets its fields.
  [key: string]: unknown;
}

interface SchedulerState {
  running: boolean;
  lastStartedAtMs: number | null;
  lastFinishedAtMs: number | null;
  lastSummary: RunSummary | null;
  lastError: string | null;
  nextAlarmAtMs: number | null;
}

const STATE_KEY = "state";

const EMPTY_STATE: SchedulerState = {
  running: false,
  lastStartedAtMs: null,
  lastFinishedAtMs: null,
  lastSummary: null,
  lastError: null,
  nextAlarmAtMs: null,
};

/** Durable Object: the updater's schedule and its lock. `alarm()` is the
 * only thing that ever starts a run on its own; `fetch()` answers
 * `start`/`stop`/`run`/`status` for the Worker's `/admin/scheduler/*`
 * routes (contract section 7). Being a Durable Object, all of this is
 * single-threaded, so `running` can never race with itself here — the
 * container's own 409-while-busy response is the second line of defense
 * documented in `docs/design.md` section 4.2. */
export class UpdaterScheduler extends DurableObject<UpdaterEnv> {
  private async getState(): Promise<SchedulerState> {
    const stored = await this.ctx.storage.get<SchedulerState>(STATE_KEY);
    return stored ?? EMPTY_STATE;
  }

  private async setState(state: SchedulerState): Promise<void> {
    await this.ctx.storage.put(STATE_KEY, state);
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const action = url.pathname.split("/").pop();
    switch (action) {
      case "start":
        return this.handleStart();
      case "stop":
        return this.handleStop();
      case "run":
        return this.handleRunNow();
      case "status":
        return this.handleStatus();
      default:
        return new Response("not found", { status: 404 });
    }
  }

  private async handleStart(): Promise<Response> {
    const existing = await this.ctx.storage.getAlarm();
    if (existing === null) {
      // Arm immediately; the first run then re-arms itself on its own
      // 60s cadence.
      await this.ctx.storage.setAlarm(Date.now());
    }
    return Response.json({ ok: true, armed: true });
  }

  private async handleStop(): Promise<Response> {
    await this.ctx.storage.deleteAlarm();
    return Response.json({ ok: true, armed: false });
  }

  private async handleRunNow(): Promise<Response> {
    const result = await this.runOnce();
    return Response.json(result);
  }

  private async handleStatus(): Promise<Response> {
    const state = await this.getState();
    const alarm = await this.ctx.storage.getAlarm();
    return Response.json({
      running: state.running,
      lastStartedAt: state.lastStartedAtMs ? new Date(state.lastStartedAtMs).toISOString() : null,
      lastFinishedAt: state.lastFinishedAtMs ? new Date(state.lastFinishedAtMs).toISOString() : null,
      lastSummary: state.lastSummary,
      lastError: state.lastError,
      nextAlarmAt: alarm ? new Date(alarm).toISOString() : null,
      scheduled: alarm !== null,
    });
  }

  /** Cloudflare calls this when the armed alarm fires. Never called
   * concurrently with itself for the same DO instance (Durable Objects
   * process one request/alarm at a time), which is what makes "never
   * overlapping runs" hold even though the container it drives could in
   * principle be reached another way. */
  async alarm(): Promise<void> {
    await this.runOnce();
  }

  /** Start one updater run if none is in flight, record its outcome, and
   * re-arm the alarm 60s after this run *started* (contract section 7 /
   * `docs/design.md` section 4.2: "re-arms the alarm 60 s after the run
   * started ... a slow run just delays the next"). Returns a small
   * summary of what happened, used both by the alarm path and by the
   * `run` admin action for an on-demand trigger. */
  private async runOnce(): Promise<{ started: boolean; summary?: RunSummary; error?: string }> {
    const state = await this.getState();
    if (!canStartRun(state)) {
      return { started: false };
    }

    const startedAtMs = Date.now();
    await this.setState({ ...state, running: true, lastStartedAtMs: startedAtMs });
    // Re-arm right away, from the run's start time, so a slow run delays
    // the next one instead of the next one being skipped entirely if the
    // DO were to restart mid-run.
    const { fireAtMs } = nextAlarmAfterRun(startedAtMs, startedAtMs, RUN_INTERVAL_MS);
    await this.ctx.storage.setAlarm(fireAtMs);

    let summary: RunSummary | undefined;
    let errorMessage: string | undefined;
    try {
      const stub = getContainer(this.env.UPDATER, UPDATER_INSTANCE_NAME);
      const response = await stub.containerFetch(
        new Request("http://updater/run", { method: "POST" }),
        8081,
      );
      if (response.status === 409) {
        // The container itself thinks a run is in flight (e.g. after a
        // DO restart lost our own `running` flag); treat as "did not
        // start", not an error.
        errorMessage = "updater container reported a run already in progress (409)";
      } else if (!response.ok) {
        errorMessage = `updater container returned ${response.status}: ${await response.text()}`;
      } else {
        summary = (await response.json()) as RunSummary;
      }
    } catch (err) {
      errorMessage = err instanceof Error ? err.message : String(err);
    }

    const finishedAtMs = Date.now();
    await this.setState({
      running: false,
      lastStartedAtMs: startedAtMs,
      lastFinishedAtMs: finishedAtMs,
      lastSummary: summary ?? state.lastSummary,
      lastError: errorMessage ?? null,
      nextAlarmAtMs: fireAtMs,
    });

    return errorMessage ? { started: true, error: errorMessage } : { started: true, summary };
  }
}
