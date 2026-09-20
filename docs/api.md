# HTTP API

The query API is a small FastAPI app (`src/osmpq/server.py`), run either
directly with `osmpq serve` (see [docs/cli.md](cli.md)) or behind the
Cloudflare Worker in `deploy/cloudflare/` (`docs/m3-contracts.md` section
7). Both expose the same paths; the Worker adds edge-side caching, rate
limiting and an attribution header in front of the same container image.

This document covers the wire protocol: endpoints, request/response
shapes, headers, status codes, environment variables, and the attic
(history) query settings. For which Overpass QL statements and filters
are actually implemented, see
[docs/overpass-ql-support.md](overpass-ql-support.md) — this document does
not repeat that matrix. For how to build and run a dataset the server can
point at, see [docs/cli.md](cli.md); for the code path a query takes
internally, see [docs/development.md](development.md).

All curl examples below assume a Minnesota dataset served locally
(`OSMPQ_ROOT=<root> osmpq serve --port 8080`) and use the downtown
Minneapolis bbox `44.97,-93.28,44.985,-93.255` (`south,west,north,east`),
the same box named `downtown_minneapolis` in `tests/corpus/bboxes.json`.

## Endpoints

### `GET/POST /api/interpreter`

The Overpass-compatible query endpoint (`src/osmpq/server.py`,
`_handle_interpreter`). Both methods are handled identically.

The query text is taken, in order:

1. a `data` query-string parameter (either method), else
2. for a request with a body: a `data` field in a form-encoded body
   (`application/x-www-form-urlencoded`), else
3. the raw request body decoded as UTF-8 — i.e. `curl --data-binary
   '<query>'` with no `data=` prefix also works, matching the reference
   server's "raw POST body also accepted when no data field" behavior.

An empty query (no `data`, no body) parses as an empty program and returns
an empty result rather than erroring.

**Settings clamping.** `[timeout:n]` and `[maxsize:n]` in the query are
never widened, only narrowed: each is set to `min(requested,
server_max)` when present, or to the server's max when absent
(`OSMPQ_MAX_TIMEOUT`, `OSMPQ_MAX_MAXSIZE` below). `[maxsize:n]` (bytes) is
translated to a DuckDB `SET memory_limit='<n//1MB>MB'` for that run's
cursor only.

**Response envelope.** On success (HTTP 200) the body is the
Overpass-shaped envelope for the requested `[out:...]` format:

- `[out:json]` (default) → `application/json`:
  ```json
  {
    "version": 0.6,
    "generator": "osmpq 0.0.1",
    "osm3s": {
      "timestamp_osm_base": "2026-09-19T00:21:52Z",
      "copyright": "The data included in this document is from www.openstreetmap.org. The data is made available under ODbL."
    },
    "elements": [ ... ]
  }
  ```
  `remark` is added as a top-level key only when present (see below).
- `[out:xml]` → `application/osm3s+xml`: `<osm version="0.6"
  generator="osmpq 0.0.1">`, a `<note>` with the same copyright text, a
  `<meta osm_base="...">`, one element per result, and a trailing
  `<remark>` when present.
- `[out:csv(...)]` → `text/csv; charset=utf-8`: the column list from the
  query, `::id`/`::lat`/`::lon`/etc. headed as `@id`/`@lat`/`@lon` (the
  `::` → `@` spelling is display-only), tab-separated by default
  (`docs/overpass-ql-support.md` T2 row; matches the reference including
  that header spelling).
- `[diff:]`/`[adiff:]` responses use the same envelope but `elements`
  holds `{"action": "create"|"modify"|"delete", "type", "id", "old"?,
  "new"?}` dicts instead of plain elements; JSON for diff/adiff is an
  osmpq **extension** — the reference only renders XML for these and
  rejects `[out:json]` with a static error (`docs/m4-report.md` section
  2, `docs/overpass-ql-support.md` "Status after M4").

**`remark`.** Set (and, for JSON, added as a top-level key; for XML, as a
trailing `<remark>` element) in three cases, all with HTTP 200:

- a runtime error (timeout, cancellation, or another `RuntimeQueryError`
  such as a bad set reference) — e.g. `runtime error: Query timed out in
  "osmpq" at line 1 after 25 seconds.`;
- a `[date:]`/`retro`/history-backed read whose instant is before the
  dataset's history coverage starts — `history starts at <since>; earlier
  dates return the earliest known state` (`docs/m4-contracts.md` section
  3.1);
- there is no `remark` key at all when a query simply succeeds with no
  warning.

**Errors.** A **parse error** (invalid Overpass QL) or an **unsupported**
but syntactically valid construct returns **HTTP 400** with an HTML body
shaped like the reference's error page, not the JSON/XML envelope:
```
<html><body><p><strong style="color:#FF0000">Error</strong>: line 1: parse error: <message></p></body></html>
```
overpass turbo and other clients parse this page to show the error inline.
A **rate limit** (see below) returns **HTTP 429** with a different,
Overpass-shaped HTML body containing the literal substring
`rate_limited` and a hint to check `/api/status`.

**Response headers** (on the 200 success path only):

| Header | Value |
| --- | --- |
| `X-OSMPQ-Manifest` | The dataset's current manifest number (`manifest/LATEST`'s contents), as a string. |
| `Cache-Control` | `public, max-age=60` |

CORS is wide open (`allow_origins=["*"]`, all methods, all headers, no
credentials).

**Attribution.** `osmpq serve` itself puts the ODbL copyright/note text
*inside* the response body (`osm3s.copyright` / `<note>`), not in a
header. The Cloudflare Worker additionally adds an edge-level header,
`X-Attribution: (c) OpenStreetMap contributors, ODbL`
(`deploy/cloudflare/src/logic.ts`), to every response it proxies,
including 404s and rate-limit responses.

**Request log.** One JSON line per request on stdout: `ts`, `ip`,
`status`, `seconds`, `files_read`, `elements`, `bytes`, `query_sha256`,
`timed_out`, `remark`, and `query` (the raw query text) when
`OSMPQ_LOG_QUERIES=1`.

### `GET /api/status`

Plain text, Overpass-shaped, scoped to the caller's own IP:

```
Connected as: <ip>
Current time: <UTC ISO8601>
Announced endpoint: <OSMPQ_ANNOUNCED_ENDPOINT>
Rate limit: <OSMPQ_SLOTS_PER_IP>
<n> slots available now.
Currently running queries (pid, space limit, time limit, start time):
<id> <maxsize> <timeout> <start ISO8601>
...
```
The running-queries block lists only queries from the same client IP,
one line per query, as the reference does.

### `GET /api/kill_my_queries`

Interrupts (via DuckDB `.interrupt()`) every query currently running from
the caller's IP and returns an HTML page listing the killed pids:
```
<html><body><p>The following queries have been killed:</p>
<p> pid: <id> </p>
...
</body></html>
```
A killed query's own `/api/interpreter` response gets `remark: runtime
error: Query aborted in "osmpq" at line 1 after <n> seconds (killed by
kill_my_queries).` with HTTP 200.

### `GET /healthz`

`200 {"manifest": <n>, "timestamp_osm_base": "..."}` once the dataset's
manifest has loaded; `503 {"error": "<message>"}` if `OSMPQ_ROOT` is
unset or the manifest can't be read. Also triggers the same
manifest-refresh check as a query (see below).

### `GET /api/timestamp`

Plain text: the dataset's `timestamp_osm_base`, or an empty body if the
manifest doesn't carry one.

### Manifest refresh

`Engine` re-reads `manifest/LATEST` at most once every
`OSMPQ_MANIFEST_REFRESH_SECONDS` seconds (checked lazily, on the next
request after the interval elapses — no background thread). When the
manifest number changed, the manifest (and the row-group index cache) are
reloaded and swapped in atomically; a query already in flight keeps the
manifest it started with even if a refresh happens mid-run.

## Rate limits and concurrency slots

Two independent caps, both enforced with an atomic check-and-increment
before a query is allowed to start (`src/osmpq/server.py`,
`_try_acquire`):

- **Per-IP**: at most `OSMPQ_SLOTS_PER_IP` concurrent `/api/interpreter`
  queries from the same client IP.
- **Global**: at most `OSMPQ_MAX_CONCURRENT` concurrent queries across the
  whole process, regardless of IP.

A query that arrives when either cap is already at capacity gets
**HTTP 429** immediately (before parsing counts against neither cap) with
this exact body (kept byte-for-byte close to what overpass turbo greps
for):

```
<html><body><p>Error: runtime error: open64: 0 Success /osm3s_v0.7.62_osm_base Dispatcher_Client::request_read_and_idx::rate_limited. Please check /api/status for the quota of your IP address.</p></body></html>
```

The Cloudflare Worker adds its own, separate rate limit in front of this
(a Workers rate-limiting binding, e.g. 30 requests/60s per client IP,
`docs/m3-contracts.md` section 7) using the same body text and status
code, so a request can be rejected at the edge before ever reaching a
container.

Client IP is determined by `_client_ip()`: with `OSMPQ_TRUST_PROXY=0`
(the default), it is always the raw peer address. With
`OSMPQ_TRUST_PROXY=1`, it is `CF-Connecting-IP` if present, else the
first entry of `X-Forwarded-For`, else the peer address — these headers
are otherwise ignored because they are trivially spoofable without an
actual trusted proxy in front.

## Environment variables

All read fresh on every request (not cached at import time), from
`src/osmpq/server.py`'s module docstring and `_env_*` accessors:

| Variable | Default | Meaning |
| --- | --- | --- |
| `OSMPQ_ROOT` | *(required)* | Dataset root: a local directory or an `s3://bucket/prefix` URL. `osmpq serve` fails to construct its `Engine` without this. |
| `OSMPQ_SLOTS_PER_IP` | `2` | Concurrent `/api/interpreter` queries allowed per client IP. |
| `OSMPQ_MAX_CONCURRENT` | `8` | Concurrent queries allowed per server process, across all IPs. |
| `OSMPQ_MAX_TIMEOUT` | `180` | Upper bound (seconds) `[timeout:n]` is clamped to. |
| `OSMPQ_MAX_MAXSIZE` | `1073741824` (1 GiB) | Upper bound (bytes) `[maxsize:n]` is clamped to. |
| `OSMPQ_TRUST_PROXY` | `0` | `1` to trust `CF-Connecting-IP`/`X-Forwarded-For` for the client IP; `0` always uses the raw peer address. |
| `OSMPQ_ANNOUNCED_ENDPOINT` | `"none"` | Echoed verbatim in `/api/status`'s `Announced endpoint:` line. |
| `OSMPQ_MANIFEST_REFRESH_SECONDS` | `60` | Minimum interval between `manifest/LATEST` re-reads. |
| `OSMPQ_LOG_QUERIES` | `0` (false) | `1` to include the raw query text in the stdout request log. |

Object-store credentials (used by `Engine` when `OSMPQ_ROOT` starts with
`s3://`; shared with `osmpq update`/`osmpq updater-server` and
`tools/upload_root.py`, all reading the same variables from
`src/osmpq/store.py`):

| Variable | Default | Meaning |
| --- | --- | --- |
| `OSMPQ_S3_KEY_ID` | *(none)* | Access key id. |
| `OSMPQ_S3_SECRET` | *(none)* | Secret access key. |
| `OSMPQ_S3_ENDPOINT` | *(none)* | Host only, no scheme, e.g. `<account>.r2.cloudflarestorage.com`. |
| `OSMPQ_S3_REGION` | `"auto"` | S3 region. |
| `OSMPQ_S3_URL_STYLE` | `"path"` | `"path"` or virtual-hosted addressing. |
| `OSMPQ_S3_USE_SSL` | `"true"` | `"0"`/`"false"`/`"no"` (case-insensitive) disables SSL. |

If `OSMPQ_S3_KEY_ID`/`OSMPQ_S3_SECRET`/`OSMPQ_S3_ENDPOINT` are not *all*
set, no DuckDB secret is created and DuckDB's normal credential chain
applies instead (e.g. ambient AWS environment variables or an instance
role).

The updater's control API (`osmpq updater-server`) is a separate FastAPI
app with its own environment variables (`OSMPQ_REPLICATION_SOURCE`,
`OSMPQ_UPDATE_MAX_DIFFS`, etc.) — see
[docs/cli.md](cli.md#osmpq-updater-server).

## Attic (history) settings

`[date:]`, `retro`, `timeline`, `[diff:]`/`[adiff:]` and an exact
`(changed:)`/`(newer:)` all need the dataset to carry a **history**
dataset — i.e. a manifest with a `history` section (manifest version 5+),
produced by `osmpq history init` or `osmpq history build` and kept
current by `osmpq update` (see [docs/cli.md](cli.md) and
`docs/m4-contracts.md`). Against a dataset with no history section, these
settings fall back to M3 behavior (`(changed:)`/`(newer:)` use the
current meta columns only) or are simply unsupported.

| Setting | Effect | Needs history? |
| --- | --- | --- |
| `[date:"t"]` | Whole program reads the state of every element as of instant `t` instead of "now". | Yes |
| `retro("t") { ... }` | Block-local snapshot at `t`; sets assigned inside stay visible outside the block (Overpass sets are global — note that in practice this makes a `.before - .after` idiom collapse to `.before` if `.after` is read outside the block; see `docs/m4-report.md` section 3's finding). | Yes |
| `timeline(type, id[, version])` | One result entry per known state (own version and minor/geometry-only versions) of one element; `expired` is absent on the still-current state. Only lists versions the history dataset actually knows — a regional extract started at time `T` knows nothing earlier. | Yes |
| `[diff:"a"[,"b"]]`, `[adiff:"a"[,"b"]]` | Runs the whole program twice (at `a` and at `b`, `b` defaults to now) and emits `create`/`modify`/`delete` actions between the two result sets. `adiff` includes minor-version-only changes; `diff` does not carry the same claim confirmed here — see `docs/m4-report.md` section 2 for the one documented difference found against the reference. | Yes |
| `(changed:"a"[,"b"])`, `(newer:"t")` | Exact membership from history rows (`a < valid_from <= b`) when history is present; without it, falls back to comparing the current meta `timestamp` column (M3 behavior, less precise on repeated edits). | Improves with history, works without it |

A `[date:]` (or other attic-time) before the history's coverage start
answers from the earliest known state and adds the `remark` described
above rather than erroring. See `docs/m4-contracts.md` sections 3 and 7,
and `docs/overpass-ql-support.md`'s "Status after M4" section for the
documented divergences from the reference (areas not versioned,
`timeline` not splitting minor versions, `compare` unimplemented, etc.).

## curl examples

All against `osmpq serve --port 8080` with `OSMPQ_ROOT` pointing at a
Minnesota dataset, using the downtown Minneapolis bbox.

**1. Cafes in the bbox, JSON, via POST form field:**
```sh
curl 'http://127.0.0.1:8080/api/interpreter' \
  --data-urlencode 'data=[out:json][timeout:25];
node["amenity"="cafe"](44.97,-93.28,44.985,-93.255);
out body;'
```

**2. Same query, GET with the query string:**
```sh
curl -G 'http://127.0.0.1:8080/api/interpreter' \
  --data-urlencode 'data=[out:json];node["amenity"="cafe"](44.97,-93.28,44.985,-93.255);out;'
```

**3. Raw POST body, no `data=` prefix, XML output:**
```sh
curl 'http://127.0.0.1:8080/api/interpreter' \
  --data-binary '[out:xml][timeout:25];way["highway"](44.97,-93.28,44.985,-93.255);out geom;'
```

**4. CSV output (name, id, lat, lon of cafes):**
```sh
curl 'http://127.0.0.1:8080/api/interpreter' \
  --data-urlencode 'data=[out:csv(name,::id,::lat,::lon)];node["amenity"="cafe"](44.97,-93.28,44.985,-93.255);out;'
```

**5. Attic read: node tags as of a past instant (needs a history dataset):**
```sh
curl 'http://127.0.0.1:8080/api/interpreter' \
  --data-urlencode 'data=[out:json][date:"2026-09-19T06:00:00Z"];
node["amenity"](44.97,-93.28,44.985,-93.255);
out tags;'
```

**6. Status, and killing your own running queries:**
```sh
curl 'http://127.0.0.1:8080/api/status'
curl 'http://127.0.0.1:8080/api/kill_my_queries'
```

## See also

- [docs/overpass-ql-support.md](overpass-ql-support.md) — which Overpass
  QL statements, filters and evaluators are implemented, by tier, and the
  documented divergences from the reference server.
- [docs/cli.md](cli.md) — building, updating and serving a dataset from
  the command line, including `osmpq serve` and `osmpq updater-server`.
- [docs/development.md](development.md) — the code path a query takes
  through the engine, and how to add a new filter or statement.
- `docs/m3-contracts.md` section 6 — the original service contract this
  implements. `docs/m3-contracts.md` section 7 and `docs/m3-runbook.md`
  — the Cloudflare Worker/Container deployment in front of it.
- `docs/m4-contracts.md` — the attic (history) design in full.
