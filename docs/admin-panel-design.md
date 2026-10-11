# Admin panel + fleet health for the Node collector — design

Date: 2026-10-09
Status: approved (hosting = provider, heartbeat = yes, counters = exact but single-writer)

## What is being asked for

The Python web app used to be the way to look at the data and to grow the
catalogue (seed states, seed ACs, discover parts). Only the Node collector runs
now, and its dashboard is deliberately a *device monitor*: it shows this
process's numbers plus a few fleet totals. What is missing is the operator's
view — the real rows, the buttons that add work, and the numbers that say
whether adding another device is worth it.

## What the live store looks like (measured 2026-10-09)

| Fact | Value |
|---|---|
| Fleet DB | PG 14.24, db `old_eci`, 8,095 MB, `shared_buffers` 128 MB, `max_connections` 100 |
| `electors` | 8,052 MB, 22.4M rows (`reltuples`) |
| `old_parts` | 22 MB, 36,899 rows — 29,001 done, 7,894 pending, 4 error, 4 running |
| `acs` | 4,252 rows — 193 discovered, 4,059 never discovered |
| `states` | 38 |
| Live devices | `render-1..4`, ~1,100 parts/hour fleet-wide, ~275/hour each |
| Remaining discovered work | 7,876 parts ≈ 7 hours at the current rate |

Data sources that the seed actions need, all reachable from a plain Node process
(no new dependency):

* legacy catalogue DB `eci` on the same host (`ECI_GEO_DSN`) — 38 states, 4,129
  ACs, 300–620 ms/query, read-only;
* `gateway-vha…/common/states` — 200, 36 live states;
* `gateway-voters…/citizen/sir/getAsmbly` — 200, 187 ACs for S01 with `acType`,
  district and both name scripts (the legacy catalogue says 175: the same 12-AC
  gap the Python client documents).

## The problem that has to be fixed first

`claim.js → heavyCounts()` refreshes `count(*) from electors` and
`count(distinct cur_epic)` on **every** instance, per process, every
`heavyTtlMs` = 120 s. On the live store those two scans do not finish: four
instances were each running `count(distinct cur_epic)` for 7+ minutes
concurrently, taking the whole box's IO. `config.js` assumed a 120 s
`statement_timeout` would bound them — the server has `statement_timeout = 0`,
and no connection in this project sets one, so they are unbounded.

Effect: a trivial aggregate measured 0.3–2.6 s instead of ~0.1 s, and every page
of the new panel would inherit that.

## Design

### 1. Exact counters, one writer (`stats.js`)

Numbers stay **exact** (the operator must not see two different values for the
same thing). Exactly one process runs the scan, at most once per lease:

* the value lives in `settings` under `stats@<schema>` —
  `{at, electors, unique_epics, records, updated_by}`;
* an atomic `insert … on conflict … do update … where <lease expired>` claims
  the refresh; the process that gets a row back runs the scans, everyone else
  reads the cached value;
* the scan runs in a transaction with `set local statement_timeout`, so it can
  never become an unbounded 8 GB scan again;
* all instances read the same row, so all four dashboards show the same number,
  with its age. No instance ever falls back to an estimate, because an estimate
  next to an exact value *is* the inconsistency being avoided.

Cost: one scan per ~10 min instead of four scans per 2 min, single-stream.

### 2. Catalog browsing (`server.js` + `admin.html`)

Every list query is one pass, no join fan-out and no per-row subquery. Measured
shapes:

| Page | Shape | Cost |
|---|---|---|
| states + counts | aggregate subqueries joined on `state_cd` | **307 ms** (fan-out join: 137 s) |
| one state's ACs | `group by ac_no` over `old_parts` + `acs` metadata | 2.7 s |
| one AC's parts | plain select, `records`/`epics` from the row | 0.5 s |
| one part's electors | `electors_old_idx` | 0.6 s |
| elector by source_id / EPIC | `electors_pkey` / `electors_epic_idx` | 0.19 s |
| elector by name | prefix only (`lower(full_name) like 'x%'`), scoped | uses `electors_name_idx` |

The parts page shows `old_parts.records` (already tracked) rather than counting
`electors` per part: the Python page's correlated count costs 2.1 s per AC and
grows with the table. `full_name ilike '%x%'` is never issued — the leading
wildcard cannot use the index, and a miss scans all 8 GB.

### 3. Actions (`catalog.js`)

* `seedStates()` — legacy catalogue first, live `common/states` merged on top.
* `seedAcs({state}|all)` — legacy catalogue merged with the live AC list,
  `on conflict do update … coalesce(excluded.…)`, exactly the Python semantics,
  in one batched write per state.
* `discoverParts({state, ac})` — port of `worker.py`: probe part numbers in
  chunks of 200 upward while the top of a chunk answers, hard cap 3000,
  `discover_max_part` as a floor; bulk upsert `old_parts`; then update
  `acs.discover_status/old_parts_found/discover_max/discovered_at` and
  `states.has_old_data`. Probing uses `api.js`'s existing
  `getEroll(…, '')` window — no new gateway knowledge.
* `discoverNext({state, batch})` — discovery as a **queue over
  `acs.discover_status`**: a single atomic `update … where (state_cd, ac_no) in
  (select … where status='pending' … limit 1) returning` claims the next AC. Any
  number of instances can run it concurrently without a lock, a crash leaves the
  claim to be reaped after `discover_stale_min`, and it resumes where it stopped.

Jobs run in the panel process; progress goes to `events` (readable by every
existing tool) and `acs.discover_started_at` carries the state.

### 4. Fleet / streaming health (the "how many devices" answer)

Each worker writes one small `settings` row per device — `worker_heartbeat@<tag>`,
the key shape the Python and Android clients already use — every 15 s:
AIMD limit/inflight/recent req-s, `errByStatus` (so **429s become visible**),
current part, parts done, plus its egress IP refreshed hourly
(`ECI_REPORT_IP`, opt-out).

That makes the question answerable in the panel:

* per-device rate and last-seen (is a device alive?);
* the first 429/5xx across the fleet — the signal that says stop adding;
* **egress-IP grouping**: Render shares its regional outbound ranges across
  services (`render.com/docs/outbound-ip-addresses`), so a fifth Render service
  adds concurrency against the same per-IP ceiling. Genuinely new capacity comes
  from a new IP (the Oracle box, the desktop, the phone). The panel shows the
  grouping instead of asserting it.

### 5. Database health

Extends `dbStatus()` with server version/uptime, database and per-table sizes,
connection count by application, and the longest-running statement — all from
`pg_stat_activity` and the `pg_class` catalogue, no table scans.

### 6. Where it runs

**Provider (Render), on the instances that already exist.** The panel is a thin
DB client; its cost is server-side CPU/IO, not the ~100 ms of network, so
co-locating it with Postgres on the 1 GB box buys little and risks the one
process that must not die (an OOM there takes Postgres with it, stopping the
whole fleet). The Oracle box is better used as a *collector* — a fresh egress
IP, which is the actual capacity lever.

## Files

New: `stats.js`, `catalog.js`, `admin.html`, this doc.
Changed: `config.js`, `db.js`, `api.js`, `claim.js`, `worker.js`, `server.js`,
`index.js`, `.env.example`, `README.md`.

No new npm dependency; no new service; `/` stays the device monitor and `/admin`
is the fleet/catalog panel.

## Verification

* syntax check every changed file; `ensureSchema()` still a no-op on the live
  schema (it must not run DDL there);
* read endpoints exercised against the **live** store through HTTP on a spare
  port with a throwaway device tag, checking real state/AC/part/elector rows;
* write paths (seed + discover) exercised end-to-end against a **throwaway
  schema** in the same database, then dropped — never against `public`;
* the stats lease tested by racing two processes to confirm exactly one refresh;
* timing recorded before/after for the catalog queries.
