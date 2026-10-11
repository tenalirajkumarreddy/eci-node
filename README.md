# eci-node-collector

Clean Node.js (>= 18, ESM, one dependency: `pg`) port of the old-roll (2003 / SIR)
collector. PostgreSQL store + atomic part claiming + adaptive-throughput sweep +
single-page dashboard. Runs standalone or alongside the existing Python/Android
fleet — they share the same `old_parts` ledger, so devices never sweep the same part.

## Quickstart

```bash
cp .env.example .env          # fill in ECI_PG_DSN
npm install
npm start                     # worker + dashboard on http://127.0.0.1:8008/
```

CLI (single binary, mirrors the Python collector's flags):

```
node src/index.js [--parts N] [--minutes N] [--idle-exit N]
                  [--state S01] [--ac 26] [--force]
                  [--workers N]            # AIMD starting inflight limit
                  [--no-init] [--no-recover] [--headless]
                  [--serve] [--port N] [--host H]
node src/index.js --status
node src/index.js --epic <EPIC>
node src/index.js --export out.csv [--state S01] [--ac 26]
```

## No hardcoded rate caps

There is no `req/s` constant and no `workers x parts_parallel <= 32` rule.
`src/aimd.js` runs additive-increase / multiplicative-decrease congestion
control: it starts at `ECI_START_INFLIGHT`, grows while the gateway answers
cleanly, and halves on any 429 / 5xx / timeout — honoring `Retry-After` when
present. The practical ceiling is discovered at runtime, per source IP, by the
gateway itself. `ECI_MAX_INFLIGHT=0` means "unbounded"; set it only to protect
the client (fds/memory), never as a server-side target.

Multi-device note: the fleet cap was empirically per-source (~80 req/s). Phones
behind one NAT may share a budget. AIMD per device handles this automatically —
each device converges to its share.

## Catalog & fleet admin panel (`/admin`)

The device dashboard at `/` is a *monitor for this process*. `/admin` is the
operator's view of the whole thing: real rows, the buttons that add work, and the
numbers that say whether adding another device is worth it. Same process, same
port, no extra service and no new dependency.

**Catalog** — states → ACs → parts → electors, drilled down by clicking. Every
list is one pass with no per-row subqueries, which is what keeps it usable on a
live store: the states page measures ~0.3s (the obvious three-way join fans out
to 6.8M rows and took 137s), and the parts page shows `old_parts.records` instead
of counting `electors` per part. Elector search takes either an **EPIC / source
id** (exact index lookup, ~0.2s) or a **name prefix** — never `%name%`, which
cannot use `electors_name_idx` and scans the whole table when nothing matches.

**Actions** — the catalogue work the Python web app used to do:

| button | what it writes |
|---|---|
| Seed states | legacy catalogue DB + the live state list (upsert, nothing deleted) |
| Seed ACs | legacy catalogue merged with the live list, one batched write |
| Discover next | claims the next undiscovered AC and probes its part numbers |
| Requeue errored | puts failed ACs back in the queue after you fix the cause |

Discovery is a **queue over `acs.discover_status`**: one atomic `UPDATE` claims the
next AC, so several instances can discover in parallel with no lock, a crash is
reaped after `ECI_DISCOVER_STALE_MIN`, and a restart resumes where it stopped.
Part numbers are probed in chunks while the top of each chunk answers, so
`ECI_DISCOVER_HARD_CAP` is a ceiling and the reported `truncated` flag means the
cap was actually reached.

Same actions from the CLI, for cron or a machine with no browser:

```
node index.js --status
node index.js --seed-states
node index.js --seed-acs [S01]
node index.js --discover-next [S01] [--batch 10]
node index.js --discover S01/1          # one AC
node index.js --acs-backlog
node index.js --no-collect --port 8008  # serve the panel without sweeping
```

**Fleet health** answers "how many more devices can I add?" from measurements:
per-device req/s and AIMD limit, the **429/5xx mix** (invisible in the database
before this), parts/hour, the queue and an ETA, and the **egress IP** each device
reports — because a provider hands every service in a region the same outbound
ranges, so several instances can be competing for one per-IP ceiling. The page
says so when it sees it instead of assuming.

Each worker publishes its own numbers to `worker_heartbeat@<tag>` in `settings`
(the same key the Python and Android clients use), so a device running an older
build still appears in the table — it just has fewer fields filled in.

## The expensive counters are shared, not repeated

`count(*) from electors` and `count(distinct cur_epic)` are full scans (8 GB /
22M rows on the live store). Refreshing them per instance every 120s is a
stampede: four of those scans ran concurrently for 7+ minutes each and never
finished, because nothing sets `statement_timeout` and the server's value is `0`.
They are now computed by **one process at a time** — claimed with a single atomic
statement against a shared `settings` row — and every dashboard reads that row,
so all instances show the same exact number with its age (never an estimate
beside an exact value). The scan itself runs with `set statement_timeout`.

## Layout

Paths are relative to this directory.

| file | role |
|---|---|
| `config.js` | env config; device tag; no caps |
| `db.js` | `pg` pool, `q()`, `logEvent()`, `geoQ()`, `dbHealth()` |
| `schema.js` | DDL + skip-if-complete (no ACCESS EXCLUSIVE on live DBs) |
| `aimd.js` | congestion controller (the "no cap" piece) |
| `api.js` | gateway client: sweeps, EPIC search, live state/AC lists |
| `claim.js` | atomic claim / reserve / take / complete / stale reaper |
| `collector.js` | part sweep, serial iteration, batched upserts |
| `worker.js` | auto loop, reservations, crash recovery, heartbeat |
| `catalog.js` | seed states/ACs + the discover-parts queue (`/admin` actions) |
| `stats.js` | the two exact counters over `electors`: one scanner, shared result |
| `server.js` | zero-dep HTTP dashboard + JSON API + `/admin` |
| `admin.html` | the catalog / actions / fleet / database panel |
| `epic.js` | EPIC processor (national search, passKey = SHA512(APP_CONST+key)) |
| `index.js` | CLI entry |

## Data model

`old_parts` is the ledger (`pending | running | done | error`) with
`claimed_by/last_seen/reserved_by/reserved_at`; `electors` is one row per old-roll
serial, upserted on `source_id = state:ac:part:serial`, `raw jsonb` kept whole.
First connecting device creates the schema; later ones detect it and skip DDL.

## Field mapping caveat

`mapRecord()` in `src/collector.js` extracts likely field names defensively.
Verify against one live response (`node src/index.js --parts 1 --state S01 --ac 26
--headless` then check `electors.raw`) and tighten the keys to match the real
payload — the Python client's exact mapping lives in `work/old_eci/client.py`.

## Responsible-use note

Uncapped probing should only run against infrastructure you own or are
explicitly authorized to load-test. AIMD + Retry-After respect is the difference
between finding the ceiling and causing an outage — keep that behavior on.
