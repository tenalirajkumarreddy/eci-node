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

## Layout

| file | role |
|---|---|
| `src/config.js` | env config; device tag; no caps |
| `src/db.js` | `pg` pool, `q()`, `logEvent()` |
| `src/schema.js` | DDL + skip-if-complete (no ACCESS EXCLUSIVE on live DBs) |
| `src/aimd.js` | congestion controller (the "no cap" piece) |
| `src/api.js` | gateway client: keep-alive fetch, timeout, 404-as-empty |
| `src/claim.js` | atomic claim / reserve / take / complete / stale reaper |
| `src/collector.js` | part sweep, serial iteration, batched upserts |
| `src/worker.js` | auto loop, reservations, crash recovery |
| `src/epic.js` | EPIC processor (national search, passKey = SHA512(APP_CONST+key)) |
| `src/server.js` | zero-dep HTTP dashboard + JSON API |
| `src/index.js` | CLI entry |

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
