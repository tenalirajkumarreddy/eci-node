// config.js — environment configuration for the Node collector.
//
// Loads `.env` from the project root (or the cwd) before reading anything, so
// `npm start` behaves like the Python fleet's dotenv load. Zero dependencies:
// this uses Node's own process.loadEnvFile() (>= 20.12) and ignores a missing
// file, where the Python side silently fell back to its DSN default.
//
// There is deliberately NO req/s constant and NO workers x parts_parallel cap
// here. The only throughput knobs are the AIMD controller's starting point and
// an optional client-side safety ceiling (see aimd.js): the real ceiling is
// discovered at runtime from the gateway.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));

/** Nearest directory containing package.json (works flat or under src/). */
function findRoot() {
  let dir = HERE;
  for (let i = 0; i < 4; i++) {
    if (fs.existsSync(path.join(dir, 'package.json'))) return dir;
    const up = path.dirname(dir);
    if (up === dir) break;
    dir = up;
  }
  return HERE;
}

export const ROOT = findRoot();

function loadEnvFile() {
  for (const candidate of [path.join(ROOT, '.env'), path.join(process.cwd(), '.env')]) {
    try {
      if (fs.existsSync(candidate)) {
        // Does not override variables already present in the environment, which
        // is what we want: an explicit export wins over the .env default.
        process.loadEnvFile(candidate);
        return candidate;
      }
    } catch { /* a malformed .env must not stop the collector */ }
  }
  return null;
}

export const envFile = loadEnvFile();

const str = (v, d = null) => (v === undefined || v === '' ? d : String(v));
const num = (v, d) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
};
const bool = (v, d) => {
  if (v === undefined || v === '') return d;
  return !['0', 'false', 'no', 'off'].includes(String(v).trim().toLowerCase());
};

const host = os.hostname();

// Falls back to the same shared cluster the Python app uses, so a fresh clone
// runs without a .env (the README's "copy .env.example" step only sets the
// per-device knobs). Point ECI_PG_DSN at another database to isolate a run.
const DEFAULT_DSN =
  'postgresql://eci_app:Raj%40A2Nkufyg@129.225.75.85:5432/old_eci';

export const config = {
  // ---- store ----
  pgDsn: str(process.env.ECI_PG_DSN, DEFAULT_DSN),
  // `set search_path` on every pooled connection, exactly like db.py, so the
  // unqualified table names below resolve to the fleet's schema.
  schema: str(process.env.ECI_PG_SCHEMA, 'public'),
  pgPoolMax: num(process.env.ECI_PG_POOL_MAX, 10),
  pgConnectTimeoutMs: num(process.env.ECI_PG_CONNECT_TIMEOUT_MS, 30_000),

  // ---- identity ----
  // DEVICE_TAG owns the reservation queue and the 'key@tag' settings rows.
  // WORKER_ID is what lands in old_parts.claimed_by, so the dashboard can tell
  // which PC process / phone / Node process holds a part. Same split as
  // worker.py (DEVICE_TAG vs "pc-<host>-<pid>").
  deviceTag: str(process.env.ECI_DEVICE_TAG, host),
  workerId: str(process.env.ECI_WORKER_ID, `node-${host}-${process.pid}`),

  // ---- throughput (AIMD, discovered not configured) ----
  startInflight: Math.max(1, num(process.env.ECI_START_INFLIGHT, 8)),
  // 0 => unbounded. Set only to protect the client (fds/memory), never as a
  // server-side target: the gateway's own limit is the target.
  maxInflight: Math.max(0, num(process.env.ECI_MAX_INFLIGHT, 0)),
  aimdWindowMs: Math.max(200, num(process.env.ECI_AIMD_WINDOW_MS, 2000)),

  // ---- sweeping ----
  httpTimeoutMs: Math.max(1000, num(process.env.ECI_HTTP_TIMEOUT_MS, 30_000)),
  batchSize: Math.max(1, num(process.env.ECI_BATCH_SIZE, 200)),
  // Stop sweeping a part after this many consecutive 404s past the roll end.
  serialStopAfter: Math.max(1, num(process.env.ECI_SERIAL_STOP_AFTER, 25)),
  // Data bound, not a rate cap: the Python collector's collect_serial_cap.
  serialCap: Math.max(1, num(process.env.ECI_SERIAL_CAP, 3000)),

  // ---- fleet coordination ----
  reserveN: Math.max(0, num(process.env.ECI_RESERVE_N, 5)),
  reserveTtlSec: Math.max(1, num(process.env.ECI_RESERVE_TTL, 900)),
  // Parts whose holder has gone silent for this long go back to pending; the
  // Python reaper uses the same 30 minutes on started_at.
  staleMin: Math.max(1, num(process.env.ECI_STALE_MIN, 30)),

  // ---- dashboard aggregates ----
  // `count(*) from electors` measured 51s and `v_overall` 117s on the live store
  // (two full scans, right at the database's own 120s statement_timeout), and
  // the dashboard polls every 3s. The heavy counters are therefore cached this
  // long and refreshed in the background - same approach and spirit as app.py's
  // AGG_TTL=120. Nothing in a KPI card needs to be exact to the second.
  heavyTtlMs: Math.max(0, num(process.env.ECI_HEAVY_TTL_MS, 120_000)),

  // ---- EPIC search (national display, ~1 req/s) ----
  epicMinIntervalMs: Math.max(0, num(process.env.ECI_EPIC_MIN_INTERVAL_MS, 1100)),
  // The verified tc/external constants live with the reverse-engineering work;
  // override with ECI_APP_KEYS to point at your own copy.
  appKeysPath: str(
    process.env.ECI_APP_KEYS,
    path.resolve(ROOT, '..', 'eci rev eng', 'work', 'app_keys.json'),
  ),
};
