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

const schema = str(process.env.ECI_PG_SCHEMA, 'public');

/** The legacy catalogue (states / ACs) lives in the `eci` database on the same
 * server. Deriving its DSN from the fleet DSN keeps one copy of the credential
 * in the environment instead of a second hardcoded password. Returns null when
 * the DSN is unparseable, which geoQ() treats as "catalogue unavailable". */
function deriveGeoDsn(dsn) {
  try {
    const u = new URL(dsn);
    u.pathname = '/eci';
    return u.toString();
  } catch { return null; }
}

// EPIC keys (tc/external). A copy committed next to this file wins, so a fresh
// clone (and the Render deploy) has working --epic search; otherwise fall back
// to the reverse-engineering checkout this project was built beside.
function defaultAppKeysPath() {
  const local = path.resolve(ROOT, 'app_keys.json');
  if (fs.existsSync(local)) return local;
  return path.resolve(ROOT, '..', 'eci rev eng', 'work', 'app_keys.json');
}

export const config = {
  // ---- store ----
  pgDsn: str(process.env.ECI_PG_DSN, DEFAULT_DSN),
  // `set search_path` on every pooled connection, exactly like db.py, so the
  // unqualified table names below resolve to the fleet's schema.
  schema,
  // Read-only catalogue source for the seed actions; nothing ever writes to it.
  geoDsn: str(process.env.ECI_GEO_DSN, null) || deriveGeoDsn(str(process.env.ECI_PG_DSN, DEFAULT_DSN)),
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

  // ---- dashboard aggregates (stats.js) ----
  //
  // `count(*) from electors` and `count(distinct cur_epic)` are full scans of an
  // 8 GB table. The original 120s per-process cache turned into a stampede:
  // every instance re-ran both scans every two minutes and never finished
  // (measured: four concurrent 7-minute scans holding the box's IO), because
  // nothing sets statement_timeout and the server's own value is 0.
  //
  // The counters therefore stay EXACT but are computed by one process at a time
  // and published to a shared settings row, so every dashboard shows the same
  // number (never an estimate beside an exact value) with its age.
  statsKey: `stats@${schema}`,
  statsTtlMs: Math.max(30_000, num(process.env.ECI_STATS_TTL_MS, 300_000)),
  // How long a refresh claim is honoured. Must exceed a realistic scan time on
  // the loaded store, otherwise a second process starts a scan while the first
  // is still reading.
  statsLeaseSec: Math.max(60, num(process.env.ECI_STATS_LEASE_SEC, 900)),
  // false on instances that must never pay for the scan (they still read it).
  statsRefresh: bool(process.env.ECI_STATS_REFRESH, true),
  // Hard ceiling for the one scan, applied as `set local statement_timeout`.
  statsTimeoutMs: Math.max(10_000, num(process.env.ECI_STATS_TIMEOUT_MS, 600_000)),

  // ---- catalog admin (catalog.js) ----
  // Part numbers are probed in chunks and discovery stops only when a whole
  // chunk comes back empty, so the configured floor is a FLOOR, not a limit
  // (stopping exactly at a cap silently truncates - see worker.py).
  discoverChunk: Math.max(1, num(process.env.ECI_DISCOVER_CHUNK, 200)),
  discoverHardCap: Math.max(1, num(process.env.ECI_DISCOVER_HARD_CAP, 3000)),
  discoverConcurrency: Math.max(1, num(process.env.ECI_DISCOVER_CONCURRENCY, 8)),
  // An AC claimed as 'running' by a process that went away goes back in the
  // queue after this long.
  discoverStaleMin: Math.max(1, num(process.env.ECI_DISCOVER_STALE_MIN, 30)),

  // ---- fleet health (worker heartbeat) ----
  // One small settings row per device: AIMD counters (so 429s become visible),
  // the current part, and the egress IP - the number that answers "can I add
  // another device", because Render shares its regional outbound IPs across
  // services.
  heartbeatMs: Math.max(5_000, num(process.env.ECI_HEARTBEAT_MS, 15_000)),
  reportIp: bool(process.env.ECI_REPORT_IP, true),
  // How long the dashboard memoises the (expensive) fleet aggregates and the DB
  // health probe, so a 3s page poll never becomes a DB poll.
  fleetTtlMs: Math.max(1_000, num(process.env.ECI_FLEET_TTL_MS, 15_000)),
  dbHealthTtlMs: Math.max(1_000, num(process.env.ECI_DB_HEALTH_TTL_MS, 10_000)),

  // ---- parts collection (collection.js) ----
  // A manual seed writes part rows the whole fleet will sweep, so one click is
  // capped: past this many part numbers the operator should be probing (which
  // only writes the parts that exist) instead of hand-typing a range.
  seedMaxParts: Math.max(1, num(process.env.ECI_SEED_MAX_PARTS, 2000)),
  // The relation/gender breakdown aggregates `electors`. Scoped to a state/AC it
  // rides the (state_cd, ac_no, part_no) index; unscoped it is an 8 GB scan, so
  // it is memoised and hard-bounded by a statement timeout, exactly like the
  // shared counters in stats.js.
  breakdownTtlMs: Math.max(10_000, num(process.env.ECI_BREAKDOWN_TTL_MS, 300_000)),
  breakdownTimeoutMs: Math.max(5_000, num(process.env.ECI_BREAKDOWN_TIMEOUT_MS, 120_000)),

  // ---- EPIC search (national display, ~1 req/s) ----
  epicMinIntervalMs: Math.max(0, num(process.env.ECI_EPIC_MIN_INTERVAL_MS, 1100)),
  // Repo copy first, then the reverse-engineering checkout; override with
  // ECI_APP_KEYS to point anywhere else.
  appKeysPath: str(process.env.ECI_APP_KEYS, defaultAppKeysPath()),
};
