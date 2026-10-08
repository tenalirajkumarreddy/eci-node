// db.js — the pg pool, the query helper and the event log.
//
// Same shape as db.py: one shared pool (the store is remote, so reconnecting
// per query hurts) with `set search_path to <schema>, public` applied to every
// connection, a `q()` that returns rows, and an event log that never throws.
import pg from 'pg';
import { config } from './config.js';

const { Pool } = pg;

export const pool = new Pool({
  connectionString: config.pgDsn,
  max: config.pgPoolMax,
  connectionTimeoutMillis: config.pgConnectTimeoutMs,
  idleTimeoutMillis: 30_000,
  // Applied per connection: unqualified names resolve to the fleet's schema
  // first, exactly like db.connect()/pool() on the Python side.
  options: `-c search_path=${config.schema},public`,
  application_name: config.workerId,
});

// A dropped remote connection must surface as a rejection, not hang the sweep.
// Recorded rather than swallowed: the dashboard shows the last one so a flaky
// link is visible instead of silently retried forever.
let lastPoolError = null;
pool.on('error', (e) => {
  lastPoolError = { message: String(e?.message || e), at: new Date().toISOString() };
});

/** Connection health for the dashboard: a real round trip, not just a flag,
 * plus the pool's own numbers so a saturated pool is visible too. */
export async function dbStatus() {
  const t0 = Date.now();
  const pool_state = {
    pool_total: pool.totalCount, pool_idle: pool.idleCount, pool_waiting: pool.waitingCount,
  };
  try {
    await q('select 1');
    return { connected: true, ping_ms: Date.now() - t0, last_error: lastPoolError, ...pool_state };
  } catch (e) {
    return {
      connected: false, ping_ms: Date.now() - t0, error: String(e?.message || e),
      last_error: lastPoolError, ...pool_state,
    };
  }
}

/** Run a statement, return its rows.
 *
 * With no parameters we call pool.query(text) rather than pool.query(text, []) on
 * purpose: passing a values array makes node-postgres use the extended
 * protocol, which refuses multi-statement text. The DDL block in schema.js is
 * one multi-statement string, so it needs the simple protocol.
 */
export async function q(text, params = []) {
  const res = params && params.length
    ? await pool.query(text, params)
    : await pool.query(text);
  return res.rows;
}

/** One row or null. */
export async function q1(text, params = []) {
  return (await q(text, params))[0] ?? null;
}

/** Append to the shared, per-device event stream the dashboards read. */
export async function logEvent(device, level, message, source = 'node') {
  try {
    await q(
      'insert into events(level, source, message, device) values ($1,$2,$3,$4)',
      [level, source, String(message).slice(0, 4000), device],
    );
  } catch {
    // Logging must never be the reason a sweep dies.
  }
}
