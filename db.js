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

const poolNumbers = () => ({
  pool_total: pool.totalCount, pool_idle: pool.idleCount, pool_waiting: pool.waitingCount,
});

/** Connection health for the dashboard: a real round trip, not just a flag,
 * plus the pool's own numbers so a saturated pool is visible too. */
export async function dbStatus() {
  const t0 = Date.now();
  const pool_state = poolNumbers();
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

/**
 * Full health of the store behind the panel.
 *
 * Everything here comes from the catalogue views (`pg_settings`,
 * `pg_stat_activity`, `pg_class`) - deliberately no table scans, because this
 * runs on the same box that four collectors are writing to, and the panel polls
 * it. Callers memoise it (server.js) so one page load cannot become a DB poll.
 */
export async function dbHealth() {
  const t0 = Date.now();
  const base = { ...poolNumbers(), last_error: lastPoolError };
  try {
    const [info] = await q(`
      select version()                                                    as version,
             now() - pg_postmaster_start_time()                            as uptime,
             pg_size_pretty(pg_database_size(current_database()))          as db_size,
             current_setting('statement_timeout')                          as statement_timeout,
             current_setting('max_connections')                            as max_connections,
             current_setting('shared_buffers')                             as shared_buffers,
             current_setting('work_mem')                                   as work_mem`);
    const conns = await q(`
      select coalesce(nullif(application_name, ''), '(unnamed)') as app,
             state, count(*)::int as n,
             max(now() - query_start) filter (where state <> 'idle') as oldest_active
        from pg_stat_activity
       where datname = current_database()
       group by 1, 2 order by n desc`);
    const running = await q(`
      select pid, application_name as app, state,
             (now() - query_start) as age, wait_event_type, wait_event,
             left(regexp_replace(coalesce(query, ''), '\\s+', ' ', 'g'), 240) as query
        from pg_stat_activity
       where datname = current_database() and state <> 'idle' and query_start is not null
       order by query_start limit 8`);
    const tables = await q(`
      select relname,
             pg_size_pretty(pg_total_relation_size(c.oid)) as size,
             pg_total_relation_size(c.oid)                 as bytes,
             (select n_live_tup from pg_stat_user_tables s where s.relid = c.oid) as est_rows,
             (select last_autovacuum from pg_stat_user_tables s where s.relid = c.oid) as last_autovacuum
        from pg_class c join pg_namespace n on n.oid = c.relnamespace
       where n.nspname = $1 and c.relkind = 'r'
       order by pg_total_relation_size(c.oid) desc limit 8`, [config.schema]);
    const version = String(info.version).split(' on ')[0];
    return {
      connected: true, ping_ms: Date.now() - t0, ...base,
      server: {
        version,
        uptime_secs: Math.round(msOf(info.uptime) / 1000),
        db_size: info.db_size,
        statement_timeout: info.statement_timeout,
        max_connections: Number(info.max_connections),
        shared_buffers: info.shared_buffers,
        work_mem: info.work_mem,
      },
      connections: conns.map((r) => ({
        app: r.app, state: r.state, n: r.n,
        oldest_active_secs: r.oldest_active == null ? null : Math.round(msOf(r.oldest_active) / 1000),
      })),
      active: running.map((r) => ({
        pid: r.pid, app: r.app, state: r.state, age_secs: Math.round(msOf(r.age) / 1000),
        wait: r.wait_event ? `${r.wait_event_type}/${r.wait_event}` : null,
        query: r.query,
      })),
      tables: tables.map((r) => ({
        name: r.relname, size: r.size, bytes: Number(r.bytes),
        est_rows: r.est_rows == null ? null : Number(r.est_rows),
        last_autovacuum: r.last_autovacuum,
      })),
    };
  } catch (e) {
    return { connected: false, ping_ms: Date.now() - t0, ...base, error: String(e?.message || e) };
  }
}

/** interval -> milliseconds (pg returns a JS object of day/hour/min/sec parts). */
function msOf(v) {
  if (v == null) return 0;
  if (typeof v === 'number') return v;
  if (typeof v === 'string') { const n = Number(v); return Number.isFinite(n) ? n : 0; }
  return ((((Number(v.days) || 0) * 24 + (Number(v.hours) || 0)) * 60 + (Number(v.minutes) || 0)) * 60
    + (Number(v.seconds) || 0)) * 1000 + (Number(v.milliseconds) || 0);
}

/**
 * Read-only access to the legacy catalogue database (`eci`, same server).
 *
 * Best-effort exactly like db.py's geo_q: any failure - no such database, no
 * privilege, host unreachable - returns [] so the seed actions fall back to the
 * live gateway list instead of failing. Nothing ever writes here.
 */
let geoPool = null;
export async function geoQ(text, params = []) {
  if (!config.geoDsn) return [];
  if (!geoPool) {
    geoPool = new pg.Pool({
      connectionString: config.geoDsn,
      max: 2,
      connectionTimeoutMillis: 10_000,
      idleTimeoutMillis: 30_000,
      options: '-c statement_timeout=60000',
      application_name: `${config.workerId}-geo`,
    });
    geoPool.on('error', () => {});
  }
  try {
    return (await geoPool.query(text, params)).rows;
  } catch {
    return [];
  }
}

/**
 * Run statements on one dedicated connection with a statement timeout.
 *
 * Used by the two electors counters: they are full scans of an 8 GB table, and
 * the server's own `statement_timeout` is 0 (measured), so without this they can
 * outlive the process that started them - which is exactly how the old
 * per-instance cache turned into four unbounded scans running for minutes.
 */
export async function withStatementTimeout(ms, fn) {
  const client = await pool.connect();
  try {
    await client.query(`set statement_timeout = ${Math.max(1000, Math.round(ms))}`);
    return await fn(client);
  } finally {
    try { await client.query('reset statement_timeout'); } catch { /* connection going away */ }
    client.release();
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
