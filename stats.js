// stats.js — the two exact counters over `electors`, computed once and shared.
//
// WHY THIS EXISTS
//
// `count(*) from electors` and `count(distinct cur_epic)` are full scans of an
// 8 GB table. The first version of this cache refreshed them per process every
// 120s, which on the live store became a stampede: every instance re-ran both
// scans every two minutes and none of them ever finished. Measured on
// 2026-10-09, four instances were each running `count(distinct cur_epic)` for
// 7+ minutes concurrently, holding the box's IO; `statement_timeout` is 0 on
// that server, so nothing bounded them. Every other query paid for it: a
// trivial aggregate that costs ~0.1s idle took 0.3-2.6s.
//
// THE RULE HERE
//
// The numbers stay EXACT - an operator must never see an estimate next to an
// exact count, or two dashboards disagreeing. Instead:
//   * the value lives in one `settings` row (`stats@<schema>`);
//   * exactly one process may refresh it at a time, claimed with a single
//     atomic statement (a lease in the row itself, so no lock service);
//   * the scan runs on one connection with `statement_timeout` set, so it can
//     never become an unbounded 8 GB scan again;
//   * every process reads that same row, so all dashboards agree and the age of
//     the number is shown rather than hidden.
//
// Net cost on a 4-instance fleet: one scan per ttl instead of four per 120s,
// single-stream instead of four concurrent.
import { q, q1, withStatementTimeout, logEvent } from './db.js';
import { config } from './config.js';

const EMPTY = {
  at: null, electors: null, unique_epics: null, updated_by: null, claimed_by: null,
};

// In-process memo only: the shared row is the source of truth, this just stops
// a 3s page poll from becoming a 3s settings read. Kept short so a value
// refreshed by another instance shows up quickly.
const MEMO_MS = 5_000;
let memo = { at: 0, value: null };
let inflight = null;

function parse(row) {
  const v = row?.value;
  if (!v || typeof v !== 'object') return { ...EMPTY };
  return {
    at: typeof v.at === 'string' ? v.at : null,
    electors: Number.isFinite(v.electors) ? v.electors : null,
    unique_epics: Number.isFinite(v.unique_epics) ? v.unique_epics : null,
    updated_by: v.updated_by ?? null,
    claimed_by: v.claimed_by ?? null,
    claimed_at: v.claimed_at ?? null,
  };
}

const ageSecs = (v) => (v?.at ? Math.max(0, Math.round((Date.now() - Date.parse(v.at)) / 1000)) : null);

/**
 * Try to become the one process allowed to run the scan.
 *
 * A single statement, so two instances racing cannot both win: the UPDATE half
 * of the upsert only fires when the previous claim is older than the lease, and
 * only the process that gets a row back proceeds. The claim is stored in the
 * same row as the value, so there is no second thing to keep consistent.
 */
async function claim() {
  const rows = await q(
    `insert into settings (key, value)
     values ($1, jsonb_build_object('claimed_at', to_jsonb(now()), 'claimed_by', $2::text))
     on conflict (key) do update
        set value = settings.value || jsonb_build_object('claimed_at', to_jsonb(now()), 'claimed_by', $2::text)
      where coalesce(nullif(settings.value->>'claimed_at', '')::timestamptz, to_timestamp(0))
            < now() - make_interval(secs => $3)
     returning key`,
    [config.statsKey, config.workerId, config.statsLeaseSec],
  );
  return rows.length > 0;
}

/** Give the lease back after a failure, so a timeout does not block every other
 * instance for the whole lease window. */
async function releaseClaim() {
  try {
    await q(`update settings set value = value - 'claimed_at' - 'claimed_by'
              where key = $1 and value ? 'claimed_at'`, [config.statsKey]);
  } catch { /* best effort */ }
}

async function runScan() {
  return withStatementTimeout(config.statsTimeoutMs, async (client) => {
    // Both counters in one connection, one after the other, so the table is
    // read once per scan rather than twice in parallel.
    const a = await client.query('select count(*)::int as c from electors');
    const b = await client.query(
      `select count(distinct cur_epic)::int as c from electors
        where cur_epic is not null and cur_epic <> ''`);
    return { electors: a.rows[0]?.c ?? null, unique_epics: b.rows[0]?.c ?? null };
  });
}

async function refresh() {
  const t0 = Date.now();
  try {
    const { electors, unique_epics } = await runScan();
    const value = {
      at: new Date().toISOString(),
      electors, unique_epics,
      updated_by: config.workerId,
      // Keep the claim: the lease starts now, so no other instance re-scans
      // while this value is being read.
      claimed_at: new Date().toISOString(),
      claimed_by: config.workerId,
    };
    await q(
      `insert into settings (key, value) values ($1, $2::jsonb)
       on conflict (key) do update set value = excluded.value, updated_at = now()`,
      [config.statsKey, JSON.stringify(value)],
    );
    const secs = ((Date.now() - t0) / 1000).toFixed(0);
    await logEvent(config.deviceTag, 'info',
      `shared stats refreshed by this instance in ${secs}s: `
      + `electors=${electors} unique_epics=${unique_epics}`, 'node');
    return value;
  } catch (e) {
    await releaseClaim();
    await logEvent(config.deviceTag, 'warn',
      `shared stats refresh failed after ${((Date.now() - t0) / 1000).toFixed(0)}s: ${e.message}`
      + ` (another instance may retry)`, 'node');
    return null;
  }
}

/**
 * The shared, exact counters plus how old they are.
 *
 * Never blocks: a caller always gets the last published value (nulls until the
 * first one lands). `refreshing` is true while this process is running the scan,
 * which is what lets the page say "computing" honestly instead of showing a
 * stale number as if it were fresh.
 */
export async function sharedStats() {
  if (memo.value && Date.now() - memo.at < MEMO_MS) {
    return { ...memo.value, age_secs: ageSecs(memo.value), refreshing: !!inflight };
  }
  let value;
  try {
    value = parse(await q1('select value from settings where key = $1', [config.statsKey]));
  } catch (e) {
    // The store being down is not this module's problem to report: the caller's
    // own health probes do that. Return whatever we last saw.
    return { ...(memo.value ?? EMPTY), age_secs: ageSecs(memo.value), refreshing: !!inflight, error: String(e.message || e) };
  }
  memo = { at: Date.now(), value };

  const age = ageSecs(value);
  const stale = age == null || age * 1000 >= config.statsTtlMs;
  if (stale && config.statsRefresh && !inflight) {
    inflight = (async () => {
      let won = false;
      try {
        won = await claim();          // one instance per lease wins this
      } catch (e) {
        // Never silent: a swallowed error here once hid a broken claim for a
        // whole test run, and a failing claim means the counters never refresh.
        console.error('shared stats claim failed:', e?.stack || e?.message || e);
        return;
      }
      if (!won) return;
      const fresh = await refresh();
      if (fresh) memo = { at: Date.now(), value: parse({ value: fresh }) };
    })().catch((e) => {
      console.error('shared stats refresh failed:', e?.stack || e?.message || e);
    }).finally(() => { inflight = null; });
  }
  return { ...value, age_secs: age, refreshing: !!inflight };
}

/** Read-only view for the admin panel: same row, plus who computed it. */
export async function statsSummary() {
  const s = await sharedStats();
  return {
    ...s,
    key: config.statsKey,
    ttl_secs: Math.round(config.statsTtlMs / 1000),
    lease_secs: config.statsLeaseSec,
    this_instance_refreshes: !!config.statsRefresh,
  };
}
