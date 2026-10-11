// catalog.js — the operator's actions: seed states, seed ACs, discover parts.
//
// Ported from render_app/worker.py so the Node panel grows the catalogue with
// exactly the rules the Python fleet used, against the same tables:
//
//   * seed_states  — the legacy catalogue DB, with the live state list merged on
//                    top (the catalogue is the source of state_cd/name pairs).
//   * seed_acs_all — the legacy catalogue merged with the LIVE AC list, because
//                    the catalogue alone is not complete: it lists 175 ACs for
//                    S01 while the gateway serves 187, and every one of the 12
//                    extra ACs has old-roll data. Taking the catalogue at face
//                    value silently skipped them, so live wins per (state, ac).
//   * discover_parts — probe part numbers upward until a whole chunk comes back
//                    empty, then write the parts it found as `pending` for the
//                    whole fleet to sweep.
//
// Discovery is a QUEUE over acs.discover_status, not a job queue: `claimNextAc`
// takes the next undiscovered AC with one atomic UPDATE, so several instances
// can discover in parallel without a lock, a crash leaves the claim to be reaped
// after `discover_stale_min`, and a restart resumes where it stopped.
import { q, q1, geoQ, logEvent } from './db.js';
import { config } from './config.js';
import { Aimd } from './aimd.js';
import { EciClient, statesLive, acsLive, acTypeLabel } from './api.js';
import { getSetting } from './claim.js';

// ------------------------------------------------------------ job registry
//
// One catalogue job at a time PER PROCESS. Cross-instance safety comes from the
// work itself (the AC claim queue above), never from this: this only keeps one
// panel tab from starting two jobs in the same container.
let CURRENT = null;
let LAST = null;

export function jobStatus() {
  return { running: CURRENT, last: LAST };
}

/**
 * Start a job, or refuse because one is already running here.
 * The job runs in the background; its progress is visible through jobStatus()
 * and every durable step is in `events` / `acs`, so a page reload loses nothing.
 */
export function runJob(kind, detail, fn) {
  if (CURRENT) return { started: false, reason: `a ${CURRENT.kind} job is already running in this process` };
  const job = {
    kind, detail,
    started_at: new Date().toISOString(),
    progress: {}, result: null, error: null, done: false,
  };
  CURRENT = job;
  const progress = (patch) => { job.progress = { ...job.progress, ...patch }; };
  // NOTE: the three action starters below are deliberately NOT async - they
  // only report whether the job started, and an `async` wrapper would hand
  // callers a Promise whose `.started` is undefined. That is exactly the bug
  // this comment is here to stop coming back.
  (async () => {
    try {
      job.result = await fn(progress);
    } catch (e) {
      job.error = String(e?.message || e);
      await logEvent(config.deviceTag, 'error', `${kind} failed: ${job.error}`, 'node');
    } finally {
      job.done = true;
      job.finished_at = new Date().toISOString();
      LAST = job;
      CURRENT = null;
    }
  })();
  return { started: true, job };
}

const UNNEST_STATES = `insert into states (state_cd, name, source, last_checked)
  select t.state_cd, t.name, t.source, now()
    from unnest($1::text[], $2::text[], $3::text[]) as t(state_cd, name, source)
  on conflict (state_cd) do update
    set name = coalesce(excluded.name, states.name)`;

// ---------------------------------------------------------------- seeding

/**
 * Seed the state list. The legacy catalogue is authoritative for state_cd/name;
 * the live list is merged on top so a state the catalogue lacks still appears
 * (the live answer carried U01 and 36 rows on 2026-10-09). Nothing is ever
 * deleted, so re-running is safe and cannot lose a row a device is using.
 */
export function seedStates({ live = true } = {}) {
  return runJob('seed_states', {}, async (progress) => {
    const legacy = await geoQ('select state_cd, name from public.states order by state_cd');
    const merged = new Map();
    for (const r of legacy) merged.set(String(r.state_cd), { name: r.name ?? null, source: 'db' });
    const fromLegacy = merged.size;

    let liveOnly = 0;
    let liveRows = 0;
    if (live) {
      const rows = await statesLive();
      liveRows = rows.length;
      for (const r of rows) {
        const cur = merged.get(r.state_cd);
        if (!cur) { merged.set(r.state_cd, { name: r.name ?? null, source: 'api' }); liveOnly += 1; continue; }
        if (!cur.name && r.name) cur.name = r.name;
      }
    }

    progress({ phase: 'writing', states: merged.size });
    const keys = [...merged.keys()].sort();
    if (keys.length) {
      await q(UNNEST_STATES, [
        keys,
        keys.map((k) => merged.get(k).name),
        keys.map((k) => merged.get(k).source),
      ]);
    }
    const out = {
      states: keys.length, from_legacy: fromLegacy, live: liveRows, live_only: liveOnly,
      catalogue_available: legacy.length > 0,
    };
    await logEvent(config.deviceTag, 'info',
      `seeded ${out.states} states (${fromLegacy} from the legacy catalogue, `
      + `${liveOnly} only in the live list)`, 'node');
    return out;
  });
}

/**
 * Seed ACs for one state, or for every state in `states`.
 *
 * One batched write per call: the legacy catalogue rows plus the live rows
 * become a single `unnest` insert, so 4k ACs cost one round trip instead of one
 * per row (the Python per-state path round-trips per row and takes minutes).
 */
export function seedAcs({ state = null, live = true } = {}) {
  return runJob('seed_acs', { state }, async (progress) => {
    const merged = new Map();
    const key = (sc, ac) => `${sc}|${ac}`;

    const geo = await geoQ(
      `select state_cd, ac_number, ac_name, district_cd, category
         from public.acs ${state ? 'where state_cd = $1' : ''}
        order by state_cd, ac_number`,
      state ? [state] : []);
    for (const r of geo) {
      merged.set(key(r.state_cd, Number(r.ac_number)), {
        state_cd: String(r.state_cd), ac_no: Number(r.ac_number),
        name: r.ac_name ?? null, name_l1: null,
        district_cd: r.district_cd == null ? null : String(r.district_cd),
        ac_type: acTypeLabel(r.category),
      });
    }
    const fromLegacy = merged.size;

    // Which states to ask the live list for: the requested one, or every state
    // we know - exactly like seed_acs_all.
    const states = state
      ? [state]
      : (await q('select state_cd from states order by state_cd')).map((r) => r.state_cd);

    let liveOnly = 0;
    let failed = 0;
    const perState = [];
    for (let i = 0; i < states.length; i++) {
      const sc = states[i];
      const rows = live ? await acsLive(sc) : [];
      if (live && !rows.length) failed += 1;
      for (const r of rows) {
        const k = key(sc, r.ac_no);
        let cur = merged.get(k);
        if (!cur) {
          cur = { state_cd: sc, ac_no: r.ac_no, name: null, name_l1: null, district_cd: null, ac_type: null };
          merged.set(k, cur);
          liveOnly += 1;
        }
        cur.name = r.name || cur.name;
        cur.name_l1 = r.name_l1 || cur.name_l1;
        cur.ac_type = r.ac_type || cur.ac_type;
        cur.district_cd = r.district_cd || cur.district_cd;
      }
      perState.push(sc);
      progress({ phase: 'live', states_done: i + 1, states_total: states.length, acs: merged.size });
    }

    if (!merged.size) {
      return { acs: 0, from_legacy: 0, live_only: 0, note: 'no catalogue rows and no live answer' };
    }

    const rows = [...merged.values()].sort((a, b) =>
      a.state_cd === b.state_cd ? a.ac_no - b.ac_no : (a.state_cd < b.state_cd ? -1 : 1));
    progress({ phase: 'writing', acs: rows.length });
    await q(
      `insert into acs (state_cd, ac_no, name, name_l1, district_cd, ac_type)
       select t.state_cd, t.ac_no, t.name, t.name_l1, t.district_cd, t.ac_type
         from unnest($1::text[], $2::int[], $3::text[], $4::text[], $5::text[], $6::text[])
              as t(state_cd, ac_no, name, name_l1, district_cd, ac_type)
       on conflict (state_cd, ac_no) do update
         set name        = coalesce(excluded.name, acs.name),
             name_l1     = coalesce(excluded.name_l1, acs.name_l1),
             ac_type     = coalesce(excluded.ac_type, acs.ac_type),
             district_cd = coalesce(excluded.district_cd, acs.district_cd)`,
      [rows.map((r) => r.state_cd), rows.map((r) => r.ac_no), rows.map((r) => r.name),
       rows.map((r) => r.name_l1), rows.map((r) => r.district_cd), rows.map((r) => r.ac_type)],
    );

    const out = {
      acs: rows.length,
      states: new Set(rows.map((r) => r.state_cd)).size,
      from_legacy: fromLegacy,
      live_only: liveOnly,
      live_failed_states: failed,
    };
    await logEvent(config.deviceTag, 'info',
      `seeded ${out.acs} ACs across ${out.states} states `
      + `(${fromLegacy} from the legacy catalogue, ${liveOnly} only in the live list`
      + (failed ? `, ${failed} live list request(s) failed` : '') + ')', 'node');
    return out;
  });
}

// -------------------------------------------------------------- discovery

// Discovery traffic is its own client with its own controller, so probing never
// competes with (or hides behind) the sweeper's AIMD ceiling.
let adminClient = null;
function client() {
  if (!adminClient) {
    adminClient = new EciClient(config, new Aimd({
      start: Math.min(4, config.discoverConcurrency), max: 0, windowMs: config.aimdWindowMs,
    }));
  }
  return adminClient;
}

/** Bounded-concurrency map: keeps the gateway load flat instead of 200 at once. */
async function poolMap(items, limit, fn) {
  const out = new Array(items.length);
  let next = 0;
  const runners = Array.from({ length: Math.max(1, Math.min(limit, items.length)) }, async () => {
    for (;;) {
      const i = next++;
      if (i >= items.length) return;
      out[i] = await fn(items[i]);
    }
  });
  await Promise.all(runners);
  return out;
}

/**
 * Does this part number exist, and what is it called?
 *
 * The empty-serial window ('' -> ~50 records at a random offset) is the
 * discovery call: it answers for a part whose serial 1 is missing. A 200 with no
 * records still means "exists" in the Python original, which then checks a few
 * concrete serials - kept, because a part can hold only high serials.
 */
async function probePart(stateCd, acNo, n) {
  let status = 0;
  let payload = null;
  try {
    payload = await client().getEroll(stateCd, acNo, n, '');
    status = 200;
  } catch (e) {
    status = Number(e?.status) || 0;
  }
  if (status === 200) {
    const name = payload.find((r) => r && r.oldPartName)?.oldPartName ?? null;
    if (payload.length) return { n, exists: true, name };
    for (const serial of [1, 25, 100]) {
      try {
        const p = await client().getEroll(stateCd, acNo, n, serial);
        if (p.length) return { n, exists: true, name: p[0]?.oldPartName ?? null };
      } catch { /* keep checking the other serials */ }
    }
    return { n, exists: false, name: null };
  }
  // 404 is the route's ORDINARY answer for "nothing at this part number". Any
  // other status (5xx, timeout) also ends as not-found, but is counted so a
  // broken gateway is visible instead of looking like an empty AC.
  return { n, exists: false, name: null, transport: status === 404 ? null : status };
}

/**
 * Probe one AC's part numbers and write the parts found.
 *
 * `floor` (the `discover_max_part` setting, default 400) is a FLOOR, not a
 * limit: probing continues while the top of each chunk answers, up to the hard
 * cap. Stopping exactly at a cap truncates silently - S01 AC 1 was once recorded
 * as "12 parts, done" and hid 141 real parts until a cross-check caught it.
 */
export async function discoverAc({ state_cd, ac_no, floor = null, hardCap = null, progress = null }) {
  const stateCd = String(state_cd);
  const acNo = Number(ac_no);
  const hard = Math.max(1, Number(hardCap) || config.discoverHardCap);
  const configuredFloor = floor ?? await getSetting(config.deviceTag, 'discover_max_part', 400);
  let floorN = Math.max(1, Number(configuredFloor) || 400);
  if (floorN > hard) floorN = hard;

  await q(`update acs set discover_status = 'running', discover_max = $3,
                  last_error = null, discover_started_at = now()
            where state_cd = $1 and ac_no = $2`, [stateCd, acNo, floorN]);

  const found = new Map();
  let probedTo = 0;
  let transportErrors = 0;
  try {
    let start = 1;
    while (start <= hard) {
      let end = Math.min(start + config.discoverChunk - 1, hard);
      // While below the floor, keep going at least to it.
      if (probedTo < floorN) end = Math.min(Math.max(end, floorN), hard);
      const nums = [];
      for (let n = start; n <= end; n++) nums.push(n);

      const results = await poolMap(nums, config.discoverConcurrency, async (n) => {
        try { return await probePart(stateCd, acNo, n); }
        catch (e) { return { n, exists: false, name: null, transport: Number(e?.status) || 0 }; }
      });
      let hits = 0;
      for (const r of results) {
        if (r.transport) transportErrors += 1;
        if (r.exists) { found.set(r.n, r.name ?? null); hits += 1; }
      }
      probedTo = end;
      if (progress) progress({ phase: 'discover', state_cd: stateCd, ac_no: acNo, probed_to: probedTo, found: found.size });
      // A whole empty chunk at or past the floor means there is nothing above.
      if (hits === 0 && probedTo >= floorN) break;
      start = end + 1;
    }

    const entries = [...found.entries()].sort((a, b) => a[0] - b[0]);
    for (let i = 0; i < entries.length; i += 500) {
      const slice = entries.slice(i, i + 500);
      await q(
        `insert into old_parts (state_cd, ac_no, part_no, name, exists_, status)
         select $1, $2, t.n, t.nm, true, 'pending'
           from unnest($3::int[], $4::text[]) as t(n, nm)
         on conflict (state_cd, ac_no, part_no) do update
           set name = coalesce(excluded.name, old_parts.name),
               exists_ = true, updated_at = now()`,
        [stateCd, acNo, slice.map(([n]) => n), slice.map(([, nm]) => nm)],
      );
    }

    const truncated = found.has(probedTo) && probedTo >= hard;
    await q(`update states set has_old_data = true, last_checked = now() where state_cd = $1`, [stateCd]);
    await q(`update acs set discover_status = 'done', old_parts_found = $3,
                    discover_max = $4, discovered_at = now(), last_error = null
              where state_cd = $1 and ac_no = $2`, [stateCd, acNo, entries.length, probedTo]);
    await logEvent(config.deviceTag, 'info',
      `discover ${stateCd} AC ${acNo}: ${entries.length} old parts (probed 1..${probedTo}`
      + `${truncated ? ' - TRUNCATED at the hard cap' : ''})`
      + (transportErrors ? `, ${transportErrors} transport error(s)` : ''), 'node');
    return { found: entries.length, probed_to: probedTo, truncated, transport_errors: transportErrors };
  } catch (e) {
    await q(`update acs set discover_status = 'error', last_error = $3
              where state_cd = $1 and ac_no = $2`, [stateCd, acNo, String(e?.message || e).slice(0, 500)]);
    await logEvent(config.deviceTag, 'error',
      `discover ${stateCd} AC ${acNo} failed: ${e.message}`, 'node');
    throw e;
  }
}

/**
 * Claim the next undiscovered AC, atomically.
 *
 * One statement, so N instances can run this concurrently and each gets a
 * different AC - no lock, no coordinator. An AC stuck in 'running' (the process
 * died mid-probe) becomes claimable again after `discover_stale_min`, which is
 * what makes a crash resume instead of stranding work.
 */
export async function claimNextAc({ state = null } = {}) {
  const rows = await q(
    `update acs
        set discover_status = 'running', discover_started_at = now(), last_error = null
      where (state_cd, ac_no) in (
              select state_cd, ac_no from acs
               where ($1::text is null or state_cd = $1::text)
                 and (discover_status = 'pending'
                      or (discover_status = 'running'
                          and (discover_started_at is null
                               or discover_started_at < now() - make_interval(mins => $2))))
               order by state_cd, ac_no
               limit 1)
      returning state_cd, ac_no`,
    [state, config.discoverStaleMin],
  );
  return rows[0] || null;
}

/** Claim and discover up to `batch` ACs (one panel click, or one CLI run). */
export function discoverNext({ state = null, batch = 1, floor = null, hardCap = null } = {}) {
  const n = Math.max(1, Math.min(Number(batch) || 1, 500));
  return runJob('discover_parts', { state, batch: n }, async (progress) => {
    const results = [];
    for (let i = 0; i < n; i++) {
      const ac = await claimNextAc({ state });
      if (!ac) { results.push({ exhausted: true }); break; }
      progress({ phase: 'discover', done: i, batch: n, state_cd: ac.state_cd, ac_no: ac.ac_no });
      try {
        const r = await discoverAc({ state_cd: ac.state_cd, ac_no: ac.ac_no, floor, hardCap, progress });
        results.push({ state_cd: ac.state_cd, ac_no: ac.ac_no, ...r });
      } catch (e) {
        results.push({ state_cd: ac.state_cd, ac_no: ac.ac_no, error: String(e?.message || e) });
      }
    }
    const done = results.filter((r) => !r.exhausted && !r.error);
    const out = {
      acs_done: done.length,
      parts_found: done.reduce((a, r) => a + (r.found || 0), 0),
      errors: results.filter((r) => r.error).length,
      exhausted: results.some((r) => r.exhausted),
      results,
    };
    await logEvent(config.deviceTag, 'info',
      `discover batch: ${out.acs_done} AC(s) done, ${out.parts_found} parts added`
      + (out.errors ? `, ${out.errors} error(s)` : '')
      + (out.exhausted ? ' - queue empty' : ''), 'node');
    return out;
  });
}

/** Put failed ACs back in the queue (one panel click after fixing the cause). */
export async function retryErroredAcs({ state = null } = {}) {
  const rows = await q(
    `update acs set discover_status = 'pending', discover_started_at = null
      where discover_status = 'error' and ($1::text is null or state_cd = $1::text)
      returning state_cd, ac_no`, [state]);
  await logEvent(config.deviceTag, 'info', `requeued ${rows.length} errored AC(s)`, 'node');
  return { requeued: rows.length };
}

/** Discovery backlog, for the panel's progress line (one cheap aggregate). */
export async function acsBacklog({ state = null } = {}) {
  return q(
    `select discover_status,
            count(*)::int as acs,
            count(*) filter (where discover_max is not null
                             and old_parts_found >= discover_max)::int as maybe_truncated
       from acs where ($1::text is null or state_cd = $1::text)
      group by 1 order by 1`, [state]);
}
