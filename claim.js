// claim.js — the fleet's arbitration layer, ported from worker.py.
//
// This file is the contract with every OTHER collector: the Python web
// workers, the Android devices and this Node process all race on one
// `old_parts` table, so the rules here are deliberately the same statements
// worker.py runs, against the same composite key (state_cd, ac_no, part_no).
//
// Identity is split exactly like the Python side:
//   * DEVICE_TAG ("deviceTag") owns the reservation queue and the 'key@tag'
//     settings rows - one tag per machine;
//   * WORKER_ID ("workerId", "node-<host>-<pid>") is what lands in
//     `claimed_by`, so the dashboard can name the process holding a part.
//
// There is no `id` column on old_parts: the primary key IS
// (state_cd, ac_no, part_no), and every claim is an upsert on that key.
import { q, q1 } from './db.js';
import { config } from './config.js';

// ---------------------------------------------------------------- settings
//
// Settings resolve per device: '<key>@<tag>' first, then the shared '<key>'.
// Same layout db.py uses, so a knob set by the Python dashboard is visible
// here and vice-versa - the value stays jsonb, never a JSON string.

export async function getSetting(device, key, def = null) {
  if (device) {
    const row = await q1('select value from settings where key = $1', [`${key}@${device}`]);
    if (row) return row.value;
  }
  const row = await q1('select value from settings where key = $1', [key]);
  return row ? row.value : def;
}

export async function setSetting(device, key, value) {
  const k = device ? `${key}@${device}` : key;
  await q(
    `insert into settings (key, value) values ($1, $2::jsonb)
     on conflict (key) do update set value = excluded.value, updated_at = now()`,
    [k, JSON.stringify(value === undefined ? null : value)],
  );
}

/** All of this device's knobs, with the '@tag' suffix stripped. */
export async function deviceSettings(device) {
  const rows = await q(
    `select key, value from settings where key like '%@' || $1 and key <> $1`, [device],
  );
  return Object.fromEntries(rows.map((r) => [r.key.slice(0, r.key.lastIndexOf('@')), r.value]));
}

// ------------------------------------------------------------------- picks
//
// A part is fair game when nobody else holds a LIVE reservation on it. An
// expired hold is not a hold (the expiry is a time predicate, so a dead
// device's queue frees itself without a reaper). Both the picker and the claim
// test this, so a race between them cannot steal a live hold.

const RESV_OK = `(reserved_by is null or reserved_by = $2
                  or reserved_at < now() - make_interval(secs => $3))`;

/** Best pending part: most done-neighbour yield first, skipping other devices'
 * live holds. `scope` ({state, ac}) narrows the sweep to one AC. */
export async function bestPendingPart(tag, ttlSec, scope = {}) {
  const rows = await q(
    `select p.state_cd, p.ac_no, p.part_no, p.name, p.status, p.attempts,
            coalesce((
              select sum(n.records) from old_parts n
               where n.state_cd = p.state_cd and n.ac_no = p.ac_no
                 and abs(n.part_no - p.part_no) = 1 and n.status = 'done'), 0) as nyield
       from old_parts p
      where p.status in ('pending','error')
        and ${RESV_OK}
        and ($1::text is null or p.state_cd = $1::text)
        and ($4::text is null or p.ac_no  = $4::text::int)
      order by nyield desc, p.state_cd, p.ac_no, p.part_no
      limit 1`,
    [scope.state || null, tag, ttlSec, scope.ac || null],
  );
  return rows[0] || null;
}

/**
 * Atomically claim one part.
 *
 * The `where old_parts.status <> 'running'` guard makes the claim a single
 * atomic statement: with the web app's worker processes, phones and this Node
 * process racing on one database, exactly one claimer wins and every loser
 * sees a visible skip instead of double-sweeping the part through the gateway.
 * A live reservation held by ANOTHER device also blocks it; this device's own
 * hold always passes, which is how the reservation queue is drained.
 */
export async function claimPart(workerId, tag, part, ttlSec, force = false) {
  const rows = await q(
    `insert into old_parts (state_cd, ac_no, part_no, status, started_at, attempts, claimed_by)
     values ($1, $2, $3, 'running', now(), 1, $4)
     on conflict (state_cd, ac_no, part_no) do update set
       status = 'running', started_at = now(),
       attempts = old_parts.attempts + 1, last_error = null,
       claimed_by = excluded.claimed_by,
       reserved_by = null, reserved_at = null, updated_at = now()
     where old_parts.status <> 'running'
       and ($7::boolean or old_parts.status <> 'done')
       and (old_parts.reserved_by is null
            or old_parts.reserved_by = $5
            or old_parts.reserved_at < now() - make_interval(secs => $6))
     returning state_cd, ac_no, part_no, name, status`,
    [part.state_cd, part.ac_no, part.part_no, workerId, tag, ttlSec, !!force],
  );
  return rows[0] || null;
}

/** Why a claim was refused, for the visible skip line. Runs only on a lost
 * claim, so the normal path still costs exactly one round trip. */
export async function holdReason(part, ttlSec) {
  const row = await q1(
    `select status, claimed_by, reserved_by,
            (reserved_by is not null and reserved_at is not null
             and reserved_at >= now() - make_interval(secs => $4)) as held
       from old_parts where state_cd = $1 and ac_no = $2 and part_no = $3`,
    [part.state_cd, part.ac_no, part.part_no, ttlSec],
  );
  if (!row) return 'not in catalogue';
  if (row.status === 'running') return `already running on ${row.claimed_by || 'another device'}`;
  if (row.held) return `reserved by ${row.reserved_by}`;
  return `status changed to ${row.status}`;
}

// -------------------------------------------------------- reservation queue
//
// Holding the next few parts is what stops every device re-ranking 30k pending
// rows and racing for the same top one: the picker reads only this device's
// handful of holds (partial index) and the take is a point write.

/** Top up this device's reservation queue. Only parts nobody claimed. */
export async function reserveParts(tag, n, ttlSec, scope = {}) {
  if (n <= 0) return [];
  const rows = await q(
    `with cand as (
       select p.state_cd, p.ac_no, p.part_no from old_parts p
        where p.status in ('pending','error')
          and ${RESV_OK}
          and ($1::text is null or p.state_cd = $1::text)
          and ($4::text is null or p.ac_no  = $4::text::int)
        order by p.state_cd, p.ac_no, p.part_no
        limit $5
     )
     update old_parts p set reserved_by = $2, reserved_at = now(), updated_at = now()
       from cand
      where p.state_cd = cand.state_cd and p.ac_no = cand.ac_no
        and p.part_no = cand.part_no
        and p.status in ('pending','error')
        and p.claimed_by is null
     returning p.state_cd, p.ac_no, p.part_no, p.name`,
    [scope.state || null, tag, ttlSec, scope.ac || null, n],
  );
  return rows;
}

/** How many LIVE holds this device has right now (expired ones do not count,
 * otherwise a stalled device thinks its queue is full and never tops up). */
export async function reserveCount(tag, ttlSec) {
  const row = await q1(
    `select count(*)::int as c from old_parts
      where reserved_by = $1 and status in ('pending','error')
        and reserved_at >= now() - make_interval(secs => $2)`,
    [tag, ttlSec],
  );
  return row?.c ?? 0;
}

/** Take the next part out of this device's own queue (FIFO on reserved_at, so
 * a hold is never held past its turn). Returns null when the queue is empty. */
export async function takeReserved(workerId, tag, ttlSec, force = false) {
  const rows = await q(
    `update old_parts p set
        status = 'running', started_at = now(),
        attempts = p.attempts + 1, last_error = null,
        claimed_by = $1, reserved_by = null, reserved_at = null, updated_at = now()
      where (p.state_cd, p.ac_no, p.part_no) in (
          select r.state_cd, r.ac_no, r.part_no from old_parts r
           where r.reserved_by = $2
             and (r.status in ('pending','error')${force ? ` or r.status = 'done'` : ''})
           order by r.reserved_at, r.state_cd, r.ac_no, r.part_no
           limit 1)
        and p.reserved_by = $2
      returning p.state_cd, p.ac_no, p.part_no, p.name, p.status`,
    [workerId, tag],
  );
  return rows[0] || null;
}

/** Give back every unused hold (called on a clean stop, so a stopped collector
 * does not hide parts from the fleet for the whole TTL). */
export async function releaseReservations(tag) {
  const rows = await q(
    `update old_parts set reserved_by = null, reserved_at = null, updated_at = now()
      where reserved_by = $1 and status in ('pending','error')
      returning state_cd`, [tag],
  );
  return rows.length;
}

// ------------------------------------------------------------- completion

/** Sweeping counts as progress: the reaper only looks at `started_at`, so a
 * part that outlives the staleness window must keep its claim alive. */
export async function touchPart(part) {
  await q(
    `update old_parts set updated_at = now()
      where state_cd = $1 and ac_no = $2 and part_no = $3`,
    [part.state_cd, part.ac_no, part.part_no],
  );
}

/**
 * Finish a part. Tolerant of a stale-reaper reset mid-sweep: if the row is no
 * longer 'running' under us, reclaim it and close it rather than losing the
 * collected data.
 */
export async function completePart(workerId, tag, part, { records, epics, rollEnd, unmapped, offset, curPartMode }) {
  const args = [part.state_cd, part.ac_no, part.part_no];
  const upd = (extra) => `
    update old_parts set
      status = 'done', finished_at = now(), claimed_by = null,
      reserved_by = null, reserved_at = null, last_error = null,
      records = $4, epics = $5, roll_end = $6, unmapped = $7,
      mapping_offset = $8, cur_part_mode = $9, updated_at = now()
      ${extra}
    returning state_cd, ac_no, part_no, records, epics, roll_end`;
  const vals = [...args, records, epics, rollEnd, unmapped ?? (records - epics), offset ?? null, curPartMode ?? null];

  let rows = await q(upd(`where state_cd = $1 and ac_no = $2 and part_no = $3`), vals);
  if (!rows.length) {
    // Lost the row (reaped). Re-claim, then close it out.
    await q(
      `update old_parts set status = 'running', started_at = now(), claimed_by = $4,
              reserved_by = null, reserved_at = null, updated_at = now()
        where state_cd = $1 and ac_no = $2 and part_no = $3`,
      [...args, workerId],
    );
    rows = await q(upd(`where state_cd = $1 and ac_no = $2 and part_no = $3`), vals);
  }
  return rows[0] || null;
}

/** Hand a part back as 'error' (visible to every device's retry path). */
export async function failPart(part, message) {
  await q(
    `update old_parts set status = 'error', last_error = $4, claimed_by = null,
            reserved_by = null, reserved_at = null, updated_at = now()
      where state_cd = $1 and ac_no = $2 and part_no = $3`,
    [part.state_cd, part.ac_no, part.part_no, String(message).slice(0, 1000)],
  );
}

/** Hand a part back to pending without a failure (cancel, stop). */
export async function releasePart(part, note = null) {
  await q(
    `update old_parts set status = 'pending', claimed_by = null,
            reserved_by = null, reserved_at = null,
            last_error = coalesce($4, last_error), updated_at = now()
      where state_cd = $1 and ac_no = $2 and part_no = $3 and status = 'running'`,
    [part.state_cd, part.ac_no, part.part_no, note],
  );
}

// --------------------------------------------------------------- recovery

/** Crash recovery: anything this device held mid-sweep goes back to pending. */
export async function requeueMine(workerId) {
  const rows = await q(
    `update old_parts set status = 'pending', claimed_by = null, updated_at = now()
      where claimed_by = $1 and status = 'running' returning state_cd, ac_no, part_no`,
    [workerId],
  );
  return rows;
}

/** Stale reaper: a part whose holder went silent. Uses `started_at` because
 * that is the column the Python reaper reads (30 minutes), and because
 * `old_parts` has no last_seen column. */
export async function requeueStale(min) {
  const rows = await q(
    `update old_parts set status = 'pending', claimed_by = null, updated_at = now()
      where status = 'running'
        and (started_at is null or started_at < now() - make_interval(mins => $1))
      returning state_cd, ac_no, part_no`,
    [min],
  );
  return rows;
}

// ------------------------------------------------------------------ stats

// The dashboard polls this every few seconds, so it is built from cheap queries
// over old_parts/states/acs plus cached heavy aggregates. `v_overall` is
// deliberately NOT used: it measured 117s on the live store (a count(*) and a
// count(distinct cur_epic) over electors), and it grows with the table. app.py
// made exactly this change for exactly this reason.

let heavy = { at: 0, value: { electors: null, unique_epics: null }, inflight: null };

/** Exact-but-expensive electors counters, refreshed in the background.
 * Never blocks a request: callers get the last value (null until the first
 * refresh lands) and the single-flight guard keeps one query in the air. */
export function heavyCounts() {
  const fresh = heavy.at > 0 && Date.now() - heavy.at < config.heavyTtlMs;
  if (!fresh && !heavy.inflight) {
    heavy.inflight = (async () => {
      const electors = (await q1('select count(*)::int as c from electors'))?.c ?? null;
      const uniq = (await q1(
        `select count(distinct cur_epic)::int as c from electors
          where cur_epic is not null and cur_epic <> ''`))?.c ?? null;
      heavy = { at: Date.now(), value: { electors, unique_epics: uniq }, inflight: null };
    })().catch(() => { heavy.inflight = null; });
  }
  return {
    ...heavy.value,
    heavy_age_secs: heavy.at ? Math.round((Date.now() - heavy.at) / 1000) : null,
  };
}

/** One pass over old_parts (a few thousand rows) for every part counter. */
export async function overallCounts() {
  const p = await q1(`
    select count(*)::int                                   as old_parts,
           count(*) filter (where status='done')::int      as done_parts,
           count(*) filter (where status='pending')::int   as pending_parts,
           count(*) filter (where status='running')::int   as running_parts,
           count(*) filter (where status='error')::int     as error_parts,
           coalesce(sum(records),0)::int                   as records,
           coalesce(sum(epics),0)::int                     as epics
      from old_parts`);
  const slow = heavyCounts();
  const overall = {
    ...p,
    states: (await q1('select count(*)::int as c from states'))?.c ?? 0,
    acs: (await q1('select count(*)::int as c from acs'))?.c ?? 0,
    epic_lookups: (await q1('select count(*)::int as c from epic_lookups'))?.c ?? null,
    ...slow,
  };
  // Free = unprocessed, unclaimed AND unreserved. A live hold belongs to a
  // device's next few sweeps, so it is not free either - what is left is
  // exactly the pool another device's picker may scan.
  const reserved = (await q1(
    `select count(*)::int as c from old_parts
      where reserved_by is not null and status in ('pending','error')
        and reserved_at >= now() - make_interval(secs => $1)`,
    [config.reserveTtlSec]))?.c ?? 0;
  overall.reserved_parts = reserved;
  overall.free_parts = Math.max(0, (p.pending_parts + p.error_parts) - reserved);
  // The dashboard's own labels for the same numbers.
  overall.total_parts = p.old_parts;
  overall.todo_parts = p.pending_parts + p.error_parts;
  return overall;
}

export async function summary() {
  const overall = await overallCounts();
  const devices = await q(
    `select claimed_by, count(*)::int as c from old_parts
      where status = 'running' group by 1 order by 1`,
  );
  const holds = await q(
    `select reserved_by d, count(*)::int c from old_parts
      where reserved_by is not null and status in ('pending','error')
        and reserved_at >= now() - make_interval(secs => $1)
      group by reserved_by order by 2 desc, 1`,
    [config.reserveTtlSec],
  );
  const myFree = await q1(
    `select count(*)::int as c from old_parts
      where status in ('pending','error')
        and (reserved_by is null or reserved_at < now() - make_interval(secs => $1))`,
    [config.reserveTtlSec],
  );
  return { overall, devices, holds, free: myFree?.c ?? 0 };
}
