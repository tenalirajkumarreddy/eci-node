// collection.js — the operator's part actions, ported from app.py's parts routes.
//
// The Python panel's "Parts & collection" tab did four things, each on its own
// route. This module is the Node equivalent against the same tables, so both apps
// stay one fleet:
//
//   * seed parts   — probe the gateway for an AC's real part numbers (catalog.js
//                    discoverAc, the verified path), or write part numbers the
//                    operator already knows straight into `old_parts` as pending
//                    (instant, unverified: nothing asks the gateway).
//   * collect      — put a part, or every part of an AC, back in the fleet's
//                    queue. The Python route ran the sweep in the device that
//                    clicked; here the queue IS the shared table, so the work is
//                    handed to whichever device picks it up next. That is what
//                    makes Collect work from a panel that never sweeps itself
//                    (--no-collect).
//   * part detail  — one part's row, its current-roll mapping and its relation
//                    mix, all served from the electors index for that part.
//   * breakdown    — the value domains actually present (relation / gender). An
//                    unrecognised code shows up as itself instead of vanishing,
//                    which is how "are we missing a relation type" is answered.
//
// Plus the two shared caches the panel reads: the current-roll part list
// (`current_parts`) and the EPIC lookup cache (`epic_lookups`, written by
// epic.js).
import { q, q1, logEvent, withStatementTimeout } from './db.js';
import { config } from './config.js';
import { relationLabel, genderLabel, currentPartsLive } from './api.js';
import { holdReason } from './claim.js';
import { EpicProcessor } from './epic.js';

// ------------------------------------------------------------------ seeding

/** Build the part numbers to seed: an explicit list, or the inclusive range. */
export function seedList({ from = null, to = null, parts = [] } = {}) {
  const out = new Set();
  for (const p of Array.isArray(parts) ? parts : []) {
    const n = Number(p);
    if (Number.isInteger(n) && n > 0) out.add(n);
  }
  const a = Number(from);
  if (Number.isInteger(a) && a > 0) {
    const rawTo = to === null || to === undefined || to === '' ? a : Number(to);
    if (!Number.isInteger(rawTo) || rawTo < a) throw new Error('the range must be from <= to');
    if (rawTo - a + 1 > config.seedMaxParts) {
      throw new Error(`that range is ${rawTo - a + 1} parts; one seed is capped at ${config.seedMaxParts}`);
    }
    for (let n = a; n <= rawTo; n++) out.add(n);
  }
  const nums = [...out].sort((x, y) => x - y);
  if (!nums.length) throw new Error('give part numbers, or a from/to range');
  if (nums.length > config.seedMaxParts) {
    throw new Error(`${nums.length} part numbers in one seed; the cap is ${config.seedMaxParts}`);
  }
  return nums;
}

/**
 * Write part numbers as pending rows for the fleet, WITHOUT probing.
 *
 * The instant path: nothing asks the gateway whether each part exists, so a wrong
 * number becomes a sweep that finds nothing. It never touches a row that already
 * exists (status, name and counters stay as they are) and it deliberately leaves
 * `acs.discover_status` alone — an AC is not "discovered" because an operator
 * typed a range. Every seed is logged as unverified, so the event stream says
 * where the work came from.
 */
export async function seedPartsManual({ state, ac, parts = [], from = null, to = null } = {}) {
  const stateCd = String(state || '').trim();
  const acNo = Number(ac);
  if (!stateCd) throw new Error('state is required');
  if (!Number.isInteger(acNo) || acNo <= 0) throw new Error('ac is required');
  const nums = seedList({ from, to, parts });

  const inserted = await q(
    `insert into old_parts (state_cd, ac_no, part_no, status, priority)
     select $1, $2, t.n, 'pending', 100
       from unnest($3::int[]) as t(n)
     on conflict (state_cd, ac_no, part_no) do nothing
     returning part_no`,
    [stateCd, acNo, nums],
  );
  const out = {
    state: stateCd, ac: acNo, requested: nums.length,
    inserted: inserted.length, existing: nums.length - inserted.length, verified: false,
  };
  await logEvent(config.deviceTag, 'warn',
    `seeded ${out.inserted} unverified part(s) for ${stateCd} AC ${acNo} `
    + `(${out.existing} already known) — no probe, the fleet confirms them by sweeping`, 'node');
  return out;
}

// ---------------------------------------------------------------- collection

/**
 * Put one part back in the fleet's queue.
 *
 * Two guards, both copied from the claim the workers use, because a manual queue
 * must not fight a sweep in progress:
 *   * a part another device is sweeping RIGHT NOW (`status = 'running'`) is left
 *     alone and the refusal names the holder;
 *   * a part that is already `done` is only re-opened when `force` is set, so an
 *     accidental click cannot throw away finished work.
 *
 * A part that was never discovered is created by the upsert, which is exactly what
 * a claim does when it meets an unknown part number.
 */
export async function collectPart({ state, ac, part, force = false, priority = null } = {}) {
  const stateCd = String(state || '').trim();
  const acNo = Number(ac);
  const partNo = Number(part);
  if (!stateCd) throw new Error('state is required');
  if (!Number.isInteger(acNo) || acNo <= 0) throw new Error('ac is required');
  if (!Number.isInteger(partNo) || partNo <= 0) throw new Error('part is required');
  if (priority !== null && !Number.isInteger(Number(priority))) throw new Error('priority must be an integer');

  const rows = await q(
    `insert into old_parts (state_cd, ac_no, part_no, status, priority, attempts)
     values ($1, $2, $3, 'pending', coalesce($5::int, 100), 0)
     on conflict (state_cd, ac_no, part_no) do update set
       status = 'pending',
       priority = coalesce($5::int, old_parts.priority),
       claimed_by = null, reserved_by = null, reserved_at = null,
       started_at = null, finished_at = null, last_error = null,
       updated_at = now()
     where old_parts.status <> 'running'
       and ($4::boolean or old_parts.status <> 'done')
     returning state_cd, ac_no, part_no, status, priority`,
    [stateCd, acNo, partNo, !!force, priority === null ? null : Number(priority)],
  );

  if (!rows.length) {
    return {
      queued: false,
      reason: await holdReason({ state_cd: stateCd, ac_no: acNo, part_no: partNo }, config.reserveTtlSec),
    };
  }
  await logEvent(config.deviceTag, 'info',
    `queued ${stateCd} AC ${acNo} part ${partNo} for collection`
    + (force ? ' (force: re-opening a done part)' : ''), 'node');
  return { queued: true, part: rows[0] };
}

/**
 * Queue every part of one AC (the Python tab's "Collect this AC (auto queue)").
 *
 * Without `force` this re-queues the parts that still need work (pending + error),
 * exactly like the Python pipeline did; with `force` it re-opens the done ones too.
 * The `status <> 'running'` guard is in the WHERE clause, so parts held by other
 * devices are counted as blocked instead of being yanked mid-sweep.
 */
export async function collectAc({ state, ac, force = false } = {}) {
  const stateCd = String(state || '').trim();
  const acNo = Number(ac);
  if (!stateCd) throw new Error('state is required');
  if (!Number.isInteger(acNo) || acNo <= 0) throw new Error('ac is required');

  const before = await q1(
    `select count(*)::int as parts,
            count(*) filter (where status = 'running')::int as running,
            count(*) filter (where status = 'done')::int as done,
            count(*) filter (where status in ('pending','error'))::int as waiting
       from old_parts where state_cd = $1 and ac_no = $2`, [stateCd, acNo]);

  const rows = await q(
    `update old_parts
        set status = 'pending', claimed_by = null,
            reserved_by = null, reserved_at = null,
            started_at = null, finished_at = null, last_error = null,
            updated_at = now()
      where state_cd = $1 and ac_no = $2
        and status <> 'running'
        and ($3::boolean or status <> 'done')
      returning part_no`,
    [stateCd, acNo, !!force],
  );

  const out = {
    state: stateCd, ac: acNo, queued: rows.length, force: !!force,
    parts_in_ac: before.parts, held_running: before.running,
    done_skipped: force ? 0 : before.done,
  };
  if (!before.parts) out.hint = 'no parts are known for this AC yet — seed parts first';
  await logEvent(config.deviceTag, 'info',
    `queued ${out.queued} part(s) of ${stateCd} AC ${acNo} for collection`
    + `${force ? ' (force)' : ''}${out.held_running ? `, ${out.held_running} left running on another device` : ''}`,
    'node');
  return out;
}

// --------------------------------------------------------------- part detail

/**
 * One part: its row (with the current-roll name of the part it maps to), where its
 * people ended up on the current roll, and the relation mix of its rows.
 *
 * Every query is scoped to a single part, which is what the
 * `electors_old_idx (state_cd, ac_no, part_no, serial_no)` index serves — a part
 * holds ~1.5k rows, so this stays a point lookup instead of the per-part
 * count(*) fan-out that made the Python parts page slow.
 */
export async function partDetail({ state, ac, part } = {}) {
  const stateCd = String(state || '').trim();
  const acNo = Number(ac);
  const partNo = Number(part);
  if (!stateCd) throw new Error('state is required');
  if (!Number.isInteger(acNo) || acNo <= 0) throw new Error('ac is required');
  if (!Number.isInteger(partNo) || partNo <= 0) throw new Error('part is required');

  const row = await q1(
    `select p.*, p.mapping_offset as "offset",
            cp.part_name as cur_part_name, cp.part_name_l1 as cur_part_name_l1
       from old_parts p
       left join current_parts cp
         on cp.state_cd = p.state_cd and cp.ac_no = p.ac_no and cp.part_no = p.cur_part_mode
      where p.state_cd = $1 and p.ac_no = $2 and p.part_no = $3`,
    [stateCd, acNo, partNo],
  );
  if (!row) return null;

  const scope = [stateCd, acNo, partNo];
  const [byCur, byRel, sample] = await Promise.all([
    q(`select cur_state_cd, cur_ac_no, cur_part_no,
              count(*)::int as n, count(distinct cur_epic)::int as epics
         from electors
        where state_cd = $1 and ac_no = $2 and part_no = $3
        group by 1, 2, 3 order by n desc limit 50`, scope),
    q(`select relation_type, count(*)::int as n,
              count(*) filter (where cur_epic is not null and cur_epic <> '')::int as mapped
         from electors
        where state_cd = $1 and ac_no = $2 and part_no = $3
        group by 1 order by n desc`, scope),
    q(`select serial_no, full_name, full_name_l1, relative_name, relative_name_l1,
              relation_type, gender, age_snapshot, epic_2003, cur_epic,
              cur_ac_no, cur_part_no, marked_by_blo
         from electors
        where state_cd = $1 and ac_no = $2 and part_no = $3
        order by serial_no limit 30`, scope),
  ]);

  return {
    part: row,
    by_current_part: byCur,
    by_relation: byRel.map((r) => ({ ...r, label: relationLabel(r.relation_type) })),
    sample: sample.map((r) => ({
      ...r,
      relation_label: relationLabel(r.relation_type),
      gender_label: genderLabel(r.gender),
    })),
  };
}

// ---------------------------------------------------------------- breakdown

// Memoised per (state, AC) for the same reason the Python route cached it: these
// are aggregates over `electors`, and a page polling every few seconds must not
// re-run them each time. One process, one value, with its age reported.
const BD = new Map();

/** Drop the memo (tests, or after a scope's data changed a lot). */
export function clearBreakdown() {
  BD.clear();
}

/**
 * Relation types and genders actually present, with their decoded labels.
 *
 * `state`/`ac` narrow it to something the electors index can serve; without them
 * this is a scan of the whole table, so it runs on one connection with a hard
 * statement timeout and a failure is reported (and never cached) instead of being
 * allowed to run for minutes like the old unbounded counters.
 */
export async function breakdown({ state = null, ac = null } = {}) {
  const stateCd = state ? String(state).trim() : null;
  const acNo = ac === null || ac === undefined || ac === '' ? null : Number(ac);
  if (acNo !== null && !Number.isInteger(acNo)) throw new Error('ac must be an integer');

  const key = `${stateCd || '*'}|${acNo === null ? '*' : acNo}`;
  const ageOf = (at) => Math.max(0, Math.round((Date.now() - at) / 1000));
  const ttlSecs = Math.round(config.breakdownTtlMs / 1000);
  const hit = BD.get(key);
  if (hit && Date.now() - hit.at < config.breakdownTtlMs) {
    return { ...hit.value, cached_secs: ageOf(hit.at), ttl_secs: ttlSecs };
  }

  const params = [stateCd, acNo];
  const where = `($1::text is null or state_cd = $1) and ($2::int is null or ac_no = $2)`;
  try {
    const value = await withStatementTimeout(config.breakdownTimeoutMs, async (client) => {
      const rel = await client.query(
        `select relation_type as code, count(*)::int as n,
                count(*) filter (where cur_epic is not null and cur_epic <> '')::int as mapped
           from electors where ${where} group by 1 order by n desc`, params);
      const gen = await client.query(
        `select gender as code, count(*)::int as n,
                count(*) filter (where cur_epic is not null and cur_epic <> '')::int as mapped
           from electors where ${where} group by 1 order by n desc`, params);
      const cov = await client.query(
        `select count(*)::int as total,
                count(*) filter (where cur_epic is not null and cur_epic <> '')::int as with_epic,
                count(*) filter (where relative_name is null or relative_name = '')::int as no_relative,
                count(*) filter (where full_name is null or full_name = '')::int as no_name
           from electors where ${where}`, params);
      return {
        relation: rel.rows.map((r) => ({ ...r, label: relationLabel(r.code), short: relationLabel(r.code, true) })),
        gender: gen.rows.map((r) => ({ ...r, label: genderLabel(r.code) })),
        coverage: cov.rows[0] ?? {},
        scope: { state: stateCd, ac: acNo },
      };
    });
    BD.set(key, { at: Date.now(), value });
    return { ...value, cached_secs: 0, ttl_secs: ttlSecs };
  } catch (e) {
    // Serve the previous value rather than an empty table, and say plainly that
    // this is what is on screen: an unbounded aggregate on a live store is exactly
    // the failure mode stats.js exists to prevent.
    return {
      ...(hit ? hit.value : { relation: [], gender: [], coverage: {}, scope: { state: stateCd, ac: acNo } }),
      cached_secs: hit ? ageOf(hit.at) : null,
      ttl_secs: ttlSecs,
      error: String(e?.message || e),
    };
  }
}

// ------------------------------------------------------------- current parts

/**
 * Current-roll part names/ids for one AC, cached in the DB.
 *
 * `refresh` asks the live gateway (api.js currentPartsLive) and upserts what it
 * answers; without it this only reads the cache, so opening the parts page never
 * fires an upstream request. An empty live answer leaves the cache untouched -
 * "no answer" must not wipe names we already have.
 */
export async function currentParts({ state, ac, refresh = false } = {}) {
  const stateCd = String(state || '').trim();
  const acNo = Number(ac);
  if (!stateCd) throw new Error('state is required');
  if (!Number.isInteger(acNo) || acNo <= 0) throw new Error('ac is required');

  let fetched = 0;
  let liveError = null;
  const cached = await q1(
    'select count(*)::int as c from current_parts where state_cd = $1 and ac_no = $2',
    [stateCd, acNo]);

  if (refresh || !cached.c) {
    try {
      const rows = await currentPartsLive(stateCd, acNo);
      fetched = rows.length;
      if (rows.length) {
        await q(
          `insert into current_parts (state_cd, ac_no, part_no, part_name, part_name_l1,
                                      part_id, district_cd, ps_type, ps_caty, old_pdf_url, fetched_at)
           select $1, $2, t.n, t.nm, t.nm1, t.pid, t.dist, t.pt, t.pc, t.url, now()
             from unnest($3::int[], $4::text[], $5::text[], $6::bigint[], $7::text[],
                         $8::text[], $9::text[], $10::text[])
                  as t(n, nm, nm1, pid, dist, pt, pc, url)
           on conflict (state_cd, ac_no, part_no) do update
             set part_name = excluded.part_name,
                 part_name_l1 = excluded.part_name_l1,
                 part_id = excluded.part_id,
                 district_cd = excluded.district_cd,
                 ps_type = excluded.ps_type,
                 ps_caty = excluded.ps_caty,
                 old_pdf_url = excluded.old_pdf_url,
                 fetched_at = now()`,
          [
            stateCd, acNo,
            rows.map((r) => Number(r.part_no)),
            rows.map((r) => r.part_name ?? null),
            rows.map((r) => r.part_name_l1 ?? null),
            rows.map((r) => (r.part_id === null || r.part_id === undefined ? null : Number(r.part_id))),
            rows.map((r) => r.district_cd ?? null),
            rows.map((r) => r.ps_type ?? null),
            rows.map((r) => r.ps_caty ?? null),
            rows.map((r) => r.old_pdf_url ?? null),
          ],
        );
        await logEvent(config.deviceTag, 'info',
          `current-roll parts for ${stateCd} AC ${acNo}: ${rows.length} row(s) cached from the gateway`, 'node');
      }
    } catch (e) {
      liveError = String(e?.message || e);
      await logEvent(config.deviceTag, 'warn',
        `current-roll parts for ${stateCd} AC ${acNo} failed: ${liveError}`, 'node');
    }
  }

  const rows = await q(
    `select * from current_parts where state_cd = $1 and ac_no = $2 order by part_no`,
    [stateCd, acNo]);
  return { rows, fetched, cached: cached.c, refreshed: !!refresh, error: liveError };
}

// ---------------------------------------------------------------------- jobs
//
// The shared `jobs` table is the Python/Android clients' queue. This panel lists
// and cancels those jobs so one place answers "what is the fleet working on" -
// the Node worker itself sweeps parts directly (claim.js) and does not need a job
// row to do it.

export async function listJobs({ status = null, limit = 50 } = {}) {
  const n = Math.min(Math.max(1, Number(limit) || 50), 500);
  if (status) {
    return q('select * from jobs where status = $1 order by id desc limit $2', [String(status), n]);
  }
  return q('select * from jobs order by id desc limit $1', [n]);
}

/**
 * Cancel a queued or running job.
 *
 * Same two statements as the Python route: a queued job has no runner to notice
 * the flag, so it is closed out here directly; a running one is flagged and its
 * runner stops at the next checkpoint. The close-out only matches `queued`, so it
 * cannot race a claim that just flipped the row to `running`.
 */
export async function cancelJob(jobId) {
  const id = Number(jobId);
  if (!Number.isInteger(id) || id <= 0) throw new Error('job id is required');
  const flagged = await q(
    `update jobs set cancel = true where id = $1 and status in ('queued','running') returning id`, [id]);
  await q(
    `update jobs set status = 'cancelled', finished_at = now() where id = $1 and status = 'queued'`, [id]);
  const row = await q1('select id, kind, status, cancel from jobs where id = $1', [id]);
  await logEvent(config.deviceTag, 'api', `cancel requested for job #${id}`, 'node');
  return { cancelled: flagged.length > 0, job: row };
}

// ---------------------------------------------------------------------- EPIC
//
// epic.js has been able to do this since the port; nothing ever called it from a
// route. Lookups are serialised inside EpicProcessor (the national display answers
// at ~1 req/s), and the result lands in the shared epic_lookups cache the Python
// routes read.

let processor = null;

export async function epicLookup(epic, { refresh = false } = {}) {
  const code = String(epic || '').split(/\s+/).join('').toUpperCase();
  if (!code) throw new Error('epic is required');
  if (!refresh) {
    const cached = await q1('select * from epic_lookups where epic = $1', [code]);
    if (cached) return { cached: true, row: cached };
  }
  if (!processor) processor = new EpicProcessor(config);
  const result = await processor.lookup(code);
  const row = await q1('select * from epic_lookups where epic = $1', [code]);
  return {
    cached: false,
    row,
    // The raw response and the decrypted content are large and partly personal
    // data; the panel shows the mapped profile, the cache keeps the detail.
    result: { epic: result.epic, status: result.status, hits: result.hits, profile: result.profile },
  };
}

export async function recentEpics({ limit = 50 } = {}) {
  const n = Math.min(Math.max(1, Number(limit) || 50), 500);
  return q(
    `select epic, found, http_status, name, part_no, part_name, ac_name, serial_no, fetched_at
       from epic_lookups order by fetched_at desc limit $1`, [n]);
}

export async function getEpic(epic) {
  const code = String(epic || '').split(/\s+/).join('').toUpperCase();
  if (!code) throw new Error('epic is required');
  return q1('select * from epic_lookups where epic = $1', [code]);
}
