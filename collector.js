// collector.js — sweep one part: measure the roll end, pull every serial,
// upsert the electors into the shared table.
//
// Ported from worker.py's _collect_part_impl, including the two hard-won
// details:
//
//  1. The roll end is MEASURED, never spotted. An earlier Python version
//     guessed by stepping candidates (50, 100, ... 2000) and stopped when a
//     candidate missed while it was >120 past the last hit - so any roll with a
//     gap wider than 120 was declared absent and the part was marked done
//     forever. Measured on S01 AC 1 part 1: the roll really holds 960 serials
//     but was recorded roll_end=840, dropping 120 electors. So: gallop upward
//     until a serial misses, then bisect the boundary between the last hit and
//     the first miss.
//
//  2. A sweep that "succeeds" with 0 records WHILE fetch errors happened is
//     handed back as `error`, not `done` - rate-limit storms answer 200 with
//     no parsable rows, and 63 parts in the live DB were lost that way.
//
// Everything the old-roll route returns lands in `electors` using the same
// columns and the same conflict key the Python fleet writes, so the two
// collectors fill one table rather than two.
import { setTimeout as sleep } from 'node:timers/promises';
import { q } from './db.js';
import { config } from './config.js';

const int = (v) => {
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : null;
};

/** Best-effort readers over the gateway's 2003-vintage field names. The exact
 * names below are the ones measured live (`oldFullName`, `epicNumber`,
 * `bloMappedEpicNo`, ...); the extras are kept as fallbacks only. */
const pick = (o, keys) => {
  for (const k of keys) {
    const v = o?.[k];
    if (v !== undefined && v !== null && v !== '') return v;
  }
  return null;
};

export function extractRows(body) {
  if (Array.isArray(body)) return body;
  for (const k of ['payload', 'data', 'records', 'rows', 'result', 'response', 'content']) {
    const v = body?.[k];
    if (Array.isArray(v)) return v;
    if (Array.isArray(v?.payload)) return v.payload;
    if (Array.isArray(v?.data)) return v.data;
  }
  return [];
}

/** One API record -> one electors row (same columns worker.py writes). */
export function mapRecord(raw, stateCd, acNo, partNo, serialFallback) {
  return [
    raw.id ?? `${stateCd}:${acNo}:${partNo}:${serialFallback}`,   // source_id
    stateCd, acNo, partNo,
    int(pick(raw, ['oldPartSerialNo', 'partSerialNumber', 'serialNo']) ?? serialFallback),
    pick(raw, ['oldFullName', 'firstName', 'fullName']),
    pick(raw, ['oldFullNameL1', 'firstNameL1']),
    pick(raw, ['oldRelativeFullName', 'relativeFName', 'relativeFullName']),
    pick(raw, ['oldRelativeFullNameL1', 'relativeFNameL1']),
    pick(raw, ['relationType']),
    pick(raw, ['gender']),
    int(pick(raw, ['age'])),
    pick(raw, ['epicNumber']),                 // the 2003-roll EPIC
    pick(raw, ['markedByBlo']),
    pick(raw, ['bloMappedStateCd']),           // current mapping
    int(pick(raw, ['bloMappedAcNo'])),
    int(pick(raw, ['bloMappedPartNo'])),
    pick(raw, ['bloMappedEpicNo']),            // the current EPIC
  ];
}

const ELECTOR_UPSERT = `
insert into electors(source_id, state_cd, ac_no, part_no, serial_no,
        full_name, full_name_l1, relative_name, relative_name_l1, relation_type,
        gender, age_snapshot, epic_2003, marked_by_blo,
        cur_state_cd, cur_ac_no, cur_part_no, cur_epic, last_seen)
values %VALUES%
on conflict (source_id) do update set
  serial_no = excluded.serial_no, full_name = excluded.full_name,
  full_name_l1 = excluded.full_name_l1, relative_name = excluded.relative_name,
  relative_name_l1 = excluded.relative_name_l1, relation_type = excluded.relation_type,
  gender = excluded.gender, age_snapshot = excluded.age_snapshot,
  epic_2003 = excluded.epic_2003, marked_by_blo = excluded.marked_by_blo,
  cur_state_cd = excluded.cur_state_cd, cur_ac_no = excluded.cur_ac_no,
  cur_part_no = excluded.cur_part_no, cur_epic = excluded.cur_epic,
  last_seen = now()`;

// Live write-path state, read by the dashboard so "is everything uploaded?"
// has an answer: `buffered` is fetched-but-not-yet-written, and `flushed` only
// counts rows a successful upsert has actually committed. If a flush fails, its
// rows go back into the batch (never dropped) and the sweep aborts with the part
// handed back to `pending` instead of `done`, so the same part is re-fetched.
export const writeState = {
  buffered: 0,
  flushed: 0,
  flushes: 0,
  flush_errors: 0,
  inflight: 0,
  last_flush_at: null,
  last_flush_ms: null,
};

let flushChain = Promise.resolve();

/** Write a batch (rows are positional tuples built by mapRecord). */
function flushBatch(batch) {
  const items = batch.splice(0);                       // snapshot+drain, race-safe
  if (!items.length) return flushChain;
  const ph = [];
  const vals = [];
  items.forEach((row, i) => {
    const b = i * 18;
    ph.push(`(${Array.from({ length: 18 }, (_, k) => `$${b + k + 1}`).join(',')}, now())`);
    vals.push(...row);
  });
  const text = ELECTOR_UPSERT.replace('%VALUES%', ph.join(','));
  const t0 = Date.now();
  writeState.buffered = batch.length;
  writeState.inflight += 1;
  flushChain = flushChain
    .then(() => q(text, vals))
    .then(() => {
      writeState.flushed += items.length;
      writeState.flushes += 1;
      writeState.last_flush_at = new Date().toISOString();
      writeState.last_flush_ms = Date.now() - t0;
    })
    .catch((e) => {
      // Never drop a row: put them back for the retry and let the caller abort
      // the part (which returns it to `pending`, so it gets swept again).
      writeState.flush_errors += 1;
      batch.unshift(...items);
      throw e;
    })
    .finally(() => {
      writeState.inflight -= 1;
      writeState.buffered = batch.length;
    });
  return flushChain;
}

/**
 * Highest serial that answers, +20 margin (30 when the part is empty).
 * Gallop up from 1 until a miss, then bisect. Costs ~13 requests where a
 * blind stepping probe cost more AND returned a wrong answer.
 */
export async function probeRollEnd(client, part, hardCap = config.serialCap) {
  const answers = async (serial) => {
    try {
      const rows = await client.getEroll(part.state_cd, part.ac_no, part.part_no, String(serial));
      return rows.length > 0;
    } catch (e) {
      if (e.status === 404) return false;
      throw e;
    }
  };

  // Serial 1 is the only serial always probed, so an empty part costs exactly
  // one request (the old roll can start at 1 but several parts are empty).
  if (!(await answers(1))) return 30;

  let last = 1;               // highest serial PROVEN to answer
  let firstMiss = null;
  for (let step = 2; ; step = Math.min(step * 2, hardCap)) {
    const cand = Math.min(hardCap, last + step);
    if (cand === last) break;                   // reached the cap: roll is that long
    if (await answers(cand)) last = cand;
    else { firstMiss = cand; break; }
  }

  // Bisect the boundary between the last hit and the first miss.
  if (firstMiss !== null) {
    let lo = last, hi = firstMiss;
    while (hi - lo > 1) {
      const mid = lo + Math.floor((hi - lo) / 2);
      if (await answers(mid)) lo = mid; else hi = mid;
    }
  }
  // +20 margin (the route answers a little past the printed roll; a measured
  // end is the true end + 20). Never below the cap.
  return Math.min(hardCap, last + 20);
}

export function metaFrom(raw) {
  return {
    old_state_name: raw.oldStateName ?? null,
    old_dist_no: raw.oldDistNo ? String(raw.oldDistNo) : null,
    old_dist_name: raw.oldDistName ?? null,
    old_ac_name: raw.oldAcName ?? null,
  };
}

/**
 * Sweep one part. Concurrency is shaped by AIMD alone: serial pullers just
 * fire and the client's acquire() throttles them to the discovered ceiling.
 * Returns {records, epics, rollEnd, errors, status, lastError, t0}.
 */
export async function sweepPart(client, part, cfg, fail) {
  const t0 = Date.now();
  const batch = [];
  const seen = new Set();
  let records = 0, epics = 0, errors = 0, hits = 0, misses = 0;
  let meta = null;

  const addRows = (body, serial) => {
    for (const raw of extractRows(body)) {
      const id = raw?.id;
      if (id !== undefined && seen.has(id)) continue;   // the route repeats rows across serials
      if (id !== undefined) seen.add(id);
      if (!meta) meta = metaFrom(raw);
      batch.push(mapRecord(raw, part.state_cd, part.ac_no, part.part_no, serial));
      writeState.buffered = batch.length;
      records += 1;
      if (raw?.bloMappedEpicNo) epics += 1;
    }
  };

  // ---- 1. discovery window + measured roll end
  //
  // The window (~50 records at a random offset) is kept: those rows are real
  // electors, they carry the part's old-location descriptor, and `seen`
  // deduplicates them against the serial sweep below - one `seen` set for the
  // whole part, exactly like worker.py's seen_ids.
  let rollEnd;
  try {
    const win = await client.getEroll(part.state_cd, part.ac_no, part.part_no, '');
    addRows(win, 0);
    rollEnd = await probeRollEnd(client, part, cfg.serialCap);
  } catch (e) {
    await fail(`probe: ${e.message}`);
    throw e;
  }

  // Publish roll_end before sweeping: it is the denominator the dashboard uses
  // for live speed and ETA, and last_serial is only written every 200 serials,
  // so without this the live card is blank for the first seconds of a part.
  await q(
    `update old_parts set roll_end = $4, old_state_name = coalesce($5, old_state_name),
            old_dist_no = coalesce($6, old_dist_no), old_dist_name = coalesce($7, old_dist_name),
            old_ac_name = coalesce($8, old_ac_name), updated_at = now()
      where state_cd = $1 and ac_no = $2 and part_no = $3`,
    [part.state_cd, part.ac_no, part.part_no, rollEnd,
     meta?.old_state_name ?? null, meta?.old_dist_no ?? null,
     meta?.old_dist_name ?? null, meta?.old_ac_name ?? null],
  );

  // ---- 2. sweep 1..rollEnd (+ a small tail, so a low measurement cannot truncate)
  const sweepTo = Math.min(cfg.serialCap, rollEnd + cfg.serialStopAfter);
  const width = Math.max(4, Math.min(64, cfg.startInflight * 2));
  let next = 1, consecutiveMiss = 0, stop = false;

  async function puller() {
    while (!stop) {
      const serial = next++;
      if (serial > sweepTo) return;
      try {
        const body = await client.getEroll(part.state_cd, part.ac_no, part.part_no, String(serial));
        hits += 1;
        consecutiveMiss = 0;
        addRows(body, serial);
      } catch (e) {
        if (e.status === 404) {
          misses += 1;
          consecutiveMiss += 1;
          // Past the margin: the measurement was optimistic, stop rather than
          // burn requests to the hard cap.
          if (consecutiveMiss > cfg.serialStopAfter * 2) { stop = true; return; }
          continue;
        }
        errors += 1;
        if (errors > 25) { stop = true; return; }        // transport trouble
        await sleep(1000);
        continue;
      }
      if (batch.length >= cfg.batchSize) await flushBatch(batch);
      // Every 50 requests, not 200: this row is what the dashboard's progress bar
      // and req/s read, and at ~50 req/s a 200-request heartbeat left a freshly
      // claimed part showing 0% for the first several seconds. A point update on
      // the primary key at ~1/s is nothing next to the writes already happening.
      if ((hits + misses) % 50 === 0) {
        await q(
          `update old_parts set last_serial = $4, records = $5, epics = $6,
                  old_state_name = coalesce($7, old_state_name),
                  old_dist_no = coalesce($8, old_dist_no),
                  old_dist_name = coalesce($9, old_dist_name),
                  old_ac_name = coalesce($10, old_ac_name),
                  exists_ = true, updated_at = now()
            where state_cd = $1 and ac_no = $2 and part_no = $3`,            [part.state_cd, part.ac_no, part.part_no, serial, records, epics,
             meta?.old_state_name ?? null, meta?.old_dist_no ?? null,
             meta?.old_dist_name ?? null, meta?.old_ac_name ?? null],
        );
      }
    }
  }

  await Promise.all(Array.from({ length: width }, puller));
  await flushBatch(batch);

  // ---- 3. verdict, with the 0-records-but-errors fingerprint kept as an error
  let status = stop && errors > 25 ? 'error' : 'done';
  let lastError = null;
  if (records === 0 && errors > 0) {
    status = 'error';
    lastError = `suspicious: 0 records with ${errors} fetch errors`;
  }
  return { records, epics, rollEnd, errors, misses, hits, status, lastError, meta, t0 };
}
