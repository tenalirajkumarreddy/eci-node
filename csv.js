// csv.js — the electors export used by both the CLI (--export) and the dashboard.
//
// Column set and ordering are ported from app.py's /api/export.csv so a file
// pulled from the Node dashboard is byte-compatible with one from the Python
// dashboard (including the UTF-8 BOM, which is what makes Excel open the
// Devanagari name columns correctly).
import { q } from './db.js';
import { relationLabel, genderLabel } from './api.js';

export const HEADER = [
  'epic', 'name', 'name_local', 'relation_type', 'relation', 'relation_local',
  'gender', 'age_2003', 'epic_2003', 'old_serial', 'old_part', 'cur_ac', 'cur_part',
];

function csvCell(v) {
  if (v === null || v === undefined) return '';
  const s = String(v);
  return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

export function toCsvRow(values) {
  return values.map(csvCell).join(',') + '\r\n';
}

/**
 * Stream `electors` as CSV into any writable stream.
 * `{state, ac}` are optional filters; like the Python route the export is
 * EPIC-only (rows whose current mapping has an EPIC), so the file is a list of
 * people you can actually search for.
 */
export async function writeElectorsCsv(stream, { state = null, ac = null } = {}) {
  const rows = await q(
    `select cur_epic, full_name, full_name_l1, relative_name, relative_name_l1,
            relation_type, gender, age_snapshot, epic_2003, serial_no, part_no,
            cur_ac_no, cur_part_no
       from electors
      where ($1::text is null or state_cd = $1)
        and ($2::int  is null or ac_no = $2)
        and cur_epic is not null and cur_epic <> ''
      order by cur_ac_no, cur_part_no, cur_epic`,
    [state, ac === null || ac === '' ? null : Number(ac)],
  );

  // BOM first: without it Excel renders the Devanagari columns as mojibake.
  // A closed stream (the client cancelled) stops the export quietly instead of
  // throwing into a socket nobody is listening on.
  if (!(await write(stream, '\ufeff' + toCsvRow(HEADER)))) return 0;
  let written = 0;
  for (const r of rows) {
    const ok = await write(stream, toCsvRow([
      r.cur_epic, r.full_name, r.full_name_l1, relationLabel(r.relation_type),
      r.relative_name, r.relative_name_l1, genderLabel(r.gender), r.age_snapshot,
      r.epic_2003, r.serial_no, r.part_no, r.cur_ac_no, r.cur_part_no,
    ]));
    if (!ok) break;
    written += 1;
  }
  if (!stream.destroyed && !stream.writableEnded) stream.end();
  return written;
}

/** Respect backpressure (a 200k-row export must not buffer in memory), and
 * report `false` once the stream is gone so the caller can stop.
 *
 * The listeners are attached per chunk and removed when it settles: a `once`
 * that never fires would otherwise accumulate two listeners per row and trip
 * MaxListeners on a real export. */
function write(stream, chunk) {
  if (stream.destroyed || stream.writableEnded) return Promise.resolve(false);
  return new Promise((resolve) => {
    let settled = false;
    const cleanup = () => {
      stream.removeListener('error', onGone);
      stream.removeListener('close', onGone);
      stream.removeListener('drain', onDrain);
    };
    const finish = (v) => { if (settled) return; settled = true; cleanup(); resolve(v); };
    const onGone = () => finish(false);
    const onDrain = () => finish(true);
    stream.on('error', onGone);
    stream.on('close', onGone);
    let ok;
    try { ok = stream.write(chunk); } catch { finish(false); return; }
    if (ok) finish(true);
    else stream.on('drain', onDrain);
  });
}
