import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { q, q1, logEvent, dbStatus, dbHealth } from './db.js';
import { writeState } from './collector.js';
import { config, ROOT } from './config.js';
import { setSetting, getSetting, deviceSettings, summary, reserveCount } from './claim.js';
import { statsSummary } from './stats.js';
import {
  seedStates, seedAcs, discoverNext, discoverAc, jobStatus, acsBacklog, retryErroredAcs, runJob,
} from './catalog.js';
import { writeElectorsCsv } from './csv.js';
import {
  seedPartsManual, collectPart, collectAc, partDetail, breakdown, currentParts,
  listJobs, cancelJob, epicLookup, recentEpics, getEpic,
} from './collection.js';
import { startSweep, stopSweep, sweepStatus } from './sweep.js';

/** Tiny TTL memo. The admin page polls every few seconds; the aggregates behind
 * it (events over 24h, pg_stat_activity) are not free on a box that four
 * collectors are writing to, so each is computed at most once per TTL per
 * process. */
function memo(ttlMs) {
  let at = 0;
  let value = null;
  let inflight = null;
  const get = async (fn) => {
    if (value && Date.now() - at < ttlMs) return { value, cached: true };
    if (!inflight) {
      inflight = (async () => {
        try { value = await fn(); at = Date.now(); } finally { inflight = null; }
      })();
    }
    await inflight.catch(() => {});
    return { value, cached: true };
  };
  return get;
}

const json = (res, code, obj) => { res.writeHead(code, { 'content-type': 'application/json' }); res.end(JSON.stringify(obj)); };
const readBody = (req) => new Promise((res, rej) => {
  let d = ''; req.on('data', (c) => { d += c; if (d.length > 1e6) req.destroy(); });
  req.on('end', () => { try { res(d ? JSON.parse(d) : {}); } catch (e) { rej(e); } });
});

/** Live progress of the part this device is sweeping: how far through the roll
 * it is, how fast, and how long is left. `last_serial` is written every 200
 * serials while sweeping, so this is a real instantaneous rate rather than an
 * average of finished work. */
async function currentProgress(worker) {
  const c = worker?.current;
  if (!c) return null;
  const row = await q1(
    `select state_cd, ac_no, part_no, name, status, started_at, last_serial,
            roll_end, records, epics, attempts, last_error
       from old_parts where state_cd = $1 and ac_no = $2 and part_no = $3`,
    [c.state_cd, c.ac_no, c.part_no],
  );
  if (!row) return null;
  const started = row.started_at ? new Date(row.started_at).getTime() : null;
  const elapsed = started ? Math.max(0.001, (Date.now() - started) / 1000) : null;
  const done = row.last_serial ?? 0;
  const total = row.roll_end ?? null;
  const reqPerSec = elapsed && done ? Number((done / elapsed).toFixed(1)) : null;
  // Rows this part currently has in the shared table. Compared against the
  // sweep's own `records` counter it answers "did everything get uploaded?":
  // they match unless a neighbouring part re-attributed one of the same people
  // (an upsert on source_id moves the row, it never duplicates it).
  const stored = await q1(
    `select count(*)::int as c from electors
      where state_cd = $1 and ac_no = $2 and part_no = $3`,
    [row.state_cd, row.ac_no, row.part_no],
  );
  return {
    ...row,
    stored_rows: stored?.c ?? null,
    pct: total ? Math.min(100, Math.round((100 * done) / total)) : null,
    elapsed_s: elapsed ? Math.round(elapsed) : null,
    req_per_sec: reqPerSec,
    rec_per_sec: elapsed && row.records ? Number((row.records / elapsed).toFixed(1)) : null,
    eta_secs: reqPerSec && total ? Math.max(0, Math.round((total - done) / reqPerSec)) : null,
  };
}

// --------------------------------------------------------------- fleet view
//
// Built from the devices' own heartbeats (settings rows), the fleet's part
// counters and the 24h event log. Memoised per process, because the event
// aggregate is ~2s on the live store and the page polls every 5s.

const ALIVE_SECS = 120;

async function fleetSnapshot() {
  const [beats, queueRows, perDevice, backlog] = await Promise.all([
    q(`select key, value, updated_at from settings
        where key like 'worker_heartbeat@%' order by key`),
    q(`select status, count(*)::int as n,
              count(distinct claimed_by)::int as holders,
              count(*) filter (where reserved_by is not null
                and reserved_at >= now() - make_interval(secs => $1))::int as reserved,
              max(updated_at) as latest
         from old_parts group by 1`, [config.reserveTtlSec]),
    q(`select device,
              count(*) filter (where message like 'done %')::int as parts_24h,
              count(*) filter (where ts > now() - interval '1 hour'
                                 and message like 'done %')::int as parts_1h,
              max(ts) as last_seen
         from events where ts > now() - interval '24 hours'
        group by 1`),
    acsBacklog(),
  ]);

  const evByTag = new Map(perDevice.map((r) => [r.device, r]));
  const now = Date.now();
  const devices = [];
  const seen = new Set();

  for (const row of beats) {
    const v = row.value && typeof row.value === 'object' ? row.value : {};
    const tag = v.tag || String(row.key).replace(/^worker_heartbeat@/, '');
    seen.add(tag);
    const ev = evByTag.get(tag) || {};
    const iso = v.iso || (Number.isFinite(v.at) ? new Date(v.at * 1000).toISOString() : null);
    const age = iso ? Math.max(0, Math.round((now - Date.parse(iso)) / 1000)) : null;
    const aimd = v.aimd || {};
    devices.push({
      tag,
      worker_id: v.worker_id || null,
      engine: v.source === 'node' ? 'node'
        : (v.source ? String(v.source) : (v.pid || /^pc-/.test(v.worker_id || '') ? 'python/android' : 'unknown')),
      heartbeat_age_secs: age,
      last_seen: iso,
      events_last_seen: ev.last_seen || null,
      running: v.running === true,
      paused: v.paused === true || v.stopped === true,
      parts_done_run: v.parts_done ?? null,
      parts_24h: ev.parts_24h ?? null,
      parts_1h: ev.parts_1h ?? null,
      current: v.current || null,
      aimd: v.aimd ? {
        limit: aimd.limit ?? null, inflight: aimd.inflight ?? null, peak: aimd.peak ?? null,
        reqs: aimd.reqs ?? null, err_reqs: aimd.errReqs ?? null,
        by_status: aimd.byStatus || {}, err_by_status: aimd.errByStatus || {},
        retry_afters: aimd.retryAfters ?? null,
        req_per_sec: aimd.recentReqPerSec ?? null,
        window_avg_req_per_sec: aimd.windowAvgReqsPerSec ?? null,
      } : null,
      egress_ip: v.egress_ip || null,
      egress_at: v.egress_at || null,
    });
  }

  // Devices that log but keep no heartbeat (the Python/Android clients): still
  // visible, so "who is working" is never blank just because a device is older.
  for (const ev of perDevice) {
    if (!ev.device || seen.has(ev.device)) continue;
    devices.push({
      tag: ev.device, worker_id: null, engine: 'no heartbeat', heartbeat_age_secs: null,
      last_seen: null, events_last_seen: ev.last_seen, running: null, paused: null,
      parts_done_run: null, parts_24h: ev.parts_24h ?? null, parts_1h: ev.parts_1h ?? null,
      current: null, aimd: null, egress_ip: null, egress_at: null,
    });
  }

  for (const d of devices) {
    d.state = d.heartbeat_age_secs != null && d.heartbeat_age_secs <= ALIVE_SECS
      ? (d.running ? 'running' : 'paused')
      : ((d.events_last_seen && (now - Date.parse(d.events_last_seen)) < 10 * 60_000) ? 'working, no heartbeat' : 'silent');
  }
  devices.sort((a, b) => (b.parts_1h ?? 0) - (a.parts_1h ?? 0) || String(a.tag).localeCompare(String(b.tag)));

  // Which devices share one public IP? Render hands every service in a region
  // the same outbound ranges (render.com/docs/outbound-ip-addresses), so this is
  // the difference between "another instance" and "another IP".
  const byIp = new Map();
  for (const d of devices) {
    if (!d.egress_ip) continue;
    if (!byIp.has(d.egress_ip)) byIp.set(d.egress_ip, []);
    byIp.get(d.egress_ip).push(d.tag);
  }
  const egressGroups = [...byIp.entries()]
    .map(([ip, tags]) => ({ ip, tags, count: tags.length }))
    .sort((a, b) => b.count - a.count);
  const sharedIps = egressGroups.filter((g) => g.count > 1);

  const sumStatus = (pick) => devices.reduce((acc, d) => {
    for (const [k, v] of Object.entries(pick(d) || {})) acc[k] = (acc[k] || 0) + Number(v || 0);
    return acc;
  }, {});
  const errMix = sumStatus((d) => d.aimd?.err_by_status);
  const statusMix = sumStatus((d) => d.aimd?.by_status);
  const reqPerSec = Number(devices.reduce((a, d) => a + (d.aimd?.req_per_sec || 0), 0).toFixed(1));
  const parts24 = devices.reduce((a, d) => a + (d.parts_24h || 0), 0);
  const parts1h = devices.reduce((a, d) => a + (d.parts_1h || 0), 0);
  // Prefer the last hour when it exists: it is what the fleet is doing now, and
  // a 24h average hides a fleet that just stopped.
  const partsPerHour = parts1h > 0 ? parts1h : Number((parts24 / 24).toFixed(1));

  const byStatus = Object.fromEntries(queueRows.map((r) => [r.status, r]));
  const pending = byStatus.pending?.n ?? 0;
  const errors = byStatus.error?.n ?? 0;
  const running = byStatus.running?.n ?? 0;
  const done = byStatus.done?.n ?? 0;
  const reserved = (byStatus.pending?.reserved ?? 0) + (byStatus.error?.reserved ?? 0);
  const work = pending + errors - reserved;

  const backpressure = Object.keys(errMix).filter((k) => k !== '0');
  return {
    generated_at: new Date().toISOString(),
    devices,
    fleet: {
      devices: devices.length,
      running: devices.filter((d) => d.state === 'running').length,
      alive: devices.filter((d) => d.state !== 'silent').length,
      req_per_sec: reqPerSec,
      parts_24h: parts24,
      parts_1h: parts1h,
      parts_per_hour: partsPerHour,
      status_mix: statusMix,
      error_mix: errMix,
      backpressure,
      retry_afters: devices.reduce((a, d) => a + (d.aimd?.retry_afters || 0), 0),
    },
    egress: { groups: egressGroups, shared: sharedIps },
    queue: {
      done, pending, running, errors, reserved,
      free: Math.max(0, work),
      total: done + pending + running + errors,
      // Remaining discovered work at the fleet's current pace. It says nothing
      // about undiscovered ACs, which is what the discovery panel is for.
      eta_secs: partsPerHour > 0 && work > 0 ? Math.round((work / partsPerHour) * 3600) : null,
    },
    discovery: backlog,
  };
}

const fleetMemo = memo(config.fleetTtlMs);
const getFleet = () => fleetMemo(fleetSnapshot);
const dbHealthMemo = memo(config.dbHealthTtlMs);
const getDbHealth = () => dbHealthMemo(dbHealth);

// --------------------------------------------------------------- catalog jobs

/** Discover one named AC (the per-AC button), on its own claim: the row is
 * marked 'running' first, so two instances cannot probe the same AC twice. */
function runSingleDiscover(stateCd, acNo) {
  return runJob('discover_parts', { state: stateCd, ac: acNo }, async (progress) => {
    const claimed = await q(
      `update acs set discover_status = 'running', discover_started_at = now(), last_error = null
        where state_cd = $1 and ac_no = $2
          and (discover_status <> 'running'
               or discover_started_at is null
               or discover_started_at < now() - make_interval(mins => $3))
        returning state_cd, ac_no`, [stateCd, acNo, config.discoverStaleMin]);
    if (!claimed.length) {
      return { skipped: true, reason: 'that AC is already being discovered by another device' };
    }
    return discoverAc({ state_cd: stateCd, ac_no: acNo, progress });
  });
}

export function createServer({ worker } = {}) {
  const server = http.createServer(async (req, res) => {
    // A client that goes away mid-answer - a browser cancelling a CSV download,
    // a phone losing signal - resets the socket. That is routine traffic, not a
    // crash: without these listeners the write error is unhandled and takes the
    // whole process down, which would also stop the collector mid-sweep.
    res.on('error', () => {});
    req.on('error', () => {});

    const u = new URL(req.url, 'http://localhost');
    const p = u.pathname;
    try {
      if (p === '/' || p === '/index.html') {
        res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
        res.end(DASHBOARD);
        return;
      }

      // The catalog/fleet admin panel. Read from disk on every request so an
      // edit is visible without a restart, and so this file does not grow the
      // device dashboard's template.
      if (p === '/admin' || p === '/admin.html') {
        const file = path.join(ROOT, 'admin.html');
        const html = await fs.promises.readFile(file, 'utf8');
        res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' });
        res.end(html);
        return;
      }

      if (p === '/api/health') {
        return json(res, 200, { ok: true, device: config.deviceTag, ts: new Date().toISOString() });
      }

      if (p === '/api/summary') {
        // Cheap counters + cached heavy aggregates (claim.js). The page polls
        // every 3s, so nothing that costs a full table scan runs here.
        const s = await summary();
        const queue = await reserveCount(config.deviceTag, config.reserveTtlSec);
        const current = await currentProgress(worker);
        const auto = await getSetting(config.deviceTag, 'auto_enabled', true);
        return json(res, 200, {
          device: config.deviceTag,
          workerId: config.workerId,
          worker: {
            running: !!worker?.running,
            auto: auto === true || String(auto).toLowerCase() === 'true',
            done_parts: worker?.doneParts ?? 0,
            errors: worker?.errors ?? 0,
            queue,
            reserve_n: config.reserveN,
          },
          current,
          last_done: worker?.lastDone ?? null,
          fleet: s.overall,
          aimd: worker?.aimd.snapshot() ?? null,
          holds: s.holds ?? [],
          free: s.free,
          db: await dbStatus(),
          upload: {
            ...writeState,
            source: `${config.schema}@${config.pgDsn.replace(/^.*@/, '').replace(/^.*\//, '')}`,
          },
        });
      }

      // ---- worker control (the Start/Stop buttons) ----
      //
      // Stop pauses at a part boundary and hands this device's reservations
      // back, but the process and dashboard stay up so Start can revive it.
      // The `auto_enabled@<tag>` knob is the single source of truth, so flipping
      // the same switch from the Python dashboard pauses this device too.
      if (p === '/api/worker/start' && req.method === 'POST') {
        await setSetting(config.deviceTag, 'auto_enabled', true);
        const started = worker ? await worker.start() : false;
        await logEvent(config.deviceTag, 'info', 'auto collect on (dashboard)', 'node');
        return json(res, 200, { ok: true, started, running: !!worker?.running });
      }

      if (p === '/api/worker/stop' && req.method === 'POST') {
        await setSetting(config.deviceTag, 'auto_enabled', false);
        worker?.pause();
        await logEvent(config.deviceTag, 'warn', 'auto collect off (dashboard) - finishing current part', 'node');
        return json(res, 200, { ok: true, running: !!worker?.running });
      }

      if (p === '/api/events') {
        // `all=1` reads the whole fleet's stream (the Logs tab); the default stays
        // this device, which is what the device dashboard wants and what keeps the
        // query on the (device, id) path.
        const all = ['1', 'true', 'yes'].includes(String(u.searchParams.get('all') || '').toLowerCase());
        const device = u.searchParams.get('device') || config.deviceTag;
        const limit = Math.min(Number(u.searchParams.get('limit')) || 200, 1000);
        if (all) return json(res, 200, await q('select * from events order by id desc limit $1', [limit]));
        return json(res, 200, await q(
          'select * from events where device = $1 order by id desc limit $2', [device, limit]));
      }

      if (p === '/api/parts') {
        const st = u.searchParams.get('state'), ac = u.searchParams.get('ac'), status = u.searchParams.get('status');
        const text = (u.searchParams.get('q') || '').trim();
        const limit = Math.min(Number(u.searchParams.get('limit')) || 500, 5000);
        const offset = Math.max(0, Number(u.searchParams.get('offset')) || 0);
        // No fan-out and no per-row subquery: `records`/`epics` are already
        // columns on the row, and their sum is the honest "rows fetched" figure.
        // Counting `electors` per part instead costs ~2.1s per AC and grows with
        // an 8 GB table, which is why the Python parts page was slow.
        return json(res, 200, await q(
          `select * from old_parts
            where ($1::text is null or state_cd = $1) and ($2::int is null or ac_no = $2)
              and ($3::text is null or status = $3)
              and ($4::text is null or name ilike '%' || $4 || '%' or part_no::text = $4)
            order by state_cd, ac_no, part_no limit $5 offset $6`,
          [st, ac ? Number(ac) : null, status, text || null, limit, offset]));
      }

      if (p === '/api/electors') {
        const st = u.searchParams.get('state'), ac = u.searchParams.get('ac'), part = u.searchParams.get('part');
        const curPart = u.searchParams.get('cur_part');
        const limit = Math.min(Number(u.searchParams.get('limit')) || 200, 5000);
        const offset = Math.max(0, Number(u.searchParams.get('offset')) || 0);
        const text = (u.searchParams.get('q') || '').trim();
        // Two search styles, because only one of them is cheap on this table:
        //   * an identifier (EPIC or source_id) is an exact index lookup
        //     (`electors_pkey` / `electors_epic_idx`, measured 0.19s);
        //   * a name is matched on a PREFIX only, which is what the
        //     `electors_name_idx (lower(full_name))` index can actually serve.
        // `%name%` cannot use that index: the moment nothing matches it scans
        // all 8 GB of electors. So it is never issued.
        const byId = text ? /^[A-Za-z0-9_-]{4,32}$/.test(text) : false;
        const namePrefix = !!text && !byId;
        const rows = await q(
          `select source_id, state_cd, ac_no, part_no, serial_no,
                  full_name as name, relative_name, relation_type, gender,
                  age_snapshot as age, cur_epic as epic,
                  cur_state_cd, cur_ac_no, cur_part_no, last_seen as updated_at
             from electors
            where ($1::text is null or state_cd = $1)
              and ($2::int  is null or ac_no = $2)
              and ($3::int  is null or part_no = $3)
              and ($6::int  is null or cur_part_no = $6)
              and ($4::text is null or source_id = $4 or cur_epic = $4)
              and ($5::text is null or lower(full_name) like lower($5) || '%')
            order by part_no, serial_no limit $7 offset $8`,
          [st, ac ? Number(ac) : null, part ? Number(part) : null,
           byId ? text : null, namePrefix ? text : null,
           curPart ? Number(curPart) : null, limit, offset]);
        return json(res, 200, {
          mode: !text ? 'browse' : (byId ? 'id' : 'name-prefix'),
          rows,
        });
      }

      if (p === '/api/export.csv') {
        // Name the file after the selection, like the Python route did, so a
        // folder of exports says which AC/part each one came from.
        const bits = [u.searchParams.get('state'), u.searchParams.get('ac')].filter(Boolean);
        const partBit = u.searchParams.get('part') ? `_P${u.searchParams.get('part')}` : '';
        const name = bits.length ? `epics_${bits[0]}_AC${bits[1] || ''}${partBit}.csv` : 'epics.csv';
        res.writeHead(200, {
          'content-type': 'text/csv; charset=utf-8',
          'content-disposition': `attachment; filename="${name.replace(/"/g, '')}"`,
        });
        return writeElectorsCsv(res, {
          state: u.searchParams.get('state'),
          ac: u.searchParams.get('ac'),
          part: u.searchParams.get('part'),
          cur_part: u.searchParams.get('cur_part'),
          // Python's route defaults to EPIC-only; `epics_only=0` exports every row
          // of the part, mapped or not.
          epics_only: !['0', 'false', 'no'].includes(String(u.searchParams.get('epics_only') || '1').toLowerCase()),
        });
      }

      if (p === '/api/auto' && req.method === 'POST') {
        const b = await readBody(req);
        await setSetting(config.deviceTag, 'auto_enabled', !!b.enabled);
        if (b.enabled) await worker?.start(); else worker?.pause();
        await logEvent(config.deviceTag, 'info', `auto collect ${b.enabled ? 'on' : 'off'}`, 'node');
        return json(res, 200, { ok: true, enabled: !!b.enabled });
      }

      if (p === '/api/settings') {
        if (req.method === 'POST') {
          const b = await readBody(req);
          for (const [k, v] of Object.entries(b)) await setSetting(config.deviceTag, k, v);
          return json(res, 200, { ok: true });
        }
        // Per-device knobs live in the shared settings table as 'key@tag' rows
        // (db.py's convention), so a value set here is the same row the Python
        // and Android clients read.
        return json(res, 200, await deviceSettings(config.deviceTag));
      }

      // ------------------------------------------------------- catalog views

      // Every state with its AC/part progress. Aggregate-in-subquery, then join:
      // the obvious `states join acs join old_parts` fans out to 6.8M rows for
      // S01 alone and measured 137s, this shape measured 0.31s.
      if (p === '/api/states') {
        return json(res, 200, await q(`
          select s.state_cd, s.name, s.has_old_data, s.last_checked,
                 coalesce(a.acs, 0)::int         as acs,
                 coalesce(a.discovered, 0)::int  as acs_discovered,
                 coalesce(p.parts, 0)::int       as parts,
                 coalesce(p.done, 0)::int        as done,
                 coalesce(p.pending, 0)::int     as pending,
                 coalesce(p.errors, 0)::int      as errors,
                 coalesce(p.running, 0)::int      as running,
                 coalesce(p.records, 0)::int     as records,
                 coalesce(p.epics, 0)::int       as epics,
                 p.last_finished
            from states s
            left join (
              select state_cd,
                     count(*)::int as acs,
                     count(*) filter (where discover_status = 'done')::int as discovered
                from acs group by 1) a on a.state_cd = s.state_cd
            left join (
              select state_cd,
                     count(*)::int as parts,
                     count(*) filter (where status = 'done')::int as done,
                     count(*) filter (where status = 'pending')::int as pending,
                     count(*) filter (where status = 'error')::int as errors,
                     count(*) filter (where status = 'running')::int as running,
                     coalesce(sum(records), 0)::bigint as records,
                     coalesce(sum(epics), 0)::bigint as epics,
                     max(finished_at) as last_finished
                from old_parts group by 1) p on p.state_cd = s.state_cd
           order by s.state_cd`));
      }

      if (p === '/api/acs') {
        const st = u.searchParams.get('state');
        const limit = Math.min(Number(u.searchParams.get('limit')) || 2000, 5000);
        return json(res, 200, await q(`
          select a.state_cd, a.ac_no, a.name, a.name_l1, a.ac_type, a.district_cd,
                 a.discover_status, a.old_parts_found, a.discover_max,
                 a.discovered_at, a.discover_started_at, a.last_error,
                 coalesce(p.parts, 0)::int    as parts,
                 coalesce(p.done, 0)::int     as done,
                 coalesce(p.pending, 0)::int  as pending,
                 coalesce(p.errors, 0)::int   as errors,
                 coalesce(p.running, 0)::int  as running,
                 coalesce(p.records, 0)::int  as records,
                 p.last_finished,
                 (a.discover_max is not null
                  and a.old_parts_found >= a.discover_max) as maybe_truncated
            from acs a
            left join (
              select state_cd, ac_no,
                     count(*)::int as parts,
                     count(*) filter (where status = 'done')::int as done,
                     count(*) filter (where status = 'pending')::int as pending,
                     count(*) filter (where status = 'error')::int as errors,
                     count(*) filter (where status = 'running')::int as running,
                     coalesce(sum(records), 0)::int as records,
                     max(finished_at) as last_finished
                from old_parts
               where ($1::text is null or state_cd = $1)
               group by 1, 2) p
              on p.state_cd = a.state_cd and p.ac_no = a.ac_no
           where ($1::text is null or a.state_cd = $1)
           order by a.state_cd, a.ac_no limit $2`, [st, limit]));
      }

      // ---------------------------------------------------------- fleet health

      // One non-idle glance at every device: what it is doing, how fast, and
      // whether the gateway is pushing back. This is the page that answers "how
      // many more devices can I add": if `errByStatus` stays empty and the AIMD
      // limit keeps climbing, the ceiling is not the gateway; the moment 429s
      // appear, this is where they show up first.
      if (p === '/api/fleet') {
        const { value, cached } = await getFleet();
        return json(res, 200, { ...value, cached });
      }

      if (p === '/api/db/health') {
        const { value, cached } = await getDbHealth();
        return json(res, 200, { ...value, cached });
      }

      // --------------------------------------------------------- catalog jobs

      if (p === '/api/catalog/job') {
        return json(res, 200, {
          job: jobStatus(),
          backlog: await acsBacklog(),
          stats: await statsSummary(),
        });
      }

      if (p === '/api/catalog/seed-states' && req.method === 'POST') {
        const r = seedStates({ live: true });
        if (!r.started) return json(res, 409, { error: r.reason });
        return json(res, 202, { started: true, job: r.job });
      }

      if (p === '/api/catalog/seed-acs' && req.method === 'POST') {
        const b = await readBody(req).catch(() => ({}));
        const r = seedAcs({ state: b.state || null, live: b.live !== false });
        if (!r.started) return json(res, 409, { error: r.reason });
        return json(res, 202, { started: true, job: r.job });
      }

      // Claim the next undiscovered AC(s) and probe them. One click here grows
      // the whole fleet's work queue, which is the difference between the
      // collectors running dry in seven hours and running all week.
      if (p === '/api/catalog/discover-next' && req.method === 'POST') {
        const b = await readBody(req).catch(() => ({}));
        const batch = Math.max(1, Math.min(Number(b.batch) || 1, 200));
        const r = discoverNext({ state: b.state || null, batch });
        if (!r.started) return json(res, 409, { error: r.reason });
        return json(res, 202, { started: true, job: r.job });
      }

      if (p === '/api/catalog/discover' && req.method === 'POST') {
        const b = await readBody(req);
        if (!b.state || !b.ac) return json(res, 400, { error: 'state and ac are required' });
        const r = runSingleDiscover(String(b.state), Number(b.ac));
        if (!r.started) return json(res, 409, { error: r.reason });
        return json(res, 202, { started: true, job: r.job });
      }

      if (p === '/api/catalog/retry-errored' && req.method === 'POST') {
        const b = await readBody(req).catch(() => ({}));
        return json(res, 200, await retryErroredAcs({ state: b.state || null }));
      }

      // ------------------------------------------------ parts & collection

      // One part: the row (with the current-roll name it maps to), where its
      // people ended up on the current roll, and its relation mix.
      if (p === '/api/part') {
        const d = await partDetail({
          state: u.searchParams.get('state'),
          ac: u.searchParams.get('ac'),
          part: u.searchParams.get('part'),
        });
        if (!d) return json(res, 404, { error: 'part not collected/discovered' });
        return json(res, 200, d);
      }

      // Current-roll part names for an AC, cached in `current_parts`. Only
      // `refresh=1` talks to the gateway; every other read is the DB cache.
      if (p === '/api/current_parts') {
        return json(res, 200, await currentParts({
          state: u.searchParams.get('state'),
          ac: u.searchParams.get('ac'),
          refresh: ['1', 'true', 'yes'].includes(String(u.searchParams.get('refresh') || '').toLowerCase()),
        }));
      }

      if (p === '/api/breakdown') {
        return json(res, 200, await breakdown({
          state: u.searchParams.get('state'), ac: u.searchParams.get('ac'),
        }));
      }

      // Seed parts for an AC. Two honest paths:
      //   probe  — the verified one (catalog.js discovery): asks the gateway for
      //            every part number and writes the ones that exist;
      //   manual — writes the numbers you give as pending straight away, with no
      //            probe, so a wrong number becomes a sweep that finds nothing.
      if (p === '/api/parts/seed' && req.method === 'POST') {
        const b = await readBody(req);
        const mode = String(b.mode || ((b.from || b.to) ? 'manual' : 'probe'));
        if (mode === 'probe') {
          if (!b.state || !b.ac) return json(res, 400, { error: 'state and ac are required' });
          const r = runSingleDiscover(String(b.state), Number(b.ac));
          if (!r.started) return json(res, 409, { error: r.reason });
          return json(res, 202, { started: true, mode, job: r.job });
        }
        if (mode !== 'manual') return json(res, 400, { error: "mode must be 'probe' or 'manual'" });
        return json(res, 200, {
          mode,
          ...(await seedPartsManual({
            state: b.state, ac: b.ac, parts: b.parts || [], from: b.from ?? null, to: b.to ?? null,
          })),
        });
      }

      // Queue work for the fleet. The part(s) go back to `pending`, which is the
      // shared picker's input, so a panel that never sweeps can still direct the
      // sweep. Nothing here touches a part another device is running right now.
      if (p === '/api/collect' && req.method === 'POST') {
        const b = await readBody(req);
        return json(res, 200, await collectPart({
          state: b.state, ac: b.ac, part: b.part, force: !!b.force,
          priority: b.priority === undefined || b.priority === null ? null : Number(b.priority),
        }));
      }

      if (p === '/api/collect_auto' && req.method === 'POST') {
        const b = await readBody(req);
        return json(res, 200, await collectAc({
          state: b.state, ac: b.ac, force: !!b.force,
        }));
      }

      // Sweep here and now, over the same direct-HTTP route the collector uses -
      // the other half of Collect (which only queues the work for the fleet).
      // One run per instance, capped, and it publishes no heartbeat and takes no
      // reservation holds, so a click cannot turn this process into a second
      // sweeper (see sweep.js).
      if (p === '/api/sweep') {
        if (req.method === 'GET') return json(res, 200, sweepStatus());
        const b = await readBody(req);
        const r = startSweep({
          state: b.state, ac: b.ac ?? null, part: b.part ?? null,
          force: !!b.force, parts: b.parts, minutes: b.minutes,
        });
        return json(res, r.started ? 202 : 409, r);
      }

      if (p === '/api/sweep/stop' && req.method === 'POST') {
        return json(res, 200, stopSweep());
      }

      // -------------------------------------------------------- jobs & EPIC

      // The shared `jobs` table belongs to the Python/Android clients' queue; this
      // panel lists and cancels those jobs so one page answers "what is the fleet
      // working on". The Node worker sweeps parts directly and needs no job row.
      if (p === '/api/jobs') {
        return json(res, 200, await listJobs({
          status: u.searchParams.get('status') || null,
          limit: u.searchParams.get('limit'),
        }));
      }

      const cancelPath = /^\/api\/jobs\/(\d+)\/cancel$/.exec(p);
      if (cancelPath && req.method === 'POST') {
        return json(res, 200, await cancelJob(cancelPath[1]));
      }

      if (p === '/api/epics') {
        return json(res, 200, await recentEpics({ limit: u.searchParams.get('limit') }));
      }
      if (p.startsWith('/api/epic/')) {
        const row = await getEpic(decodeURIComponent(p.slice('/api/epic/'.length)));
        if (!row) return json(res, 404, { error: 'not looked up yet' });
        return json(res, 200, row);
      }
      if (p === '/api/epic' && req.method === 'POST') {
        const b = await readBody(req);
        return json(res, 200, await epicLookup(b.epic, { refresh: !!b.refresh }));
      }

      return json(res, 404, { error: 'not found' });
    } catch (e) {
      if (res.writableEnded || res.destroyed) return;
      return json(res, 500, { error: String(e.message || e) });
    }
  });

  server.on('clientError', (err, socket) => {
    if (socket.writable && !socket.destroyed) socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
    else socket.destroy();
  });
  return server;
}

// The dashboard is a monitor, not a control room: the fleet numbers come from
// the database, and everything else is THIS device. There is deliberately no
// fleet-wide log view - each device reads its own stream.
const DASHBOARD = `<!doctype html>
<html><head><meta charset="utf-8"><title>eci-node-collector</title>
<style>
 body{font:14px/1.45 system-ui,sans-serif;margin:0;padding:20px 24px 40px;background:#0d1117;color:#c9d1d9}
 h1{font-size:17px;margin:0 0 4px} h1 span.dev{color:#58a6ff}
 .bar-row{display:flex;align-items:center;gap:10px;margin:10px 0 18px}
 button{background:#1f6feb;color:#fff;border:0;border-radius:6px;padding:7px 14px;font-size:13px;cursor:pointer}
 button.ghost{background:#21262d;border:1px solid #30363d}
 button:disabled{opacity:.4;cursor:default}
 .badge{font-size:12px;padding:2px 8px;border-radius:10px;border:1px solid #30363d}
 .ok{color:#3fb950} .off{color:#d29922} .muted{color:#8b949e} .small{font-size:12px}
 .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:16px}
 .card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:12px 14px}
 .k{font-size:11px;letter-spacing:.06em;text-transform:uppercase;color:#8b949e}
 .v{font-size:24px;font-weight:600;margin-top:2px}
 .sub{font-size:11px;color:#8b949e;margin-top:2px}
 .wide{grid-column:1/-1}
 .bar{height:8px;background:#21262d;border-radius:4px;overflow:hidden;margin-top:8px}
 .bar i{display:block;height:100%;background:#1f6feb}
 #logs{font:12px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace;max-height:300px;overflow:auto;white-space:pre-wrap}
 table{border-collapse:collapse;font-size:12px}
 td{padding:2px 10px 2px 0}
</style></head><body>
<h1>eci-node-collector · <span class="dev" id="dev">…</span> <span class="badge" id="wbadge">…</span>
  <a href="/admin" style="float:right;font-size:12px;color:#58a6ff;text-decoration:none">catalog &amp; fleet →</a></h1>
<div class="small muted" id="wmeta">&nbsp;</div>
<div class="bar-row">
  <button id="btnStart">▶ Start collecting</button>
  <button id="btnStop" class="ghost">■ Stop</button>
  <span class="small muted" id="ctlmsg"></span>
</div>

<div class="grid" id="fleet"></div>
<div class="grid" id="pipeline"></div>
<div class="grid" id="device"></div>

<div class="card"><div class="k">Logs — this device only</div><div id="logs"></div></div>

<script>
function esc(s){return String(s==null?'':s).replace(/[&<>"']/g,function(c){
  return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
// Same formatting as the Python dashboard: the browser's own locale, so both
// apps render identical digits on the same machine.
function nf(n){return (n==null)?'—':Number(n).toLocaleString();}
// The 4th argument (sql) becomes a hover tooltip: every number on this page can
// be reproduced with exactly one query against the shared database.
function card(k,v,sub,sql){return '<div class="card"'+(sql?' title="'+esc(sql)+'"':'')+'>'
  +'<div class="k">'+esc(k)+'</div><div class="v">'+v+'</div>'
  +(sub?'<div class="sub">'+sub+'</div>':'')+'</div>';}
function ago(iso){ if(!iso) return 'never'; var s=Math.round((Date.now()-new Date(iso).getTime())/1000);
  return s<0?'now':(s+'s ago'); }
function pct(v){return (v==null)?'—':v+'%';}
function errList(e){var p=[];for(var k in e){p.push((k==='0'?'net/timeout':k)+':'+e[k]);}return p.join(' ')||'none';}
function go(path){
  var m=document.getElementById('ctlmsg'); m.textContent='working…';
  fetch(path,{method:'POST'}).then(function(r){return r.json();}).then(function(){
    m.textContent=''; tick();}).catch(function(e){m.textContent='failed: '+e.message;});
}
document.getElementById('btnStart').onclick=function(){go('/api/worker/start');};
document.getElementById('btnStop').onclick=function(){go('/api/worker/stop');};

async function tick(){
  var s; try{ s=await (await fetch('/api/summary')).json(); }catch(e){ return; }
  var f=s.fleet||{}, a=s.aimd||{}, w=s.worker||{}, c=s.current, ld=s.last_done;
  document.getElementById('dev').textContent=s.device;
  document.getElementById('wbadge').innerHTML = w.running
    ? '<span class="ok">running</span>' : '<span class="off">paused</span>';
  document.getElementById('wmeta').textContent = s.workerId+' · queue '+w.queue+'/'+w.reserve_n
    +' · parts done here '+nf(w.done_parts)+(w.errors?(' · sweep errors '+w.errors):'');
  document.getElementById('btnStart').disabled=!!w.running;
  document.getElementById('btnStop').disabled=!w.running;

  // Two of these are exact only once every couple of minutes: a live COUNT(*)
  // over electors costs ~50s and COUNT(DISTINCT cur_epic) ~60s, so they are
  // cached and the age is shown. Nothing here is estimated.
  var heavy = (f.heavy_age_secs==null) ? 'computing…' : 'cached '+f.heavy_age_secs+'s ago';
  document.getElementById('fleet').innerHTML =
      card('States', nf(f.states), null, 'select count(*) from states')
    + card('ACs', nf(f.acs), null, 'select count(*) from acs')
    + card('Old parts', nf(f.old_parts), null, 'select count(*) from old_parts')
    + card('Parts done', nf(f.done_parts), null, "select count(*) from old_parts where status='done'")
    + card('Pending', nf(f.pending_parts), null, "select count(*) from old_parts where status='pending'")
    + card('Errors', nf(f.error_parts), 'parts handed back to retry',
           "select count(*) from old_parts where status='error'")
    + card('Rows in electors', nf(f.electors),
           'unique people, upserted on source_id · '+heavy,
           'select count(*) from electors')
    + card('Rows fetched', nf(f.records),
           'summed over parts (a person can be returned by several parts, so this runs ahead of electors) · exact',
           'select sum(records) from old_parts')
    + card('Unique EPICs', nf(f.unique_epics), heavy,
           // length()>0 instead of <> '' so no quote has to survive this template
           'select count(distinct cur_epic) from electors where cur_epic is not null and length(cur_epic) > 0');

  // ---- pipeline: is the database reachable, and is everything uploaded? ----
  var db = s.db || {}, up = s.upload || {};
  var dbCard = card('Database', db.connected
      ? '<span class="ok">connected</span>'
      : '<span class="off">DOWN</span>',
    'ping '+db.ping_ms+'ms · pool '+db.pool_total+' ('+db.pool_idle+' idle'
    + (db.pool_waiting?', '+db.pool_waiting+' waiting':'')+')<br>'+esc(up.source||'')
    + (db.last_error?('<br><span class="off">last error '+esc(db.last_error.message)+' at '+esc(db.last_error.at.slice(11,19))+'</span>'):'')
    , 'select 1');
  // "Is everything uploaded?" Every row the sweep fetches goes into one batch,
  // so fetched = written + buffered at all times; buffered is therefore the
  // exact amount not yet in the database, and it drains to 0 at part boundaries.
  var buf = up.buffered||0, fe = up.flush_errors||0;
  // Every fetched row goes through the batch, so flushed + buffered is exactly
  // the number of rows fetched this run - that is the checkable invariant.
  // Do NOT compare a part's records against how many electors carry that part
  // number: state_cd/ac_no/part_no are not updated on conflict - the same as
  // the Python upsert - so a person keeps the part that first saw them, and a
  // later part can legitimately show 0 even though every row was written.
  var perPart = '<br>this run: '+nf((up.flushed||0)+buf)+' rows fetched = '
    + nf(up.flushed||0)+' written + '+nf(buf)+' buffered';
  if (c && c.stored_rows!=null){
    perPart += ' · this part added '+nf(c.stored_rows)+' new people of '+nf(c.records)+' rows fetched';
  }
  var upCard = card('Upload / cache', buf
      ? '<span class="off">'+nf(buf)+'</span> <span class="small muted">rows not yet written</span>'
      : '<span class="ok">0</span> <span class="small muted">rows not yet written</span>',
    nf(up.flushed)+' rows written this run · '+nf(up.flushes)+' batches · last '+ago(up.last_flush_at)
    + (up.last_flush_ms!=null?(' ('+up.last_flush_ms+'ms)'):'')
    + (fe?(' · <span class="off">'+fe+' failed batches</span>'):' · 0 failed batches')
    + perPart
    + '<br>rows in the two cached counters are '+(f.heavy_age_secs==null?'being computed':(f.heavy_age_secs+'s old'))
    + '; recomputed every 120s', 'select count(*) from electors');
  document.getElementById('pipeline').innerHTML = dbCard + upCard;

  var speed;
  if (c && c.req_per_sec!=null){
    speed = '<span class="v">'+c.req_per_sec+'</span> <span class="small muted">req/s</span>'
      + '<div class="sub">'+ (c.rec_per_sec!=null?c.rec_per_sec+' rows/s · ':'')
      + (c.eta_secs!=null?('eta '+c.eta_secs+'s'):'') + '</div>';
  } else {
    speed = '<span class="v">'+a.recentReqPerSec+'</span> <span class="small muted">req/s</span>'
      + '<div class="sub">'+(c?'measuring…':'idle')+'</div>';
  }
  var ctrl = card('Controller (AIMD)',
      '<span class="v">'+a.limit+'</span> <span class="small muted">limit</span>',
      'inflight '+a.inflight+' · peak '+a.peak+'<br>reqs '+nf(a.reqs)+' · errors '+nf(a.errReqs)
      +' ('+errList(a.errByStatus)+')'+(a.retryAfters?(' · retry-after '+a.retryAfters):'')
      +'<br>404 (past roll end) is not counted as an error');

  var partHtml;
  if (c){
    var line = c.state_cd+'/'+c.ac_no+'/'+c.part_no + (c.name?(' · '+esc(c.name)):'');
    // A part that was just claimed has no measured roll end yet (the probe runs
    // first), so say so instead of showing a meaningless 0 / ? .
    var measuring = (c.roll_end == null);
    partHtml = '<div class="k">This device — current part</div>'
      + '<div style="font-size:15px;font-weight:600;margin-top:2px">'+line+'</div>'
      + '<div class="sub">status <b>'+esc(c.status)+'</b>'
      + (measuring ? ' · measuring the roll end…'
                   : ' · '+nf(c.last_serial)+' / '+nf(c.roll_end)+' serials')
      + ' · '+nf(c.records)+' rows · '+(c.elapsed_s||0)+'s elapsed'
      + (c.eta_secs!=null?(' · eta '+c.eta_secs+'s'):'')+'</div>'
      + (measuring ? ''
        : '<div class="bar"><i style="width:'+(c.pct||0)+'%"></i></div>'
          + '<div class="sub">'+pct(c.pct)+' of the measured roll end</div>');
  } else if (ld){
    partHtml = '<div class="k">This device — last finished part</div>'
      + '<div style="font-size:15px;font-weight:600;margin-top:2px">'
      + ld.state_cd+'/'+ld.ac_no+'/'+ld.part_no+(ld.name?(' · '+esc(ld.name)):'')+'</div>'
      + '<div class="sub">done · '+nf(ld.records)+' rows · '+nf(ld.epics)+' EPICs · roll_end '
      + nf(ld.roll_end)+' · '+ld.seconds+'s</div>';
  } else {
    partHtml = '<div class="k">This device — current part</div><div class="v">—</div>'
      + '<div class="sub">(nothing swept yet; press Start if paused)</div>';
  }
  document.getElementById('device').innerHTML =
      card('Speed (live)', speed)
    + card('Parts here', nf(w.done_parts), 'queue '+w.queue+' reserved')
    + ctrl
    + '<div class="card wide">'+partHtml+'</div>';

  var ev=[]; try{ ev=await (await fetch('/api/events?limit=120')).json(); }catch(e){}
  document.getElementById('logs').textContent = ev.map(function(e){
    return String(e.ts).slice(11,19)+' ['+e.level+'] '+e.message; }).join('\\n') || '(no events yet)';
}
tick(); setInterval(tick,3000);
</script></body></html>`;
