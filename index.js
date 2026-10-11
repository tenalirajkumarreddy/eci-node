#!/usr/bin/env node
import { createServer } from './server.js';
import { Worker } from './worker.js';
import { writeState } from './collector.js';
import { ensureSchema } from './schema.js';
import { q, pool, logEvent } from './db.js';
import { config } from './config.js';
import { EpicProcessor } from './epic.js';
import { writeElectorsCsv } from './csv.js';
import { setSetting } from './claim.js';
import {
  seedStates, seedAcs, discoverNext, discoverAc, acsBacklog,
} from './catalog.js';
import fs from 'node:fs';

function parseArgs(argv) {
  const a = { _: [] };
  for (let i = 0; i < argv.length; i++) {
    const k = argv[i];
    if (!k.startsWith('--')) { a._.push(k); continue; }
    const key = k.slice(2);
    const next = argv[i + 1];
    if (next !== undefined && !next.startsWith('--')) { a[key] = next; i += 1; } else a[key] = true;
  }
  return a;
}
const args = parseArgs(process.argv.slice(2));
const num = (v) => (v ? Number(v) || null : null);

/** Catalogue jobs start in the background (they are also what the /admin page
 * runs); a CLI caller wants the result, so wait for the job and echo progress. */
async function waitJob(started) {
  if (!started.started) throw new Error(started.reason);
  const job = started.job;
  let last = '';
  for (;;) {
    const line = JSON.stringify(job.progress || {});
    if (line !== last) { console.log('  progress', line); last = line; }
    if (job.done) break;
    await new Promise((r) => setTimeout(r, 2000));
  }
  if (job.error) throw new Error(job.error);
  return job.result;
}

async function printStatus() {
  const { overall, devices } = await import('./claim.js').then((m) => m.summary());
  console.log('device tag :', config.deviceTag);
  console.log('overall    :', JSON.stringify(overall));
  console.log('holders    :', devices.map((d) => `${d.claimed_by}=${d.c}`).join('  ') || '(none)');
  const st = (await q(`select count(*)::int c from old_parts where status in ('pending','error')
                       and (reserved_by is null or reserved_at < now() - make_interval(secs => $1))`,
                      [config.reserveTtlSec]))[0];
  console.log('free parts :', st.c);
  const ev = await q('select device, count(*)::int c, max(ts) latest from events group by 1 order by 2 desc');
  console.log('log streams:', ev.map((r) => `${r.device}=${r.c}`).join('  ') || '(none)');
}

async function main() {
  if (!config.pgDsn) {
    console.error('ECI_PG_DSN is not set — copy .env.example to .env and fill it in (or export the var).');
    process.exit(1);
  }
  if (!args['no-init']) {
    const created = await ensureSchema();
    if (created) console.log('schema created');
  }

  if (args['status']) {
    await printStatus();
    // The heavy electors counters refresh in the background; a one-shot CLI must
    // not sit for ~50s waiting on a full scan it will never display.
    pool.end().catch(() => {});
    process.exit(0);
  }

  if (args['epic']) {
    const p = new EpicProcessor(config);
    console.log(JSON.stringify(await p.lookup(String(args['epic'])), null, 2));
    await pool.end();
    return;
  }

  // ---- catalogue maintenance (same code the /admin buttons run) ----
  if (args['seed-states'] || args['seed-acs'] || args['discover-next'] || args['discover']
      || args['acs-backlog']) {
    if (args['acs-backlog']) {
      console.log(JSON.stringify(await acsBacklog({
        state: typeof args['acs-backlog'] === 'string' ? args['acs-backlog'] : null,
      }), null, 2));
      await pool.end();
      return;
    }
    const started = args['seed-states'] ? seedStates({ live: true })
      : args['seed-acs'] ? seedAcs({
          state: typeof args['seed-acs'] === 'string' ? args['seed-acs'] : null, live: true })
      : args['discover-next'] ? discoverNext({
          state: typeof args['discover-next'] === 'string' ? args['discover-next'] : null,
          batch: num(args['batch']) || 1 })
      : null;
    if (started) {
      const result = await waitJob(started);
      console.log(JSON.stringify(result, null, 2));
    } else {
      const [sc, ac] = String(args['discover']).split('/');
      if (!sc || !ac) throw new Error('usage: --discover S01/1  (state/AC)');
      console.log(JSON.stringify(await discoverAc({ state_cd: sc, ac_no: Number(ac) }), null, 2));
    }
    await pool.end();
    return;
  }

  if (args['export']) {
    await new Promise((res, rej) => {
      const ws = fs.createWriteStream(String(args['export']));
      // 'close' always fires (after finish, or on destroy), so an aborted write
      // can never leave the CLI waiting forever on 'finish'.
      ws.on('close', res); ws.on('error', rej);
      writeElectorsCsv(ws, { state: args['state'] || null, ac: args['ac'] || null }).catch(rej);
    });
    console.log(`wrote ${args['export']}`);
    await pool.end();
    return;
  }

  const worker = new Worker({
    scope:  { state: args['state'] || null, ac: args['ac'] || null },
    force:  !!args['force'],
    noRecover: !!args['no-recover'],
    limits: { parts: num(args['parts']), minutes: num(args['minutes']), idleExit: num(args['idle-exit']) },
  });
  if (args['workers']) worker.aimd.limit = Math.max(1, Number(args['workers']));
  // Publish this device's health (and its egress IP) for the fleet view, even
  // while paused: the panel must be able to see a stopped device too.
  if (!args['no-heartbeat']) worker.startHeartbeat();

  // --no-collect: serve the catalog/fleet panel WITHOUT joining the sweep.
  // Useful for a machine that should host the panel (or the mirror/export) but
  // whose IP or CPU the fleet does not want: it never claims a part, never
  // touches auto_enabled@<tag>, and looks exactly like a dormant device.
  const collect = !args['no-collect'];
  if (collect) {
    // Stored as a real jsonb boolean, which is what the Python clients write and
    // read for the 'auto_enabled@<tag>' knob.
    await setSetting(config.deviceTag, 'auto_enabled', true);
    await worker.start();
  } else {
    console.log('collecting disabled (--no-collect): serving the panel only');
  }

  const headless = !!args['headless'] && collect;
  let srv = null;
  if (!headless) {
    // PaaS hosts (Render, Railway, Fly) inject $PORT and require a 0.0.0.0
    // bind; a plain local run stays on 127.0.0.1 so the (unauthenticated)
    // dashboard is never exposed to the LAN by accident.
    const port = Number(args['port']) || Number(process.env.PORT) || 8008;
    const host = String(args['host'] || process.env.HOST || (process.env.PORT ? '0.0.0.0' : '127.0.0.1'));
    srv = createServer({ worker });
    await new Promise((resolve, reject) => {
      srv.once('error', reject);
      srv.listen(port, host, resolve);
    });
    console.log(`dashboard → http://${host === '0.0.0.0' ? '127.0.0.1' : host}:${port}/ (bound ${host}:${port})`);
  }

  let shuttingDown = false;
  const shutdown = async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log('shutting down…');
    worker.stop();
    try { await worker.stopHeartbeat(); } catch { /* last beat is best effort */ }
    try { await new Promise((res) => (srv ? srv.close(res) : res())); } catch { /* ignore */ }
    try { await pool.end(); } catch { /* ignore */ }
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  if (headless) {
    // One-shot mode: exit once the worker stops on its own (--parts/--minutes).
    await new Promise((res) => {
      const t = setInterval(() => { if (!worker.running) { clearInterval(t); res(); } }, 500);
      t.unref?.();
    });
    // Permanent record of the write path: rows committed vs rows the sweeps
    // reported. They agree unless a flush failed, in which case the part was
    // handed back to `pending` rather than closed as done.
    await logEvent(config.deviceTag, 'info',
      `write summary: ${writeState.flushed} rows committed in ${writeState.flushes} batches`
      + ` (${writeState.flush_errors} failed)${writeState.buffered ? `, ${writeState.buffered} still buffered` : ''}`,
      'node');
    try { await worker.stopHeartbeat(); } catch { /* best effort */ }
    await pool.end();
    process.exit(0);
  }

  // Serving mode: the dashboard OUTLIVES the worker, because Stop only pauses
  // sweeping (and hands the reserved parts back) while the page stays up so
  // Start can revive it. The process ends on SIGINT/SIGTERM.
  await new Promise(() => {});
}

main().catch(async (e) => { console.error(e); try { await pool.end(); } catch {} process.exit(1); });
