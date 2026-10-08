import http from 'node:http';
import { q, q1, logEvent, dbStatus } from './db.js';
import { writeState } from './collector.js';
import { config } from './config.js';
import { setSetting, getSetting, deviceSettings, summary, reserveCount } from './claim.js';
import { writeElectorsCsv } from './csv.js';

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
        const device = u.searchParams.get('device') || config.deviceTag;
        const limit = Math.min(Number(u.searchParams.get('limit')) || 200, 1000);
        return json(res, 200, await q(
          'select * from events where device = $1 order by id desc limit $2', [device, limit]));
      }

      if (p === '/api/parts') {
        const st = u.searchParams.get('state'), ac = u.searchParams.get('ac'), status = u.searchParams.get('status');
        const limit = Math.min(Number(u.searchParams.get('limit')) || 500, 5000);
        return json(res, 200, await q(
          `select * from old_parts
            where ($1::text is null or state_cd = $1) and ($2::text is null or ac_no = $2)
              and ($3::text is null or status = $3)
            order by state_cd, ac_no, part_no limit $4`, [st, ac, status, limit]));
      }

      if (p === '/api/electors') {
        const st = u.searchParams.get('state'), ac = u.searchParams.get('ac'), part = u.searchParams.get('part');
        const limit = Math.min(Number(u.searchParams.get('limit')) || 200, 5000);
        // The shared electors table names things the way db.py does (full_name,
        // age_snapshot, cur_epic); alias to the compact names this API promised
        // rather than duplicating the columns.
        return json(res, 200, await q(
          `select source_id, state_cd, ac_no, part_no, serial_no,
                  full_name as name, relative_name, relation_type, gender,
                  age_snapshot as age, cur_epic as epic,
                  cur_state_cd, cur_ac_no, cur_part_no, last_seen as updated_at
             from electors
            where ($1::text is null or state_cd = $1) and ($2::text is null or ac_no = $2)
              and ($3::int  is null or part_no = $3)
            order by part_no, serial_no limit $4`,
          [st, ac ? Number(ac) : null, part ? Number(part) : null, limit]));
      }

      if (p === '/api/export.csv') {
        res.writeHead(200, {
          'content-type': 'text/csv; charset=utf-8',
          'content-disposition': 'attachment; filename="electors.csv"',
        });
        return writeElectorsCsv(res, { state: u.searchParams.get('state'), ac: u.searchParams.get('ac') });
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
<h1>eci-node-collector · <span class="dev" id="dev">…</span> <span class="badge" id="wbadge">…</span></h1>
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
