// agent.js — the bridge between your local collector and the Cloudflare Worker.
//
// Run it next to the collector (`node cf-worker/agent.js`). It long-polls the
// Worker for parked panel requests, forwards each one to the local server and
// posts the real answer back. Nothing is reimplemented: the Worker is showing
// you the actual http://127.0.0.1:8008/admin.
//
// FIRST RUN
//   It generates a panel key + an agent key, registers them with the Worker
//   (POST /__agent/bootstrap), saves them to cf-worker/.agent-keys.json and
//   prints the panel URL with ?k=<panel key>. Open that once; the Worker swaps
//   it for an HttpOnly cookie so the key leaves the address bar.
//
// CONFIG (reads .env from the project root, then the environment)
//   ECI_WORKER_URL     https://<worker>.<subdomain>.workers.dev   (required)
//   ECI_LOCAL_ORIGIN   http://127.0.0.1:8008                      (default)
//   ECI_AGENT_WAIT     seconds the Worker holds the agent poll     (default 20)
//   ECI_AGENT_CONCURRENCY  parallel local requests                 (default 4)
//
// Zero dependencies (Node >= 20.12). Secrets stay in this file/on this disk.

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.dirname(HERE);                       // the collector project
const KEY_FILE = path.join(HERE, '.agent-keys.json');

// --- config -----------------------------------------------------------------
for (const candidate of [path.join(ROOT, '.env'), path.join(process.cwd(), '.env')]) {
  try { if (fs.existsSync(candidate)) { process.loadEnvFile(candidate); break; } } catch { /* keep going */ }
}

const WORKER_URL = (process.env.ECI_WORKER_URL || '').replace(/\/+$/, '');
const LOCAL_ORIGIN = (process.env.ECI_LOCAL_ORIGIN || 'http://127.0.0.1:8008').replace(/\/+$/, '');
const WAIT = Math.max(5, Number(process.env.ECI_AGENT_WAIT) || 20);
const CONCURRENCY = Math.max(1, Number(process.env.ECI_AGENT_CONCURRENCY) || 4);
// How many long-polls to keep open at once. One is not enough: while it is
// forwarding a request and posting the answer back, no poll is parked in the
// Durable Object, so the next panel request would have to wait for the next
// poll round trip (seconds, measured). Several pollers keep a rendezvous
// waiting at all times.
//
// NOTE: keep this at 1. A Durable Object serialises held requests, so several
// eager pollers do not add capacity — they queue up and delay the panel (each
// hold blocks the next event). One poller with a short Worker-side hold is the
// least-bad configuration until the hang is replaced by a hibernatable
// WebSocket (see README, "Latency").
const POLLERS = Math.max(1, Number(process.env.ECI_AGENT_POLLERS) || 1);
const HEARTBEAT_MS = 15000;
const DEVICE = process.env.ECI_DEVICE_TAG || os.hostname();

const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);

/** undici buries the real reason in `.cause` — "fetch failed" alone is useless
 * when the cause is DNS, TLS or a refused connection. */
const why = (e) => {
  const c = e && e.cause;
  const inner = c ? ` (${c.code || c.message || String(c)})` : '';
  return `${(e && e.message) || e}${inner}`;
};

/** Retry a network step a few times: the Worker is on the far side of the
 * internet, and a blip at startup must not kill a long-running agent. */
async function withRetry(label, fn, tries = 5, baseMs = 1000) {
  for (let i = 1; ; i++) {
    try { return await fn(); }
    catch (e) {
      if (i >= tries) throw new Error(`${label} failed after ${tries} attempts: ${why(e)}`);
      log(`${label} failed (${why(e)}), retry ${i}/${tries - 1}`);
      await sleep(baseMs * i);
    }
  }
}

if (!WORKER_URL) {
  console.error('ECI_WORKER_URL is not set. Add it to .env, e.g.');
  console.error('  ECI_WORKER_URL=https://eci-node.<your-subdomain>.workers.dev');
  process.exit(1);
}

// --- keys -------------------------------------------------------------------
function loadOrCreateKeys() {
  try {
    const saved = JSON.parse(fs.readFileSync(KEY_FILE, 'utf8'));
    if (saved.panelKey && saved.agentKey) return { ...saved, fresh: false };
  } catch { /* first run */ }
  const keys = {
    panelKey: crypto.randomBytes(24).toString('base64url'),
    agentKey: crypto.randomBytes(24).toString('base64url'),
    worker: WORKER_URL,
    created_at: new Date().toISOString(),
  };
  fs.writeFileSync(KEY_FILE, JSON.stringify(keys, null, 2), { mode: 0o600 });
  return { ...keys, fresh: true };
}

let KEYS = loadOrCreateKeys();

async function bootstrap() {
  const res = await withRetry('bootstrap', () => fetch(`${WORKER_URL}/__agent/bootstrap`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ panel_key: KEYS.panelKey, agent_key: KEYS.agentKey }),
  }));
  const text = await res.text();
  if (res.ok) return { ok: true };
  // 409 = the Worker already has keys. That is only a problem when they are a
  // DIFFERENT pair (a stale local file, or a Worker someone else bootstrapped).
  if (res.status === 409) return { ok: false, conflict: true, text };
  return { ok: false, text: `${res.status} ${text}` };
}

async function ensureRegistered() {
  if (!KEYS.fresh) {
    // Cheap liveness probe: an authorised status call proves the stored keys
    // still match this Worker before we start polling with them.
    const res = await withRetry('status probe', () => fetch(`${WORKER_URL}/__agent/status`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${KEYS.agentKey}` },
      body: JSON.stringify({ device: DEVICE, origin: LOCAL_ORIGIN }),
    }), 3, 1000).catch(() => null);
    if (res && res.ok) return;
  }
  const r = await bootstrap();
  if (r.ok) {
    KEYS.fresh = false;
    log('registered with the Worker ✓');
    if (r.__fresh) {
      console.log('');
      console.log('  Panel URL (open once, it sets a cookie):');
      console.log(`  ${WORKER_URL}/?k=${KEYS.panelKey}`);
      console.log('');
    }
    return;
  }
  if (r.conflict) {
    console.error('The Worker is already bootstrapped with a different key pair.');
    console.error(`Delete ${KEY_FILE} to adopt the existing pair, or reset the Worker's`);
    console.error('agent_state table to re-register.');
    process.exit(2);
  }
  throw new Error(`bootstrap failed: ${r.text}`);
}

// --- forwarding -------------------------------------------------------------
async function forward(req) {
  const target = `${LOCAL_ORIGIN}${req.path}`;
  const init = { method: req.method, headers: {} };
  if (req.ctype) init.headers['content-type'] = req.ctype;
  if (req.method !== 'GET' && req.method !== 'HEAD' && req.body) init.body = req.body;
  const t0 = Date.now();
  try {
    const res = await fetch(target, init);
    const body = await res.text();
    return {
      id: req.id,
      cache_key: `${req.method} ${req.path}`,
      status: res.status,
      ctype: res.headers.get('content-type') || 'application/octet-stream',
      body,
      ms: Date.now() - t0,
    };
  } catch (e) {
    // A dead local server must read as "local is down", not as a Worker fault.
    return {
      id: req.id,
      cache_key: null,
      status: 502,
      ctype: 'application/json',
      body: JSON.stringify({
        error: 'local collector unreachable',
        detail: `${e.name || 'Error'}: ${e.message}`,
        target,
      }),
      ms: Date.now() - t0,
    };
  }
}

async function replyTo(results) {
  if (!results.length) return;
  const res = await fetch(`${WORKER_URL}/__agent/reply`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: `Bearer ${KEYS.agentKey}` },
    body: JSON.stringify({ results }),
  });
  if (!res.ok) throw new Error(`reply rejected: ${res.status} ${await res.text()}`);
}

// --- loops ------------------------------------------------------------------
let stopping = false;

async function pollLoop(lane = 1) {
  let backoff = 1000;
  while (!stopping) {
    let parked;
    try {
      const res = await fetch(`${WORKER_URL}/__agent/poll?wait=${WAIT}`, {
        headers: { authorization: `Bearer ${KEYS.agentKey}` },
      });
      if (res.status === 401) { await ensureRegistered(); continue; }
      if (!res.ok) throw new Error(`poll ${res.status}`);
      parked = (await res.json()).requests || [];
      backoff = 1000;
    } catch (e) {
      log(`[lane ${lane}] poll failed (${why(e)}), retrying in ${Math.round(backoff / 1000)}s`);
      await sleep(backoff);
      backoff = Math.min(backoff * 2, 30000);
      continue;
    }
    if (!parked.length) continue;

    // Bounded parallelism: one slow read (a breakdown over a big table) must
    // not stall the panel's three-second poll.
    const queue = [...parked];
    const answers = [];
    const workers = Array.from({ length: Math.min(CONCURRENCY, queue.length) }, async () => {
      while (queue.length) {
        const req = queue.shift();
        const out = await forward(req);
        answers.push(out);
        log(`${out.status} ${req.method} ${req.path} ${out.ms}ms`);
      }
    });
    await Promise.all(workers);
    try { await replyTo(answers); }
    catch (e) { log(`could not return ${answers.length} answer(s): ${e.message}`); }
  }
}

async function heartbeatLoop() {
  while (!stopping) {
    try {
      await fetch(`${WORKER_URL}/__agent/status`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', authorization: `Bearer ${KEYS.agentKey}` },
        body: JSON.stringify({ device: DEVICE, origin: LOCAL_ORIGIN, pid: process.pid }),
      });
    } catch { /* the panel will show the gap */ }
    await sleep(HEARTBEAT_MS);
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// --- main -------------------------------------------------------------------
process.on('SIGINT', () => { stopping = true; log('stopping'); process.exit(0); });
process.on('SIGTERM', () => { stopping = true; process.exit(0); });

(async () => {
  log(`agent starting — worker ${WORKER_URL}`);
  log(`forwarding to ${LOCAL_ORIGIN}`);
  try { await ensureRegistered(); } catch (e) { console.error(`could not register with the Worker: ${why(e)}`); process.exit(1); }
  const health = await fetch(`${LOCAL_ORIGIN}/api/health`).then((r) => r.ok).catch(() => false);
  log(health ? 'local collector reachable ✓' : `WARNING: ${LOCAL_ORIGIN}/api/health did not answer`);
  log(`panel: ${WORKER_URL}/  (key in ${path.relative(process.cwd(), KEY_FILE)})`);
  heartbeatLoop();
  // Each poller owns one long-poll to the Worker; they run in parallel so a
  // rendezvous is always parked, whatever the others are busy with.
  await Promise.all(Array.from({ length: POLLERS }, (_, i) => pollLoop(i + 1)));
})();
