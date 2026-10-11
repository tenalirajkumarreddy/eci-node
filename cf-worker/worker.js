// worker.js — Cloudflare Worker that serves the ECI panel by proxying every
// request to the local collector.
//
// WHY THIS SHAPE
// --------------
// The panel (admin.html) is interactive: it drills into state -> AC -> part and
// its buttons write. A precomputed "snapshot" cannot answer an arbitrary query
// string, and a Worker cannot open a connection to the collector's PC. So this
// Worker is a REVERSE PROXY: the panel's request is handed to a Durable Object,
// where it is matched with the long-poll of the local agent (agent.js, running
// next to the collector). The agent forwards it to 127.0.0.1:8008 and posts the
// real answer back. The panel is byte-identical because the real app answers.
//
//   panel ──▶ Worker ──▶ PanelHub (DO) ◀── agent ◀── 127.0.0.1:8008
//   panel ◀── 200 JSON ◀── PanelHub (DO) ◀── agent ◀── real response
//
// The rendezvous is a pair of held promises inside the DO — NOT a poll loop.
// That is deliberate: a long-running Worker request that sleeps burns time on
// every tick, and Cloudflare throttles those timers, which produced multi-second
// stalls in the polling version. Holding a promise costs nothing until either
// side actually arrives, so a click costs one round trip.
//
// The database credential never leaves the PC: the Worker stores only opaque
// request/response bytes, a last-known-good cache, and two SHA-256 hashes.
//
// BINDINGS
//   DB   — D1 (cache + access hashes + heartbeats)
//   HUB  — Durable Object, class PanelHub (the rendezvous)

const JSON_CT = 'application/json';
const PANEL_WAIT_MS = 30000;    // how long a panel request waits for the agent
// How long the agent's poll is held open. Deliberately SHORT: a Durable Object
// does not interleave a plain held promise with other events, so a long hold
// blocks every panel request behind it for its whole duration (measured: 25s
// with a 25s hold). A short hold bounds that wait, and the agent re-polls
// immediately, so a parked request is picked up one round trip after the hold
// expires. The real fix is a hibernatable WebSocket instead of a held fetch.
const AGENT_HOLD_MS = 3000;
const CACHE_TTL_MS = 60000;     // serve-last-known-good window when the agent is away
const AGENT_OFFLINE_MS = 45000; // three missed 15s heartbeats = treat as offline

// ---------------------------------------------------------------- utilities

const b64url = (buf) =>
  btoa(String.fromCharCode(...new Uint8Array(buf)))
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');

const sha256 = async (s) =>
  b64url(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(s)));

const randId = () => crypto.randomUUID().replace(/-/g, '');

const json = (body, status = 200, extra = {}) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': JSON_CT, 'cache-control': 'no-store', ...extra },
  });

function parseCookies(req) {
  const out = {};
  for (const part of (req.headers.get('cookie') || '').split(';')) {
    const i = part.indexOf('=');
    if (i > 0) out[part.slice(0, i).trim()] = part.slice(i + 1).trim();
  }
  return out;
}

// ------------------------------------------------------- the rendezvous DO
//
// One instance ('main') serves the whole panel. It holds:
//   waiting  — panel requests parked until the agent answers (or they expire)
//   agents   — agent polls parked until a panel request shows up
// Both are plain promises, so neither side burns runtime while idle.

export class PanelHub {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.waiting = new Map();   // id -> { request, resolve }
    this.agents = [];           // resolver queue for agent long-polls
  }

  async fetch(req) {
    const path = new URL(req.url).pathname;
    if (path === '/park') return this.park(await req.json());
    if (path === '/take') return this.take();
    if (path === '/reply') return this.reply(await req.json());
    if (path === '/depth') {
      return json({ waiting: this.waiting.size, agents: this.agents.length });
    }
    return json({ error: 'not found' }, 404);
  }

  /** A panel request arrives: park it until the agent answers. */
  park(entry) {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.waiting.delete(entry.id);
        resolve(json({ timeout: true }, 504, { 'x-eci-timeout': '1' }));
      }, PANEL_WAIT_MS);
      this.waiting.set(entry.id, { ...entry, resolve, timer });
      this.dispatch();
    });
  }

  /** An agent poll arrives: hand it whatever is parked, or hold it open. */
  take() {
    return new Promise((resolve) => {
      const waiter = { resolve, timer: null };
      waiter.timer = setTimeout(() => {
        const i = this.agents.indexOf(waiter);
        if (i >= 0) this.agents.splice(i, 1);
        resolve(json({ requests: [] }));
      }, AGENT_HOLD_MS);
      this.agents.push(waiter);
      this.dispatch();
    });
  }

  /** Match parked requests with a waiting agent, oldest first. */
  dispatch() {
    while (this.agents.length && this.waiting.size) {
      const entries = [...this.waiting.values()].filter((e) => !e.sent);
      if (!entries.length) return;               // everything already handed out
      const waiter = this.agents.shift();
      clearTimeout(waiter.timer);
      for (const e of entries) e.sent = true;
      waiter.resolve(json({
        requests: entries.map((e) => ({
          id: e.id, method: e.method, path: e.path, ctype: e.ctype, body: e.body,
        })),
      }));
    }
  }

  /** The agent's answer: release the parked panel request. */
  async reply(result) {
    const r = result && result;
    const entry = r && this.waiting.get(r.id);
    if (!entry) return json({ ok: false, reason: 'no such waiter' }, 404);
    clearTimeout(entry.timer);
    this.waiting.delete(r.id);
    const body = typeof r.body === 'string' ? r.body : JSON.stringify(r.body ?? null);
    entry.resolve(new Response(body, {
      status: Number(r.status) || 502,
      headers: {
        'content-type': r.ctype || 'text/plain; charset=utf-8',
        'cache-control': 'no-store',
      },
    }));
    return json({ ok: true });
  }
}

// ------------------------------------------------------------- agent routes

/** GET /__agent/poll — hand the agent any parked requests. The DO holds the
 * poll open, so work is picked up the moment it arrives. */
async function agentPoll(env, url) {
  const id = env.HUB.idFromName('main');
  return env.HUB.get(id).fetch(new Request('https://hub/take', { method: 'POST' }));
}

/** POST /__agent/reply — one answer, or a batch of them. */
async function agentReply(env, req) {
  const body = await req.json().catch(() => null);
  const results = body && Array.isArray(body.results) ? body.results : [];
  if (!results.length) return json({ stored: 0 });
  const id = env.HUB.idFromName('main');
  const hub = env.HUB.get(id);
  let ok = 0;
  for (const r of results) {
    if (!r || !r.id) continue;
    const res = await hub.fetch(new Request('https://hub/reply', {
      method: 'POST', headers: { 'content-type': JSON_CT }, body: JSON.stringify(r),
    }));
    if (res.ok) ok += 1;
  }
  return json({ stored: ok });
}

/** POST /__agent/bootstrap — one-time registration of the panel key and the
 * agent key. Both are generated on the PC and sent here once; the Worker stores
 * only their SHA-256, so neither ever has to exist as a Cloudflare secret. */
async function agentBootstrap(env, req) {
  const existing = await env.DB.prepare("select v from agent_state where k = 'panel_key'").first();
  if (existing && existing.v) return json({ ok: false, error: 'already bootstrapped' }, 409);
  const body = await req.json().catch(() => ({}));
  const panelKey = String(body.panel_key || '');
  const agentKey = String(body.agent_key || '');
  if (panelKey.length < 16 || agentKey.length < 16) {
    return json({ ok: false, error: 'keys must be at least 16 characters' }, 400);
  }
  await env.DB.batch([
    env.DB.prepare("insert or replace into agent_state (k, v) values ('panel_key', ?)").bind(await sha256(panelKey)),
    env.DB.prepare("insert or replace into agent_state (k, v) values ('agent_key', ?)").bind(await sha256(agentKey)),
    env.DB.prepare("insert or replace into agent_state (k, v) values ('bootstrapped_at', ?)").bind(String(Date.now())),
  ]);
  return json({ ok: true });
}

/** POST /__agent/status — heartbeat, so the panel can say whether the PC is on. */
async function agentStatus(env, req) {
  const body = await req.json().catch(() => ({}));
  const now = Date.now();
  await env.DB.batch([
    env.DB.prepare("insert or replace into agent_state (k, v) values ('last_seen', ?)").bind(String(now)),
    env.DB.prepare("insert or replace into agent_state (k, v) values ('last_seen_info', ?)")
      .bind(JSON.stringify({ device: body.device || null, origin: body.origin || null })),
  ]);
  return json({ ok: true });
}

/** GET /__status — "is my PC feeding this?" for the panel (never proxied). */
async function panelStatus(env) {
  const rows = await env.DB
    .prepare("select k, v from agent_state where k in ('last_seen', 'last_seen_info')")
    .all();
  const map = Object.fromEntries((rows.results || []).map((r) => [r.k, r.v]));
  const last = Number(map.last_seen || 0);
  let info = {};
  try { info = JSON.parse(map.last_seen_info || '{}'); } catch { /* ignore */ }
  const id = env.HUB.idFromName('main');
  let depth = { waiting: null, agents: null };
  try {
    depth = await (await env.HUB.get(id).fetch(new Request('https://hub/depth'))).json();
  } catch { /* the DO will spin up on the next request */ }
  return json({
    online: last > 0 && Date.now() - last < AGENT_OFFLINE_MS,
    last_seen: last ? new Date(last).toISOString() : null,
    age_secs: last ? Math.round((Date.now() - last) / 1000) : null,
    ...depth,
    ...info,
  });
}

// ------------------------------------------------------------- panel proxy

/** Has the agent heartbeated recently? */
async function agentSeenRecently(env) {
  const row = await env.DB.prepare("select v from agent_state where k = 'last_seen'").first();
  const last = Number((row && row.v) || 0);
  return last > 0 && Date.now() - last < AGENT_OFFLINE_MS;
}

/** Answer from the last known good copy of this exact query, if we have one. */
async function serveCached(env, key) {
  const cached = await env.DB
    .prepare('select status, ctype, body, updated_at from response_cache where key = ?')
    .bind(key).first();
  if (!cached || Date.now() - cached.updated_at >= CACHE_TTL_MS) return null;
  return new Response(cached.body, {
    status: cached.status,
    headers: {
      'content-type': cached.ctype || 'text/plain; charset=utf-8',
      'cache-control': 'no-store',
      'x-eci-stale': '1',
      'x-eci-cached-at': new Date(cached.updated_at).toISOString(),
    },
  });
}

async function cacheIt(env, key, res, body) {
  if (!(res.status >= 200 && res.status < 400)) return;
  await env.DB.prepare(
    'insert into response_cache (key, status, ctype, body, updated_at) values (?,?,?,?,?) ' +
    'on conflict(key) do update set status=excluded.status, ctype=excluded.ctype, body=excluded.body, updated_at=excluded.updated_at',
  ).bind(key, res.status, res.headers.get('content-type') || '', body, Date.now()).run();
}

const agentOffline = (path) => json({
  error: 'the local collector did not answer',
  detail: 'agent.js is not running, or the PC is offline. The panel needs the machine that runs the collector.',
  path,
}, 504, { 'x-eci-agent': 'offline' });

/** Park one panel request in the DO and wait for the agent's answer. */
async function proxy(env, req, url) {
  const key = `${req.method} ${url.pathname}${url.search}`;

  // Do not park work for a PC that has stopped heartbeating: answer from the
  // cache immediately instead of making the panel wait PANEL_WAIT_MS per
  // request and piling an outage up behind it.
  if (!(await agentSeenRecently(env))) {
    return (await serveCached(env, key)) || agentOffline(key);
  }

  const id = randId();
  let body = '';
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    body = await req.text().catch(() => '');
  }
  const entry = {
    id,
    method: req.method,
    path: `${url.pathname}${url.search}`,
    ctype: req.headers.get('content-type') || '',
    body,
  };
  const hubId = env.HUB.idFromName('main');
  const res = await env.HUB.get(hubId).fetch(new Request('https://hub/park', {
    method: 'POST', headers: { 'content-type': JSON_CT }, body: JSON.stringify(entry),
  }));

  if (res.headers.get('x-eci-timeout')) {
    return (await serveCached(env, key)) || agentOffline(key);
  }
  if (req.method === 'GET' && res.ok) {
    // Keep the answer so the panel still renders during a short outage.
    const text = await res.text();
    await cacheIt(env, key, res, text);
    return new Response(text, { status: res.status, headers: res.headers });
  }
  return res;
}

// ---------------------------------------------------------------- access gate

const LOGIN_PAGE = `<!doctype html><meta charset="utf-8"><title>ECI panel</title>
<style>body{font:15px/1.5 system-ui,sans-serif;margin:0;display:grid;place-items:center;min-height:100vh;background:#0b1020;color:#e6e8ef}
form{background:#141a2e;padding:28px 30px;border-radius:14px;border:1px solid #253052;min-width:320px}
h1{font-size:17px;margin:0 0 4px}p{color:#8e98b5;margin:0 0 18px;font-size:13px}
input{width:100%;padding:10px 12px;border-radius:8px;border:1px solid #2b3760;background:#0e1428;color:#e6e8ef;box-sizing:border-box}
button{margin-top:14px;width:100%;padding:10px;border:0;border-radius:8px;background:#3b6ef5;color:#fff;font-weight:600;cursor:pointer}</style>
<form method="get"><h1>ECI collector panel</h1><p>Paste the panel key from your local agent.</p>
<input name="k" placeholder="panel key" autofocus autocomplete="off"><button>Open panel</button></form>`;

/** Returns a Response when the request is NOT authorised, else null. */
async function gate(env, req, url) {
  const expected = await env.DB.prepare("select v from agent_state where k = 'panel_key'").first();
  if (!expected || !expected.v) {
    return json({
      error: 'not bootstrapped',
      detail: 'Run the local agent once (node cf-worker/agent.js) to register the panel key.',
    }, 503);
  }
  const supplied = url.searchParams.get('k');
  if (supplied) {
    if (await sha256(supplied) === expected.v) {
      const clean = new URL(url);
      clean.searchParams.delete('k');
      const cookie = `eci_key=${supplied}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=2592000`;
      // A GET turns the ?k= into a cookie and gets the key out of the URL bar
      // (and out of the Referer header) without breaking the original target.
      return new Response(null, { status: 302, headers: { location: clean.pathname + clean.search, 'set-cookie': cookie } });
    }
    return new Response(LOGIN_PAGE, { status: 401, headers: { 'content-type': 'text/html; charset=utf-8' } });
  }
  const cookie = parseCookies(req).eci_key;
  if (cookie && await sha256(cookie) === expected.v) return null;
  return new Response(LOGIN_PAGE, { status: 401, headers: { 'content-type': 'text/html; charset=utf-8' } });
}

// ----------------------------------------------------------------- dispatch

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const p = url.pathname;

    try {
      if (p.startsWith('/__agent/')) {
        const agentHash = await env.DB.prepare("select v from agent_state where k = 'agent_key'").first();
        const supplied = (req.headers.get('authorization') || '').replace(/^Bearer\s+/i, '');
        const bootstrap = p === '/__agent/bootstrap';
        if (!bootstrap && (!agentHash || !agentHash.v || await sha256(supplied) !== agentHash.v)) {
          return json({ error: 'unauthorised agent' }, 401);
        }
        if (p === '/__agent/bootstrap' && req.method === 'POST') return agentBootstrap(env, req);
        if (p === '/__agent/poll' && req.method === 'GET') return agentPoll(env, url);
        if (p === '/__agent/reply' && req.method === 'POST') return agentReply(env, req);
        if (p === '/__agent/status' && req.method === 'POST') return agentStatus(env, req);
        return json({ error: 'not found' }, 404);
      }

      if (p === '/__health') return json({ ok: true, service: 'eci-panel-proxy' });
      // The panel polls this to draw its "collector reachable" badge; it must
      // stay readable without the agent, so it is not proxied.
      if (p === '/__status') return panelStatus(env);

      const denied = await gate(env, req, url);
      if (denied) return denied;

      // '/admin' is the panel; '/' is the device dashboard. Both are proxied.
      return await proxy(env, req, url);
    } catch (e) {
      return json({ error: 'worker error', detail: String(e && e.message || e) }, 500);
    }
  },
};
