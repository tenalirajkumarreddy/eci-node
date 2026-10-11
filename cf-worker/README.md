# ECI panel on Cloudflare Workers

Your `http://127.0.0.1:8008/admin` panel, reachable from anywhere, without the
database ever being exposed to the internet.

```
 browser ──▶ Worker (/admin, /api/...) ──▶ [D1 mailbox] ──▶ agent.js ──▶ 127.0.0.1:8008
 browser ◀── 200, byte-identical    ◀── [D1 mailbox] ◀── agent.js ◀── your real app
```

## Why a reverse proxy instead of a "snapshot dashboard"

The panel is *interactive*: it drills state → AC → part → elector and its buttons
write (`seed parts`, `collect`, `sweep`, `cancel job`). A precomputed snapshot
cannot answer an arbitrary query string, and a Worker cannot open a TCP
connection to your PC. So the Worker parks each request in a D1 mailbox; the
local agent picks it up, forwards it to your own collector and posts the real
answer back.

Two consequences worth knowing:

- **The panel is byte-identical** because your app is answering it — same
  `admin.html`, same JSON, same writes. `GET /admin` returns the same 64,398
  bytes the local server serves.
- **The DB credential never leaves your PC.** The Worker stores only opaque
  request/response bytes plus SHA-256 hashes of the two access keys. There is no
  connection string, no secret binding, and nothing to put in Cloudflare's
  dashboard.

## Files

| file | what it is |
|---|---|
| `worker.js` | the Cloudflare Worker (zero dependencies) |
| `agent.js` | the local bridge; run it next to the collector (zero dependencies) |
| `schema.sql` | the D1 tables (mailbox, replies, cache, agent state) |
| `wrangler.jsonc` | bindings + config for `wrangler dev` / `wrangler deploy` |
| `.agent-keys.json` | generated on first run — the panel key and agent key (gitignored) |

## Running it

**1. Point the agent at the Worker.** Add one line to the collector's `.env`:

```
ECI_WORKER_URL=https://eci-node.<your-subdomain>.workers.dev
```

**2. Start the agent** (it needs the collector running, because that is what it
forwards to):

```
node cf-worker/agent.js
```

On the first run it generates two random keys, registers their hashes with the
Worker, and prints:

```
Panel URL (open once, it sets a cookie):
https://eci-node.<your-subdomain>.workers.dev/?k=<panel key>
```

Open that once. The Worker swaps `?k=` for an `HttpOnly` cookie and redirects, so
the key leaves the address bar. After that `https://<worker>/admin` just works
from any browser, phone, or laptop.

**3. Keep it alive.** The listener only answers while the agent is running. On
Windows, either leave the terminal open or run it as a scheduled task at logon:

```
schtasks /create /tn "ECI panel agent" /sc onlogon /tr "node \"C:\Users\rajku\Documents\eci node\cf-worker\agent.js\""
```

## Configuration (all optional except `ECI_WORKER_URL`)

| variable | default | meaning |
|---|---|---|
| `ECI_WORKER_URL` | — | the deployed Worker, e.g. `https://eci-node.you.workers.dev` |
| `ECI_LOCAL_ORIGIN` | `http://127.0.0.1:8008` | what the agent forwards to |
| `ECI_AGENT_WAIT` | `20` | seconds the Worker holds the agent's long-poll open |
| `ECI_AGENT_CONCURRENCY` | `4` | parallel local requests |

## Deploying / updating the Worker

```
cd cf-worker
npx wrangler login          # once; or set CLOUDFLARE_API_TOKEN
npx wrangler d1 execute DB --remote --file=./schema.sql   # idempotent
npx wrangler deploy
```

`wrangler deploy` bundles nothing (there are no dependencies) and uploads
`worker.js` byte-for-byte, which is why it is used instead of pasting code into
an API call.

## Endpoints the Worker adds

| route | who calls it | what it does |
|---|---|---|
| `/__health` | anyone | liveness of the Worker itself |
| `/__status` | the panel | `{online, last_seen, age_secs, queued, device, origin}` — is the PC feeding this? |
| `/__agent/poll` | agent | long-poll for parked requests (bearer-protected) |
| `/__agent/reply` | agent | post answers back (bearer-protected) |
| `/__agent/bootstrap` | agent | one-time key registration (only while unregistered) |
| `/__agent/status` | agent | heartbeat every 15s |

## Two honest limits

1. **The PC must be on and the agent running.** The Worker is a window onto your
   collector, not a copy of it. When the agent has been silent for 45s, panel
   requests answer immediately from the last known good response (marked
   `x-eci-stale: 1`, 60s window) and otherwise return a clear
   `504 {"error":"the local collector did not answer"}` instead of hanging.
2. **Latency.** Each panel request costs one round trip to your PC plus one poll
   tick (~0.3–0.5s on top of however long the collector itself takes — and
   `/api/summary` alone is ~1.5s against the remote Postgres). The panel polls
   every 3s, so it stays live; it just is not sub-100ms.

The collector's own long-running work is unaffected either way: it keeps sweeping
on the PC, because a Worker cannot run a background loop.
