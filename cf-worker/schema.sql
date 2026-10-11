-- D1 schema for the panel-proxy mailbox.
-- Apply locally:  npx wrangler d1 execute DB --local --file=./schema.sql
-- Apply remotely: npx wrangler d1 execute DB --remote --file=./schema.sql
-- (the Cloudflare MCP flow runs this same SQL through the D1 query API)

-- Requests parked by the panel, waiting for the local agent to collect them.
create table if not exists mailbox_requests (
  id         text primary key,
  method     text not null,
  path       text not null,   -- pathname + query, exactly as the panel asked
  ctype      text,
  body       text,
  created_at integer not null
);

-- Answers the agent posted back; a waiter deletes its row on pickup.
create table if not exists mailbox_replies (
  id         text primary key,
  status     integer not null,
  ctype      text,
  body       text,
  created_at integer not null
);

-- Last known good answer per "METHOD path?query", served with x-eci-stale: 1
-- when the PC is off, so the panel renders real (if old) numbers instead of
-- an error page.
create table if not exists response_cache (
  key        text primary key,
  status     integer not null,
  ctype      text,
  body       text,
  updated_at integer not null
);

-- Access hashes (panel_key, agent_key) + the agent's heartbeat. Values are
-- SHA-256, so neither key is ever stored in the clear.
create table if not exists agent_state (
  k text primary key,
  v text
);

create index if not exists mailbox_requests_age on mailbox_requests (created_at);
create index if not exists mailbox_replies_age  on mailbox_replies  (created_at);
