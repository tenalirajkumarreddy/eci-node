// schema.js — DDL + the skip-if-complete fast path.
//
// The DDL below is a line-for-line port of render_app/db.py so that this
// process creates *the same ledger the Python fleet uses*, never a parallel
// one. That matters more than it looks: several devices + phones + the web app
// race on one database, so `old_parts` must have one shape and one arbitration
// rule (composite key, `status`, `claimed_by`, `reserved_by/reserved_at`).
//
// `create table if not exists` never touches an existing table, so a complete
// database would still pay for the statements - and on the live store those
// take ACCESS EXCLUSIVE locks, which turned restarts into a race against the
// running collectors (observed as QueryCanceled at startup with a 120s
// statement_timeout on a multi-30k-row old_parts). So, exactly like db.py:
// probe the catalogue first and skip the whole block when every table AND every
// migrated sentinel column is already there.
import { q } from './db.js';
import { config } from './config.js';

const TABLES = ['states', 'acs', 'old_parts', 'electors', 'current_parts',
                'epic_lookups', 'jobs', 'events', 'settings'];

// Columns added by MIGRATIONS after the fact - the version stamp of the schema.
const SENTINELS = [
  ['old_parts', 'old_state_name'], ['old_parts', 'claimed_by'],
  ['old_parts', 'reserved_by'], ['acs', 'ac_type'],
  ['acs', 'discover_started_at'], ['current_parts', 'old_pdf_url'],
  ['current_parts', 'ps_type'],
];

function schemaName() {
  const s = config.schema;
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(s)) {
    throw new Error(`ECI_PG_SCHEMA is not a plain identifier: ${JSON.stringify(s)}`);
  }
  return s;
}

/** Every table and every sentinel column present => the DDL block is skipped. */
export async function schemaOk() {
  const s = schemaName();
  const t = await q(
    `select count(*)::int as n from information_schema.tables
      where table_schema = $1 and table_name = any($2)`, [s, TABLES]);
  if (t[0].n !== TABLES.length) return false;
  const rows = await q(
    `select count(*)::int as n
       from unnest($1::text[], $2::text[]) as sn(tbl, col)
       join information_schema.columns c
         on c.table_schema = $3 and c.table_name = sn.tbl and c.column_name = sn.col`,
    [SENTINELS.map((x) => x[0]), SENTINELS.map((x) => x[1]), s]);
  return rows[0].n === SENTINELS.length;
}

const DDL = (s) => `
create table if not exists ${s}.states (
  state_cd      text primary key,
  name          text,
  has_old_data  boolean default false,
  source        text default 'api',
  last_checked  timestamptz,
  created_at    timestamptz default now()
);

create table if not exists ${s}.acs (
  state_cd        text not null,
  ac_no           integer not null,
  name            text,
  name_l1         text,
  district_cd     text,
  old_parts_found integer default 0,
  discover_status text default 'pending',
  discover_max    integer,
  discovered_at   timestamptz,
  last_error      text,
  created_at      timestamptz default now(),
  primary key (state_cd, ac_no)
);

create table if not exists ${s}.old_parts (
  state_cd       text not null,
  ac_no          integer not null,
  part_no        integer not null,
  name           text,
  exists_        boolean default true,
  status         text not null default 'pending',
  priority       integer default 100,
  attempts       integer default 0,
  last_serial    integer default 0,
  roll_end       integer,
  records        integer default 0,
  epics          integer default 0,
  unmapped       integer default 0,
  cur_part_mode  integer,
  mapping_offset integer,
  started_at     timestamptz,
  finished_at    timestamptz,
  last_error     text,
  reserved_by    text,
  reserved_at    timestamptz,
  created_at     timestamptz default now(),
  updated_at     timestamptz default now(),
  primary key (state_cd, ac_no, part_no)
);
create index if not exists old_parts_queue_idx
  on ${s}.old_parts (status, priority desc, state_cd, ac_no, part_no);
create index if not exists old_parts_cur_idx on ${s}.old_parts (cur_part_mode);
create index if not exists old_parts_neigh_idx
  on ${s}.old_parts (state_cd, ac_no, status, part_no);
create index if not exists old_parts_reserved_idx
  on ${s}.old_parts (reserved_by, reserved_at) where reserved_by is not null;

create table if not exists ${s}.electors (
  source_id     text primary key,
  state_cd      text not null,
  ac_no         integer not null,
  part_no       integer not null,
  serial_no     integer,
  full_name     text,
  full_name_l1  text,
  relative_name text,
  relative_name_l1 text,
  relation_type text,
  gender        text,
  age_snapshot  integer,
  epic_2003     text,
  marked_by_blo text,
  cur_state_cd  text,
  cur_ac_no     integer,
  cur_part_no   integer,
  cur_epic      text,
  first_seen    timestamptz default now(),
  last_seen     timestamptz default now()
);
create index if not exists electors_epic_idx on ${s}.electors (cur_epic);
create index if not exists electors_old_idx on ${s}.electors (state_cd, ac_no, part_no, serial_no);
create index if not exists electors_cur_idx on ${s}.electors (cur_state_cd, cur_ac_no, cur_part_no);
create index if not exists electors_name_idx on ${s}.electors (lower(full_name));

create table if not exists ${s}.current_parts (
  state_cd    text not null,
  ac_no       integer not null,
  part_no     integer not null,
  part_name   text,
  part_name_l1 text,
  part_id     bigint,
  district_cd text,
  fetched_at  timestamptz default now(),
  primary key (state_cd, ac_no, part_no)
);

create table if not exists ${s}.epic_lookups (
  epic          text primary key,
  found         boolean,
  http_status   integer,
  hits          integer,
  name          text,
  name_local    text,
  relation      text,
  relation_local text,
  relation_type text,
  age           integer,
  gender        text,
  state_cd      text,
  state_name    text,
  district      text,
  ac_no         integer,
  ac_name       text,
  part_no       integer,
  part_name     text,
  part_name_l1  text,
  part_id       bigint,
  serial_no     integer,
  section_no    integer,
  ps_building   text,
  ps_building_l1 text,
  record_id     text,
  raw           jsonb,
  fetched_at    timestamptz default now()
);

create table if not exists ${s}.jobs (
  id          bigserial primary key,
  kind        text not null,
  payload     jsonb not null default '{}',
  mode        text default 'manual',
  status      text default 'queued',
  priority    integer default 100,
  progress    jsonb default '{}',
  result      jsonb,
  error       text,
  cancel      boolean default false,
  device      text,
  created_at  timestamptz default now(),
  started_at  timestamptz,
  finished_at timestamptz
);
create index if not exists jobs_queue_idx on ${s}.jobs (status, priority desc, id);
create index if not exists jobs_device_idx on ${s}.jobs (device, status, priority desc, id);

create table if not exists ${s}.events (
  id      bigserial primary key,
  ts      timestamptz default now(),
  level   text default 'info',
  source  text,
  message text,
  device  text
);
create index if not exists events_device_idx on ${s}.events (device, id desc);

create table if not exists ${s}.settings (
  key        text primary key,
  value      jsonb,
  updated_at timestamptz default now()
);

create or replace view ${s}.v_overall as
select
  (select count(*) from ${s}.states)                              as states,
  (select count(*) from ${s}.acs)                                 as acs,
  (select count(*) from ${s}.old_parts)                           as old_parts,
  (select count(*) from ${s}.old_parts where status = 'done')     as done_parts,
  (select count(*) from ${s}.old_parts where status = 'pending')  as pending_parts,
  (select count(*) from ${s}.old_parts where status = 'running')  as running_parts,
  (select count(*) from ${s}.old_parts where status = 'error')    as error_parts,
  (select coalesce(sum(records),0) from ${s}.old_parts)           as records,
  (select coalesce(sum(epics),0) from ${s}.old_parts)             as epics,
  (select count(*) from ${s}.electors)                            as electors,
  (select count(distinct cur_epic) from ${s}.electors
     where cur_epic is not null and cur_epic <> '')              as unique_epics,
  (select count(*) from ${s}.epic_lookups)                        as epic_lookups;

create or replace view ${s}.v_ac_progress as
select state_cd, ac_no,
       count(*)                                as parts,
       count(*) filter (where status='done')   as done,
       count(*) filter (where status='error')  as errors,
       count(*) filter (where status='pending')as pending,
       coalesce(sum(records),0)                as records,
       coalesce(sum(epics),0)                  as epics,
       max(finished_at)                        as last_finished
from ${s}.old_parts
group by 1, 2;
`;

// Additive, idempotent column adds - the same list db.py applies when the
// sentinel probe says the schema is behind.
const MIGRATIONS = (s) => `
alter table ${s}.old_parts add column if not exists old_state_name text;
alter table ${s}.old_parts add column if not exists old_dist_no    text;
alter table ${s}.old_parts add column if not exists old_dist_name  text;
alter table ${s}.old_parts add column if not exists old_ac_name    text;
alter table ${s}.old_parts add column if not exists claimed_by     text;
alter table ${s}.old_parts add column if not exists reserved_by    text;
alter table ${s}.old_parts add column if not exists reserved_at    timestamptz;
alter table ${s}.acs add column if not exists ac_type text;
alter table ${s}.acs add column if not exists discover_started_at timestamptz;
alter table ${s}.current_parts add column if not exists ps_type     text;
alter table ${s}.current_parts add column if not exists ps_caty     text;
alter table ${s}.current_parts add column if not exists old_pdf_url text;
alter table ${s}.events add column if not exists device text;
alter table ${s}.jobs   add column if not exists device text;
`;

/** Create the schema when absent. Returns true when DDL actually ran. */
export async function ensureSchema() {
  const s = schemaName();
  if (await schemaOk()) return false;

  if (s !== 'public') {
    await q(`create schema if not exists ${s}`);
  }
  await q(DDL(s));
  await q(MIGRATIONS(s));

  // The fast path skips DDL on an up-to-date schema, so indexes added after the
  // fact would never appear on a live database. `if not exists` is a cheap
  // catalogue check (it does not rebuild), so ensure them every time - a
  // missing neighbour index makes the picker scan every pending row twice.
  await q(`create index if not exists old_parts_neigh_idx
             on ${s}.old_parts (state_cd, ac_no, status, part_no)`);
  await q(`create index if not exists old_parts_reserved_idx
             on ${s}.old_parts (reserved_by, reserved_at)
           where reserved_by is not null`);
  await q(`create index if not exists events_device_idx
             on ${s}.events (device, id desc)`);
  await q(`create index if not exists jobs_device_idx
             on ${s}.jobs (device, status, priority desc, id)`);
  return true;
}
