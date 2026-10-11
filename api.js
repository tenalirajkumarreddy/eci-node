// api.js — the anonymous ECI gateway client.
//
// One route carries the whole sweep: POST elastic-sir-citizen/get-eroll-data-2003
// on gateway-vha answers with no token at all, and every record comes back with
// its current mapping (bloMappedStateCd/AcNo/PartNo/EpicNo) attached.
//
// Concurrency is shaped only by the AIMD controller: `acquire()` blocks until
// the controller allows another request, `release()` feeds the result back so a
// 429/5xx/timeout halves the limit and a clean answer grows it. Nothing here
// hardcodes a req/s figure.
import { config } from './config.js';
import { Aimd } from './aimd.js';

const VHA = 'https://gateway-vha.eci.gov.in/api/v1/';
export const OLD_EROLL = VHA + 'elastic-sir-citizen/get-eroll-data-2003';
export const EPIC_SEARCH = VHA + 'elastic/search-by-epic-from-national-display-v1';

// Catalogue routes used by the seed actions (catalog.js). The voters web gateway
// is preferred for the AC list: the same 187 ACs as the VHA route but it also
// carries acType (GEN/SC/ST) - verified live 2026-10-09 (200, 187 rows, 898ms).
export const WEB_API = 'https://gateway-voters.eci.gov.in/api/v1/';
export const STATES_API = VHA + 'common/states';
export const ASM_API = VHA + 'citizen/sir/getAsmbly';
export const ASM_API_WEB = WEB_API + 'citizen/sir/getAsmbly';

// The AC-scoped current-roll part list. `getPartByAc` on the voters web gateway
// answers for the AC it is asked about; the VHA route below is NOT AC-scoped
// (asked for AC 1 of S01 it returns 319 rows where the AC has 153 - it answers
// for the district), so its rows are filtered to this AC and the whole answer is
// discarded when the rows name no AC at all.
export const PART_BY_AC = WEB_API + 'citizen/sir/getPartByAc';
export const PART_API = VHA + 'common/part/get/bystatecd/districtcd/acNumber';

// Exact header set the voters.eci.gov.in web app sends on gateway-voters.
export const WEB_HEADERS = {
  Accept: '*/*',
  applicationname: 'VSP',
  channelidobo: 'VSP',
  currentrole: 'citizen',
  'platform-type': 'ECIWEB',
  Origin: 'https://voters.eci.gov.in',
  Referer: 'https://voters.eci.gov.in/',
  'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    + '(KHTML, like Gecko) Chrome/154.0.0.0 Safari/537.36'),
};

// Exact header set the APK sends (measured on the device); the route 404s for
// the browser-flavoured set.
export const APP_HEADERS = {
  'Content-Type': 'application/json',
  Accept: 'application/json',
  applicationName: 'VHA',
  appName: 'VHA',
  channelidobo: 'VHA',
  'platform-type': 'ANDROIDMOB',
  currentRole: 'citizen',
  'User-Agent': 'okhttp/4.9.2',
};

export class HttpError extends Error {
  constructor(message, status, body) {
    super(message);
    this.name = 'HttpError';
    this.status = status;
    this.body = body;
  }
}

export class EciClient {
  constructor(cfg = config, aimd = null) {
    this.cfg = cfg;
    this.aimd = aimd ?? new Aimd({
      start: cfg.startInflight, max: cfg.maxInflight, windowMs: cfg.aimdWindowMs,
    });
    this.stopped = false;
  }

  stop() { this.stopped = true; }

  /**
   * One call through the shared throttle/backoff.
   * Returns { status, body } for any answered response; throws HttpError after
   * the retries are spent (network/timeout) so callers see a failure, not a lie.
   */
  async _fetchJson(url, { method = 'POST', headers = APP_HEADERS, body = null, timeoutMs = null } = {}) {
    const attempts = 3;
    let last = null;
    for (let attempt = 0; attempt < attempts; attempt++) {
      await this.aimd.acquire();
      let res = null;
      try {
        res = await fetch(url, {
          method,
          headers,
          body: body === null ? undefined : JSON.stringify(body),
          signal: AbortSignal.timeout(timeoutMs ?? this.cfg.httpTimeoutMs),
        });
      } catch (e) {
        // Network failure or timeout: no status, so the controller treats it as
        // backpressure and we retry with its (already reduced) limit.
        this.aimd.release(false, 0);
        last = new HttpError(`${e.name}: ${e.message}`, 0, null);
        if (this.stopped) throw last;
        continue;
      }

      const text = await res.text();
      const retryAfter = parseRetryAfter(res.headers.get('retry-after'));
      if (res.status === 429) {
        // Explicit server pushback: pause the whole client, then restart low.
        this.aimd.release(false, 429);
        this.aimd.backoffRetryAfter(retryAfter ?? (attempt + 1) * 2);
        last = new HttpError(`429 rate limited`, 429, text);
        continue;
      }
      if (res.status >= 500) {
        // Real trouble: count it and let the controller back off (honouring a
        // Retry-After if the gateway bothered to send one).
        this.aimd.release(false, res.status);
        if (retryAfter !== null) this.aimd.backoffRetryAfter(retryAfter);
        last = new HttpError(`HTTP ${res.status}`, res.status, text);
        continue;
      }

      // 404 is the old-roll route's ORDINARY answer for a serial past the end
      // of a part - every sweep produces a dozen while measuring where the roll
      // ends - so it must not inflate the error count or halve the limit. See
      // aimd.js: counting it as backpressure pinned the limit at 4-10 while the
      // gateway was happily answering 103 req/s. Only 429/5xx/network count.
      const ok = res.status < 400 || res.status === 404;
      this.aimd.release(ok, res.status);
      let parsed = null;
      try { parsed = text ? JSON.parse(text) : null; } catch { parsed = null; }
      return { status: res.status, body: parsed, raw: text };
    }
    throw last ?? new HttpError('request failed', 0, null);
  }

  /**
   * One serial of an old part -> the record list ([] when the serial is past
   * the roll end). Throws HttpError (with .status) on anything else, which is
   * how the sweep tells "roll ended" (404) from "transport trouble" (5xx).
   *
   * serial '' is the discovery window: ~50 records at a random offset, used to
   * learn the part without knowing where it starts.
   */
  async getEroll(stateCd, acNo, partNo, serial) {
    const body = {
      oldStateCd: String(stateCd),
      oldAcNo: String(acNo),
      oldPartNo: String(partNo),
      oldPartSerialNo: String(serial),
    };
    const { status, body: payload, raw } = await this._fetchJson(OLD_EROLL, { body });
    if (status === 404) throw new HttpError(`404 no serial ${serial}`, 404, raw);
    if (status !== 200) {
      throw new HttpError(`HTTP ${status}`, status, raw);
    }
    if (Array.isArray(payload)) return payload;
    if (Array.isArray(payload?.payload)) return payload.payload;
    return [];
  }
}

function parseRetryAfter(v) {
  if (!v) return null;
  const secs = Number(v);
  if (Number.isFinite(secs)) return Math.max(0, secs);
  const when = Date.parse(v);
  if (Number.isFinite(when)) return Math.max(0, (when - Date.now()) / 1000);
  return null;
}

// ------------------------------------------------------------ label mapping
//
// The old-roll route returns single-letter relation codes. Resolved against the
// national search for the same person (12/12 agreement on F->FTHR, H->HSBN,
// M->MTHR, O->OTHR, with the relative's name matching too), so the mapping is
// measured rather than assumed. The old roll never emitted a code outside these
// four across 50k collected rows.
export const RELATION_TYPES = {
  F: ['FTHR', 'Father'],
  H: ['HSBN', 'Husband'],
  M: ['MTHR', 'Mother'],
  O: ['OTHR', 'Other'],
};
export const GENDER_TYPES = { M: 'Male', F: 'Female', T: 'Third gender', O: 'Other' };

/** 'M' -> 'Mother' (or 'MTHR' when short). Unknown codes pass through, so a
 * new code the API starts returning shows up instead of disappearing. */
export function relationLabel(code, short = false) {
  if (code === null || code === undefined || code === '') return null;
  const hit = RELATION_TYPES[String(code).trim().toUpperCase()];
  if (!hit) return code;
  return short ? hit[0] : hit[1];
}

export function genderLabel(code) {
  if (code === null || code === undefined || code === '') return null;
  return GENDER_TYPES[String(code).trim().toUpperCase()] ?? code;
}

// ------------------------------------------------------------ catalog routes
//
// Ported from client.py's ac_type_label/_ac_row/states_live/acs_live so the seed
// actions write exactly the values the Python fleet would have written: same
// normalisation, same "unknown codes pass through" rule.

export const AC_TYPES = {
  GEN: 'GEN', GENERAL: 'GEN', G: 'GEN', UR: 'GEN', UNRESERVED: 'GEN',
  SC: 'SC', '(SC)': 'SC', '\u0905.\u091C.\u091C\u093E.': 'SC',
  ST: 'ST', '(ST)': 'ST', '\u0905.\u091C\u093E.': 'ST',
};

/** 'general' -> 'GEN'. Values with no known mapping are passed through
 * unchanged, so a new spelling shows up instead of being silently coerced. */
export function acTypeLabel(code) {
  if (code === null || code === undefined || String(code).trim() === '') return null;
  const raw = String(code).trim();
  return AC_TYPES[raw.toUpperCase()] ?? raw;
}

/** The payload array out of either shape the gateways answer with. */
function rowsOf(data) {
  if (Array.isArray(data)) return data;
  if (data && typeof data === 'object') {
    for (const k of ['payload', 'data', 'content']) {
      if (Array.isArray(data[k])) return data[k];
    }
  }
  return [];
}

function acRow(row) {
  if (!row || typeof row !== 'object') return null;
  const ac = row.acNo ?? row.asmblyNo ?? row.ac_number ?? row.assemblyNo;
  if (ac === null || ac === undefined || ac === '') return null;
  const dist = row.distNo ?? row.districtCd ?? row.district_cd;
  return {
    ac_no: Number(ac),
    name: row.acNameV1 ?? row.acName ?? row.ac_name ?? null,
    name_l1: row.acName ?? null,
    ac_type: acTypeLabel(row.acType ?? row.category ?? null),
    district_cd: dist === null || dist === undefined ? null : String(dist),
  };
}

/** Live state list (anonymous, no token). Returns [] on any failure. */
export async function statesLive({ timeoutMs = 25_000 } = {}) {
  try {
    const res = await fetch(STATES_API, {
      headers: APP_HEADERS, signal: AbortSignal.timeout(timeoutMs),
    });
    if (res.status !== 200) return [];
    const rows = rowsOf(await res.json());
    const out = [];
    for (const row of rows) {
      const cd = row?.stateCd ?? row?.stateCode ?? row?.state_cd;
      if (!cd) continue;
      out.push({ state_cd: String(cd), name: row?.stateName ?? row?.name ?? null });
    }
    return out;
  } catch {
    return [];
  }
}

/**
 * Live AC list for one state, normalised to the columns `acs` stores.
 *
 * The web gateway is tried first (it carries acType and is the service
 * voters.eci.gov.in itself uses); the VHA route is the fallback. Empty result
 * means "no answer", never "this state has no ACs" - the caller keeps whatever
 * the legacy catalogue already gave it.
 */
export async function acsLive(stateCd, { timeoutMs = 25_000 } = {}) {
  const url = `${ASM_API_WEB}?state=${encodeURIComponent(stateCd)}`;
  try {
    const res = await fetch(url, {
      headers: { ...WEB_HEADERS, state: String(stateCd) },
      signal: AbortSignal.timeout(timeoutMs),
    });
    if (res.status === 200) {
      const out = rowsOf(await res.json()).map(acRow).filter(Boolean);
      if (out.length) return out;
    }
  } catch { /* fall through to the app route */ }

  try {
    const res = await fetch(`${ASM_API}?state=${encodeURIComponent(stateCd)}`, {
      headers: { ...APP_HEADERS, state: String(stateCd) },
      signal: AbortSignal.timeout(timeoutMs),
    });
    if (res.status !== 200) return [];
    return rowsOf(await res.json()).map(acRow).filter(Boolean);
  } catch {
    return [];
  }
}

/**
 * Current-roll parts of one AC, normalised to the columns `current_parts` stores.
 *
 * Ported from client.py's current_parts, including its fallback rule: the web
 * route is preferred because it is AC-scoped and is the service
 * voters.eci.gov.in itself uses; the VHA route answers for the whole district, so
 * it is only trusted when every row names this AC. Storing the district's list is
 * what once made S01 AC 1 look like it had 319 parts when it has 153.
 *
 * Empty means "no answer" - the caller keeps whatever is already cached.
 */
export async function currentPartsLive(stateCd, acNo, { timeoutMs = 30_000 } = {}) {
  const norm = (x) => {
    const dist = x.distNo ?? x.districtCd ?? x.district_cd ?? null;
    return {
      part_no: x.partNumber ?? x.part_no ?? null,
      part_name: x.partName ?? null,
      part_name_l1: x.partNameV1 ?? x.partNameL1 ?? null,
      part_id: x.id ?? x.partId ?? null,
      district_cd: dist === null || dist === undefined ? null : String(dist),
      ps_type: x.psType ?? null,
      ps_caty: x.psCaty ?? null,
      old_pdf_url: x.oldPdfUrl ?? null,
    };
  };
  const usable = (rows) => rows.filter((r) => r.part_no !== null && r.part_no !== undefined && r.part_no !== '');
  const objects = (list) => list.filter((r) => r && typeof r === 'object');

  try {
    const res = await fetch(`${PART_BY_AC}?Asmbly=${encodeURIComponent(acNo)}`, {
      headers: { ...WEB_HEADERS, state: String(stateCd) },
      signal: AbortSignal.timeout(timeoutMs),
    });
    if (res.status === 200) {
      const out = usable(objects(rowsOf(await res.json())).map(norm));
      if (out.length) return out;
    }
  } catch { /* fall through to the app route */ }

  try {
    const res = await fetch(
      `${PART_API}?stateCd=${encodeURIComponent(stateCd)}&acNumber=${encodeURIComponent(acNo)}`,
      { headers: { ...APP_HEADERS, state: String(stateCd) }, signal: AbortSignal.timeout(timeoutMs) },
    );
    if (res.status !== 200) return [];
    const raw = objects(rowsOf(await res.json()));
    if (!raw.length) return [];
    const acOf = (x) => x.acNumber ?? x.ac_no ?? null;
    if (!raw.every((x) => acOf(x) !== null)) return [];
    return usable(raw.filter((x) => String(acOf(x)) === String(acNo)).map(norm));
  } catch {
    return [];
  }
}
