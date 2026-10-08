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
