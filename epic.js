// epic.js — the APK's own captcha-less EPIC search, ported from work/app_search.py.
//
// Wire contract (from the smali, PollingStationSearchActivity.callSearchApiTrial):
//   POST gateway-vha .../elastic/search-by-epic-from-national-display-v1
//   body {"encryptedKey": b64(RSA-OAEP-SHA256(aesKey)),
//         "iv": b64(12-byte GCM nonce),
//         "encryptedPayload": b64(AES-256-GCM(json))}
//
//   securityKey = b64(AESGCM(key = b64decode(tc), iv = 16 zero bytes,
//                          plaintext = "<EPIC>:<yyyy-MM-dd-HH-mm-ss>:<6 digits>"))
//
// Response semantics: 200 with records = found; 200 [] = not in the national
// display; 400 [] = the request was rejected (wrong tc); 500 = bad envelope.
//
// Only node:crypto is used, so the project keeps its one-dependency promise.
import fs from 'node:fs';
import { createCipheriv, createPublicKey, publicEncrypt, randomBytes, randomUUID, constants } from 'node:crypto';
import { config } from './config.js';
import { EPIC_SEARCH, APP_HEADERS, relationLabel } from './api.js';
import { q, logEvent } from './db.js';

const MOBILE_UA = 'Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 '
  + '(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36';

const b64e = (buf) => Buffer.from(buf).toString('base64');

/** android.util.Base64.DEFAULT decode is lenient about padding. */
export function b64d(text) {
  const t = String(text ?? '').trim();
  const padded = t + '='.repeat((4 - (t.length % 4)) % 4);
  return Buffer.from(padded, 'base64');
}

/** Base64 unwrap levels of a native constant: BaseActivity wraps every getter
 * as `new String(Base64.decode(value))` and callers decode again, so an
 * extracted constant may be 1-3 layers deep. */
function decodeVariants(text) {
  const out = [];
  let current = String(text ?? '').trim();
  for (let i = 0; i < 3; i++) {
    let raw;
    try { raw = b64d(current); } catch { break; }
    out.push(raw);
    const ascii = raw.toString('ascii');
    if (!/^[A-Za-z0-9+/=]+$/.test(ascii)) break;
    current = ascii.trim();
  }
  return out;
}

/** tc -> the raw AES key (16/24/32 bytes), tolerating the native double-wrap. */
export function decodeTc(tc) {
  for (const raw of decodeVariants(tc)) {
    if ([16, 24, 32].includes(raw.length)) return raw;
  }
  throw new Error('tc does not decode to a 16/24/32-byte AES key');
}

/** external -> the RSA public key, tolerating the native double-wrap. */
export function loadPublicKey(external) {
  for (const der of decodeVariants(external)) {
    if (der.length < 64) continue;
    for (const type of ['spki', 'pkcs1']) {
      try {
        const key = createPublicKey({ key: der, format: 'der', type });
        if (key.asymmetricKeyType === 'rsa') return key;
      } catch { /* try the next encoding */ }
    }
  }
  throw new Error('external is not a Base64 X.509/SPKI RSA public key');
}

function pad2(n) { return String(n).padStart(2, '0'); }

/** Python's AESGCM picks the variant from the key length, and `tc` is a
 * 16-byte key, so the app is really doing AES-128-GCM here. node:crypto wants
 * the variant named explicitly, hence this mapping. */
function gcm(key, iv) {
  const alg = { 16: 'aes-128-gcm', 24: 'aes-192-gcm', 32: 'aes-256-gcm' }[key.length];
  if (!alg) throw new Error(`unsupported AES key length ${key.length}`);
  return createCipheriv(alg, key, iv);
}

/** KGn.gPK — the per-request securityKey. Local time, exactly like SimpleDateFormat. */
export function gpk(primary, tc) {
  const d = new Date();
  const ts = `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`
    + `-${pad2(d.getHours())}-${pad2(d.getMinutes())}-${pad2(d.getSeconds())}`;
  let rand6 = '';
  for (let i = 0; i < 6; i++) rand6 += '1234567890'[randomBytes(1)[0] % 10];
  const plaintext = Buffer.from(`${primary}:${ts}:${rand6}`, 'utf8');
  const key = decodeTc(tc);
  const cipher = gcm(key, Buffer.alloc(16));          // 16 zero bytes
  const ct = Buffer.concat([cipher.update(plaintext), cipher.final(), cipher.getAuthTag()]);
  return b64e(ct);
}

/** AKgn.encryptData — random AES-256-GCM key, wrapped with RSA-OAEP-SHA256. */
export function encryptData(payload, publicKey) {
  const aesKey = randomBytes(32);
  const iv = randomBytes(12);
  const cipher = gcm(aesKey, iv);
  const ct = Buffer.concat([cipher.update(payload), cipher.final(), cipher.getAuthTag()]);
  const wrapped = publicEncrypt(
    { key: publicKey, padding: constants.RSA_PKCS1_OAEP_PADDING, oaepHash: 'sha256' },
    aesKey,
  );
  return { encryptedKey: b64e(wrapped), iv: b64e(iv), encryptedPayload: b64e(ct) };
}

/** TApiClient.getRetroProdClient attaches DecryptionInterceptor, which adds
 * `device-id` (a random UUID unless Firebase gives one). */
export function requestHeaders() {
  return { ...APP_HEADERS, 'User-Agent': MOBILE_UA, 'device-id': randomUUID() };
}

function loadKeys(keysPath = config.appKeysPath) {
  const data = JSON.parse(fs.readFileSync(keysPath, 'utf8'));
  const tc = data.tc || '';
  const external = data.external || '';
  if (!tc || !external) throw new Error(`missing tc/external in ${keysPath}`);
  return { tc, external };
}

export function buildRequest(epic, tc, external) {
  const inner = {
    captchaData: 'na',          // TElasticSearchRequest ctor default
    captchaId: 'na',            // TElasticSearchRequest ctor default
    epicNumber: epic,
    securityKey: gpk(epic, tc),
  };
  return { inner, body: encryptData(Buffer.from(JSON.stringify(inner), 'utf8'), loadPublicKey(external)) };
}

// ------------------------------------------------------------------ processor
//
// The national display answers at ~1 req/s, so lookups are serialised across
// the whole process with a minimum gap (the Python side uses one lock + one
// last-call timestamp for the same reason).
let chain = Promise.resolve();
let lastAt = 0;

export class EpicProcessor {
  constructor(cfg = config) {
    this.cfg = cfg;
    this.keys = null;
  }

  keysOnce() {
    if (!this.keys) this.keys = loadKeys(this.cfg.appKeysPath);
    return this.keys;
  }

  /** Search one EPIC, store the result in epic_lookups, return it. */
  async lookup(epicRaw) {
    const epic = String(epicRaw).split(/\s+/).join('').toUpperCase();
    if (!epic) throw new Error('epic required');
    return this._serialise(() => this._lookup(epic));
  }

  _serialise(fn) {
    const run = chain.then(async () => {
      const wait = this.cfg.epicMinIntervalMs - (Date.now() - lastAt);
      if (wait > 0) await new Promise((r) => setTimeout(r, wait));
      try { return await fn(); } finally { lastAt = Date.now(); }
    });
    // Keep the chain alive even when this call rejects.
    chain = run.catch(() => {});
    return run;
  }

  async _lookup(epic) {
    const { tc, external } = this.keysOnce();
    const { inner, body } = buildRequest(epic, tc, external);

    let status = 0, raw = '';
    try {
      const res = await fetch(EPIC_SEARCH, {
        method: 'POST',
        headers: requestHeaders(),
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(this.cfg.httpTimeoutMs),
      });
      status = res.status;
      raw = await res.text();
    } catch (e) {
      status = 0;
      raw = `${e.name}: ${e.message}`;
    }

    let hits = [];
    try {
      const parsed = raw.trim().startsWith('[') ? JSON.parse(raw) : [];
      if (Array.isArray(parsed)) hits = parsed;
    } catch { hits = []; }

    const content = (hits[0] && typeof hits[0] === 'object' ? hits[0].content ?? {} : {});
    const profile = profileFromContent(content);
    const result = {
      epic, status, hits: hits.length, content, inner: { ...inner, securityKey: '<redacted>' },
      raw: raw.slice(0, 200_000), profile, fetched_at: new Date().toISOString(),
    };
    // A cache write must not fail the lookup, but it must not vanish either:
    // report it to the shared event stream so a broken cache is visible.
    await this._cache(result).catch((e) => logEvent(
      config.deviceTag, 'warn', `epic cache write failed: ${e.message}`, 'node'));
    return result;
  }

  /** Upsert into the shared epic_lookups cache (the same table the Python
   * routes read, so a lookup here warms the dashboard's cache too). */
  async _cache(result) {
    const p = result.profile;
    await q(
      `insert into epic_lookups (epic, found, http_status, hits, name, name_local,
              relation, relation_local, relation_type, age, gender, state_cd,
              state_name, district, ac_no, ac_name, part_no, part_name, part_name_l1,
              part_id, serial_no, section_no, ps_building, ps_building_l1, record_id,
              raw, fetched_at)
       values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,
               $20,$21,$22,$23,$24,$25,$26::jsonb, now())
       on conflict (epic) do update set
         found=excluded.found, http_status=excluded.http_status, hits=excluded.hits,
         name=excluded.name, name_local=excluded.name_local, relation=excluded.relation,
         relation_local=excluded.relation_local, relation_type=excluded.relation_type,
         age=excluded.age, gender=excluded.gender, state_cd=excluded.state_cd,
         state_name=excluded.state_name, district=excluded.district, ac_no=excluded.ac_no,
         ac_name=excluded.ac_name, part_no=excluded.part_no, part_name=excluded.part_name,
         part_name_l1=excluded.part_name_l1, part_id=excluded.part_id,
         serial_no=excluded.serial_no, section_no=excluded.section_no,
         ps_building=excluded.ps_building, ps_building_l1=excluded.ps_building_l1,
         record_id=excluded.record_id, raw=excluded.raw, fetched_at=now()`,
      [result.epic, result.status === 200 && result.hits > 0, result.status, result.hits,
       p.name, p.name_local, p.relation, p.relation_local, p.relation_type, p.age,
       p.gender, p.state_cd, p.state_name, p.district, p.ac_no, p.ac_name, p.part_no,
       p.part_name, p.part_name_l1, p.part_id, p.serial_no, p.section_no,
       p.ps_building, p.ps_building_l1, p.record_id,
       JSON.stringify(result.content ?? {})],
    );
  }
}

/** Map a national-display record to flat, storable fields (profile_from_content). */
export function profileFromContent(content = {}) {
  const g = (...keys) => {
    for (const k of keys) {
      const v = content?.[k];
      if (v !== null && v !== undefined && v !== '') return v;
    }
    return null;
  };
  const relationType = g('relationType');
  return {
    name: g('fullName', 'applicantFirstName'),
    name_local: g('fullNameL1', 'applicantFirstNameL1'),
    relation: g('relativeFullName', 'relationName'),
    relation_local: g('relativeFullNameL1', 'relationNameL1'),
    relation_type: relationType,
    relation_label: relationLabel(relationType),
    age: g('age'),
    gender: g('gender'),
    state_cd: g('stateCd'),
    state_name: g('stateName'),
    district: g('districtValue'),
    ac_no: g('acNumber'),
    ac_name: g('asmblyName'),
    part_no: g('partNumber'),
    part_name: g('partName'),
    part_name_l1: g('partNameL1'),
    part_id: g('partId'),
    serial_no: g('partSerialNumber'),
    section_no: g('sectionNo'),
    ps_building: g('psbuildingName', 'buildingAddress'),
    ps_building_l1: g('psBuildingNameL1', 'buildingAddressL1'),
    record_id: g('id'),
  };
}
