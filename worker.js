// worker.js — the auto loop: top up the reservation queue, take a part, sweep
// it, close it out. Ported from worker.py's run loop, with the same split
// between DEVICE_TAG (reservations) and WORKER_ID (claimed_by).
import { setTimeout as sleep } from 'node:timers/promises';
import { logEvent } from './db.js';
import { config } from './config.js';
import { Aimd } from './aimd.js';
import { EciClient } from './api.js';
import {
  bestPendingPart, claimPart, reserveParts, takeReserved, reserveCount,
  completePart, failPart, releasePart, requeueMine, requeueStale,
  releaseReservations, getSetting, setSetting,
} from './claim.js';
import { sweepPart } from './collector.js';

const isOn = (v) => v === true || String(v).toLowerCase() === 'true' || v === 1;

export class Worker {
  constructor({ scope = {}, force = false, noRecover = false, limits = {} } = {}) {
    this.scope = scope;                          // {state, ac} - narrow the sweep
    this.force = force;
    this.noRecover = noRecover;
    this.limits = limits;                        // {parts, minutes, idleExit}
    this.aimd = new Aimd({
      start: config.startInflight, max: config.maxInflight, windowMs: config.aimdWindowMs,
    });
    this.client = new EciClient(config, this.aimd);
    this.running = false;
    this.current = null;
    this.stopFlag = false;
    this.doneParts = 0;
    this.errors = 0;
    this.startedAt = Date.now();
    this.lastProgress = Date.now();
    this.reaper = null;
    // Fleet health: this device's own numbers, published where the panel (and
    // the Python/Android dashboards) read them - see startHeartbeat().
    this.heartbeatTimer = null;
    this.ipTimer = null;
    this.egressIp = null;
    this.egressAt = null;
  }

  /**
   * Publish this device's health to the shared `settings` row, so the panel can
   * answer "how many more devices can I add" from measurements rather than
   * guesses: the controller's limit, the 429/5xx mix and the live req/s per
   * device, plus the egress IP - the number that matters most, because Render
   * shares its regional outbound ranges across services, so several instances
   * can be competing for one per-IP gateway ceiling.
   *
   * Same key shape the Python and Android clients use (`worker_heartbeat@<tag>`),
   * so every device shows up in the same fleet view. One small upsert every
   * `heartbeatMs`; failures are swallowed because a metrics write must never be
   * the reason a sweep dies.
   */
  startHeartbeat() {
    if (this.heartbeatTimer) return;
    const beat = () => { this._beat().catch(() => {}); };
    beat();
    this.heartbeatTimer = setInterval(beat, config.heartbeatMs);
    this.heartbeatTimer.unref?.();
    if (config.reportIp) {
      // Once at startup, then hourly: enough to prove whether devices share an
      // egress IP, cheap enough to be irrelevant.
      this._refreshEgressIp().catch(() => {});
      this.ipTimer = setInterval(() => { this._refreshEgressIp().catch(() => {}); }, 3_600_000);
      this.ipTimer.unref?.();
    }
  }

  /** Final beat + stop publishing (called on shutdown). */
  async stopHeartbeat() {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    if (this.ipTimer) clearInterval(this.ipTimer);
    this.heartbeatTimer = null;
    this.ipTimer = null;
    this.running = false;
    await this._beat(true).catch(() => {});
  }

  async _beat(final = false) {
    const a = this.aimd.snapshot();
    const c = this.current;
    await setSetting(config.deviceTag, 'worker_heartbeat', {
      at: Math.floor(Date.now() / 1000),
      iso: new Date().toISOString(),
      tag: config.deviceTag,
      worker_id: config.workerId,
      source: 'node',
      running: !!this.running,
      paused: !!this.stopFlag,
      stopping: final,
      parts_done: this.doneParts,
      errors: this.errors,
      uptime_s: Math.round((Date.now() - this.startedAt) / 1000),
      current: c ? {
        state_cd: c.state_cd, ac_no: c.ac_no, part_no: c.part_no, name: c.name ?? null,
      } : null,
      last_done: this.lastDone ? {
        state_cd: this.lastDone.state_cd, ac_no: this.lastDone.ac_no,
        part_no: this.lastDone.part_no, records: this.lastDone.records,
        epics: this.lastDone.epics, roll_end: this.lastDone.roll_end,
        seconds: this.lastDone.seconds, finished_at: this.lastDone.finished_at,
      } : null,
      aimd: {
        limit: a.limit, inflight: a.inflight, peak: a.peak,
        reqs: a.reqs, errReqs: a.errReqs,
        // The mix is the point: 429s are the gateway's ceiling, 5xx are its
        // trouble, and neither was visible in the database before this row.
        byStatus: a.byStatus, errByStatus: a.errByStatus,
        retryAfters: a.retryAfters, coolingForSec: a.coolingForSec,
        recentReqPerSec: a.recentReqPerSec, windowAvgReqsPerSec: a.windowAvgReqsPerSec,
      },
      egress_ip: this.egressIp,
      egress_at: this.egressAt,
    });
  }

  /** Ask a neutral third party what this process's public IP is. */
  async _refreshEgressIp() {
    try {
      const res = await fetch('https://api.ipify.org?format=json', {
        signal: AbortSignal.timeout(8000),
      });
      if (res.status !== 200) return;
      const body = await res.json().catch(() => null);
      const ip = typeof body?.ip === 'string' ? body.ip : null;
      if (ip && ip !== this.egressIp) {
        this.egressIp = ip;
        this.egressAt = new Date().toISOString();
        await logEvent(config.deviceTag, 'info', `egress ip ${ip}`, 'node');
      }
    } catch { /* offline, blocked, or rate limited: not our problem */ }
  }

  /** Start (or resume) the auto loop. Safe to call again after pause(). */
  async start() {
    if (this.running) return false;
    this.stopFlag = false;
    this.client.stopped = false;   // clear a previous pause()
    this.running = true;
    await logEvent(config.deviceTag, 'info',
      `node worker started tag=${config.deviceTag} id=${config.workerId} `
      + `aimd_start=${config.startInflight} max=${config.maxInflight || 'inf'}`,
      'node');

    // Crash recovery: anything this process held mid-sweep goes back to pending.
    if (!this.noRecover) {
      const mine = await requeueMine(config.workerId);
      if (mine.length) {
        await logEvent(config.deviceTag, 'warn', `requeued ${mine.length} parts held by this process`, 'node');
      }
    }
    // Stale reaper: parts whose holder (any device) went silent.
    this.reaper = setInterval(async () => {
      try {
        const r = await requeueStale(config.staleMin);
        if (r.length) {
          await logEvent(config.deviceTag, 'info', `stale reaper requeued ${r.length} parts`, 'node');
        }
      } catch { /* the reaper must never kill the worker */ }
    }, 60_000);
    this.reaper.unref?.();

    this._loop().catch(async (e) => {
      await logEvent(config.deviceTag, 'error', `worker loop died: ${e.message}`, 'node');
      this.running = false;
    });
  }

  async _loop() {
    while (!this.stopFlag) {
      const L = this.limits;
      if (L.parts && this.doneParts >= L.parts) break;
      if (L.minutes && Date.now() - this.startedAt > L.minutes * 60_000) break;
      if (L.idleExit && Date.now() - this.lastProgress > L.idleExit * 60_000) break;

      // Pause lands on a part boundary, never mid-sweep: the `auto_enabled@tag`
      // knob is the single source of truth, so the Stop button here and the
      // Python dashboard's switch both pause this device the same way.
      const auto = await getSetting(config.deviceTag, 'auto_enabled', true);
      if (!isOn(auto)) {
        await logEvent(config.deviceTag, 'info', 'paused (auto_enabled off)', 'node');
        break;
      }

      let part = null;
      try {
        // Top up only when the queue is short - the picker is the expensive part.
        if (await reserveCount(config.deviceTag, config.reserveTtlSec) < config.reserveN) {
          await reserveParts(config.deviceTag, config.reserveN, config.reserveTtlSec, this.scope);
        }
        part = await takeReserved(config.workerId, config.deviceTag, config.reserveTtlSec, this.force);
        if (!part) {
          const best = await bestPendingPart(config.deviceTag, config.reserveTtlSec, this.scope);
          if (best) {
            part = await claimPart(config.workerId, config.deviceTag, best, config.reserveTtlSec, this.force);
          }
        }
        if (!part) { await sleep(4000); continue; }

        this.current = part;
        const a = this.aimd.snapshot();
        await logEvent(config.deviceTag, 'info',
          `sweep ${part.state_cd}/${part.ac_no}/${part.part_no} `
          + `(aimd limit=${a.limit} inflight=${a.inflight})`, 'node');

        const res = await sweepPart(this.client, part, config, (msg) => failPart(part, msg));

        if (res.status === 'error') {
          await failPart(part, res.lastError || 'sweep reported error');
          this.errors += 1;
          await logEvent(config.deviceTag, 'warn',
            `error ${part.state_cd}/${part.ac_no}/${part.part_no}: ${res.lastError}`, 'node');
        } else {
          await completePart(config.workerId, config.deviceTag, part, {
            records: res.records, epics: res.epics, rollEnd: res.rollEnd,
          });
          await logEvent(config.deviceTag, 'info',
            `done ${part.state_cd}/${part.ac_no}/${part.part_no} records=${res.records} `
            + `epics=${res.epics} roll_end=${res.rollEnd} `
            + `${((Date.now() - res.t0) / 1000).toFixed(0)}s`, 'node');
          // Remembered so the dashboard can keep showing a just-finished part
          // instead of blanking the card the moment the next claim starts.
          this.lastDone = {
            state_cd: part.state_cd, ac_no: part.ac_no, part_no: part.part_no,
            name: part.name ?? null, status: 'done',
            records: res.records, epics: res.epics, roll_end: res.rollEnd,
            seconds: Math.round((Date.now() - res.t0) / 1000),
            finished_at: new Date().toISOString(),
          };
        }
        this.doneParts += 1;
        this.lastProgress = Date.now();
      } catch (e) {
        await logEvent(config.deviceTag, 'error', `sweep failed: ${e.message}`, 'node');
        // A part must never stay `running` after a failure: the picker only
        // looks at pending/error, so a stranded part is invisible to the whole
        // fleet until somebody restarts. Hand it back.
        if (part) { try { await releasePart(part, `${e.name}: ${e.message}`); } catch { /* ignore */ } }
        await sleep(3000);
      }
      this.current = null;
    }

    this.running = false;
    if (this.reaper) clearInterval(this.reaper);
    this.client.stop();
    await releaseReservations(config.deviceTag).catch(() => {});
    await logEvent(config.deviceTag, 'info',
      `node worker stopped after ${this.doneParts} parts (${this.errors} errors), `
      + `aimd peak=${this.aimd.stats.peak}`, 'node');
  }

  stop() { this.stopFlag = true; }

  /** Stop sweeping at the next part boundary. The loop then releases this
   * device's reservations and this.running goes false; the process and its
   * dashboard stay up so start() can revive it. */
  pause() { this.stop(); }
}
