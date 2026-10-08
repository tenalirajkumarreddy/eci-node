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
  releaseReservations, getSetting,
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
