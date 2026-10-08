import { setTimeout as sleep } from 'node:timers/promises';

/**
 * AIMD congestion controller — replaces every hardcoded req/s or worker cap.
 *
 * Additive increase while the gateway answers cleanly (windowMs), multiplicative
 * decrease (halve) on genuine backpressure. Retry-After, when the gateway sends
 * it, overrides everything for the given cooldown.
 *
 * "Genuine backpressure" is the whole point: a 429, a 5xx or a network/timeout
 * means slow down. A 404 is NOT backpressure - it is the old-roll route's
 * ordinary answer for a serial past the end of a part, and every sweep produces
 * a dozen of them while measuring where the roll ends. Counting those as errors
 * both inflated the error counter and halved the limit several times per part,
 * so the controller could never settle above ~4-10 even though the gateway
 * answered 400/400 requests at 103 req/s in testing. Callers therefore pass
 * `ok = false` only for real trouble (see api.js), and the per-status counters
 * below keep the classification visible instead of implicit.
 *
 * max = 0 means unbounded: the ceiling is discovered at runtime from the
 * gateway itself (or from client fds/memory, whichever comes first).
 */
export class Aimd {
  constructor({ start = 8, max = 0, windowMs = 2000, recentMs = 10_000 } = {}) {
    this.limit = start;
    this.max = max;                 // 0 => Infinity
    this.windowMs = windowMs;
    this.recentMs = recentMs;
    this.inflight = 0;
    this.oKs = 0;
    this.errs = 0;
    this.coolUntil = 0;
    this.stats = {
      peak: start, reqs: 0, errReqs: 0,
      byStatus: {},                 // every answered request, by HTTP status
      errByStatus: {},              // only the ones that counted as errors
      retryAfters: 0,               // honoured Retry-After cooldowns
      startedAt: Date.now(),
    };
    this._recent = [];              // completion timestamps, live req/s window
    this._t = setInterval(() => this._window(), windowMs);
    this._t.unref?.();
  }

  _window() {
    if (this.errs === 0 && this.oKs > 0) this.limit += 1;   // additive increase
    this.oKs = 0;
    this.errs = 0;
    if (this.limit > this.stats.peak) this.stats.peak = this.limit;
  }

  get effectiveLimit() { return this.max > 0 ? Math.min(this.limit, this.max) : this.limit; }

  async acquire() {
    for (;;) {
      if (Date.now() >= this.coolUntil && this.inflight < this.effectiveLimit) {
        this.inflight += 1;
        return;
      }
      await sleep(25);
    }
  }

  /**
   * @param ok     true for any normal answer (including 404 = past roll end)
   * @param status http status (0 = network failure / timeout)
   */
  release(ok, status = 200) {
    this.inflight = Math.max(0, this.inflight - 1);
    this.stats.reqs += 1;
    this.stats.byStatus[status] = (this.stats.byStatus[status] || 0) + 1;
    this._recent.push(Date.now());
    if (ok) { this.oKs += 1; return; }
    this.errs += 1;
    this.stats.errReqs += 1;
    this.stats.errByStatus[status] = (this.stats.errByStatus[status] || 0) + 1;
    this.limit = Math.max(1, Math.floor(this.limit / 2));   // multiplicative decrease
  }

  /** Hard server pushback: pause the whole client, then restart low. */
  backoffRetryAfter(seconds) {
    this.stats.retryAfters += 1;
    this.coolUntil = Date.now() + Math.max(1, seconds) * 1000;
    this.limit = Math.max(1, Math.floor(this.limit / 2));
  }

  /** Requests answered in the last `recentMs` - the honest "current speed". */
  recentReqPerSec() {
    const cut = Date.now() - this.recentMs;
    if (this._recent.length && this._recent[0] < cut) {
      this._recent = this._recent.filter((t) => t >= cut);
    }
    return this._recent.length / (this.recentMs / 1000);
  }

  snapshot() {
    return {
      limit: this.limit,
      inflight: this.inflight,
      peak: this.stats.peak,
      reqs: this.stats.reqs,
      errReqs: this.stats.errReqs,
      errByStatus: this.stats.errByStatus,
      byStatus: this.stats.byStatus,
      retryAfters: this.stats.retryAfters,
      recentReqPerSec: Number(this.recentReqPerSec().toFixed(1)),
      windowAvgReqsPerSec: Number((this.stats.reqs / Math.max(1, (Date.now() - this.stats.startedAt) / 1000)).toFixed(1)),
      coolingForSec: Math.max(0, Math.round((this.coolUntil - Date.now()) / 1000)),
    };
  }
}
