// sweep.js — the panel's "Sweep now on this device" action.
//
// WHAT THIS IS FOR
//
// The panel's Collect buttons put parts back in the shared queue; the fleet's
// sweeper does the HTTP work whenever it next picks them up. This module is the
// other half: a click that sweeps a part, or one AC, HERE AND NOW, over the same
// direct HTTP route the collector uses (EciClient.getEroll ->
// gateway-vha .../elastic-sir-citizen/get-eroll-data-2003), with the same
// claim/complete bookkeeping, so the rows it writes are indistinguishable from
// the collector's.
//
// WHAT IT DELIBERATELY IS NOT
//
// It does not use the device's permanent Worker, because a panel click must not
// change what the device is: a run here
//   * has a parts/minutes cap and then stops (the permanent loop runs until told
//     to stop),
//   * takes NO reservation holds, so it cannot disturb the queue the device's own
//     worker is draining (reservations are per DEVICE_TAG, not per worker, so a
//     second Worker in this process would release the first one's holds),
//   * runs no stale reaper and no crash recovery, so it never requeues parts that
//     belong to the running worker (`requeueMine` is keyed by worker_id, which two
//     workers in one process share),
//   * publishes no heartbeat - the device's identity in the fleet stays exactly
//     what the main worker says it is.
//
// One run at a time per process: two clicks cannot put two sweepers on one
// gateway connection budget, and a run in flight is visible to the panel.
import { logEvent } from './db.js';
import { config } from './config.js';
import { Aimd } from './aimd.js';
import { EciClient } from './api.js';
import { bestPendingPart, claimPart, holdReason, completePart, failPart, releasePart } from './claim.js';
import { sweepPart } from './collector.js';

// Hard ceilings, whatever the caller asks for. A panel click is a click, not a
// fleet-scale run: past this the operator wants the auto collector, not a button.
const MAX_PARTS = 500;
const MAX_MINUTES = 240;

let CURRENT = null;
let LAST = null;

/** The run in flight (or the last one), for the panel's progress line. */
export function sweepStatus() {
  return { running: CURRENT, last: LAST };
}

const clamp = (v, def, max) => {
  const n = Number(v);
  if (!Number.isFinite(n) || n <= 0) return def;
  return Math.max(1, Math.min(Math.floor(n), max));
};

const label = (p) => `${p.state_cd}/${p.ac_no}/${p.part_no}`;

/**
 * Start a sweep of one part, or of the next `parts` parts of one AC.
 *
 * `part` given  -> exactly that part is claimed (with `force`, even a done one)
 *                 and swept; the run ends with it.
 * `part` absent -> the AC is swept part by part until the cap, the clock, or an
 *                 empty queue for that AC.
 *
 * Returns {started:false, reason} when a run is already in flight or the part is
 * held by somebody else - never a silent success.
 */
export function startSweep({ state, ac = null, part = null, force = false, parts = 25, minutes = 30 } = {}) {
  const stateCd = String(state || '').trim();
  const acNo = ac === null || ac === undefined || ac === '' ? null : Number(ac);
  const partNo = part === null || part === undefined || part === '' ? null : Number(part);
  if (!stateCd) return { started: false, reason: 'state is required' };
  if (acNo !== null && (!Number.isInteger(acNo) || acNo <= 0)) return { started: false, reason: 'ac must be a positive integer' };
  if (partNo !== null && (!Number.isInteger(partNo) || partNo <= 0)) return { started: false, reason: 'part must be a positive integer' };
  if (CURRENT) return { started: false, reason: `a sweep of ${CURRENT.scope_label} is already running in this instance` };

  const run = {
    scope_label: partNo === null ? `${stateCd}${acNo === null ? '' : ` AC ${acNo}`}` : `${stateCd} AC ${acNo} part ${partNo}`,
    state_cd: stateCd, ac_no: acNo, part_no: partNo, force: !!force,
    max_parts: partNo === null ? clamp(parts, 25, MAX_PARTS) : 1,
    max_minutes: clamp(minutes, 30, MAX_MINUTES),
    started_at: new Date().toISOString(),
    done: 0, errors: 0, records: 0, epics: 0, current: null, last: null,
    finished_at: null, result: null, error: null, cancelled: false, stop: false,
  };
  CURRENT = run;

  // Deliberately NOT async: the caller gets the run object (and its started
  // state) immediately, the sweep happens in the background, exactly like the
  // catalogue jobs in catalog.js. An async wrapper here would hand callers a
  // Promise whose `.started` is undefined.
  (async () => {
    const aimd = new Aimd({ start: config.startInflight, max: config.maxInflight, windowMs: config.aimdWindowMs });
    const client = new EciClient(config, aimd);
    const t0 = Date.now();
    try {
      await logEvent(config.deviceTag, 'info',
        `one-off sweep started: ${run.scope_label} (cap ${run.max_parts} part(s), ${run.max_minutes} min, force=${run.force})`, 'node');
      for (;;) {
        if (run.stop) break;
        if (run.done + run.errors >= run.max_parts) break;
        if (Date.now() - t0 > run.max_minutes * 60_000) break;

        let target = null;
        if (partNo !== null) {
          target = { state_cd: stateCd, ac_no: acNo, part_no: partNo };
        } else {
          const scope = { state: stateCd, ac: acNo };
          target = await bestPendingPart(config.deviceTag, config.reserveTtlSec, scope);
          if (!target) break;   // nothing left to sweep in this scope
        }

        const claimed = await claimPart(config.workerId, config.deviceTag, target, config.reserveTtlSec, run.force);
        if (!claimed) {
          if (partNo !== null) {
            run.result = { swept: 0, reason: await holdReason(target, config.reserveTtlSec) };
            break;
          }
          continue;             // lost the race for this part; try the next one
        }

        run.current = claimed;
        const started = Date.now();
        try {
          const res = await sweepPart(client, claimed, config, (msg) => failPart(claimed, msg));
          if (res.status === 'error') {
            await failPart(claimed, res.lastError || 'sweep reported error');
            run.errors += 1;
            await logEvent(config.deviceTag, 'warn',
              `one-off sweep error ${label(claimed)}: ${res.lastError}`, 'node');
          } else {
            await completePart(config.workerId, config.deviceTag, claimed, {
              records: res.records, epics: res.epics, rollEnd: res.rollEnd,
            });
            run.done += 1;
            run.records += res.records;
            run.epics += res.epics;
            await logEvent(config.deviceTag, 'info',
              `one-off sweep done ${label(claimed)} records=${res.records} epics=${res.epics} `
              + `roll_end=${res.rollEnd} ${((Date.now() - started) / 1000).toFixed(0)}s`, 'node');
          }
          run.last = {
            ...claimed, records: res.records, epics: res.epics,
            status: res.status, seconds: Math.round((Date.now() - started) / 1000),
          };
        } catch (e) {
          // Same rule as the worker: a part must never be left `running` after a
          // failure, or the picker (which only looks at pending/error) never sees
          // it again.
          await releasePart(claimed, `${e.name}: ${e.message}`).catch(() => {});
          run.errors += 1;
          await logEvent(config.deviceTag, 'error', `one-off sweep failed ${label(claimed)}: ${e.message}`, 'node');
          if (partNo !== null) throw e;
          await new Promise((r) => setTimeout(r, 2000));
        }
        run.current = null;
      }
      run.result = run.result ?? { swept: run.done, errors: run.errors, records: run.records, epics: run.epics };
    } catch (e) {
      run.error = String(e?.message || e);
      run.result = run.result ?? { swept: run.done, errors: run.errors, error: run.error };
      await logEvent(config.deviceTag, 'error', `one-off sweep of ${run.scope_label} stopped: ${run.error}`, 'node');
    } finally {
      client.stop();
      run.current = null;
      run.finished_at = new Date().toISOString();
      run.seconds = Math.round((Date.now() - t0) / 1000);
      LAST = run;
      CURRENT = null;
      await logEvent(config.deviceTag, 'info',
        `one-off sweep finished: ${run.scope_label} ${run.done} part(s), ${run.errors} error(s), `
        + `${run.records} rows in ${run.seconds}s`, 'node');
    }
  })();

  return { started: true, run };
}

/** Ask the run in flight to stop at the next part boundary. */
export function stopSweep() {
  if (!CURRENT) return { stopped: false, reason: 'no sweep is running in this instance' };
  CURRENT.stop = true;
  return { stopped: true, scope: CURRENT.scope_label };
}
