/**
 * How often an observe tick runs the channel stores' full invariant check
 * (`verifyIntegrity()`), as a minimum wall-time gap between checks.
 *
 * A full check costs roughly 4 ms/MiB and blocks the event loop. A minimum
 * interval, unlike "every N ticks", keeps the scan rate independent of the
 * tick interval. Schema damage is still caught on every call by each store's
 * `#assertUsable`; only the window for noticing data damage widens, to
 * `max(intervalMs, tickInterval) + one tick's duration`, since
 * `./scheduled-loop.ts` skips an interval while a tick is in flight.
 */

/** Sized so a ~250MB store pair costs about 3% of a core. */
export const DEFAULT_INTEGRITY_CHECK_INTERVAL_MS = 60_000;

export interface IntegrityCheckSchedule {
  /**
   * Whether this tick runs the full check. Call exactly once per tick: a
   * `true` answer records the check as taken.
   */
  due(): boolean;
}

export interface IntegrityCheckScheduleOptions {
  /** Minimum milliseconds between two full checks. `0` means every tick. */
  readonly intervalMs?: number;
  /**
   * Monotonic milliseconds; injected only by tests. Wall clock is avoided
   * because a backwards step would suspend checks. A clock that goes
   * backwards anyway makes the check due, never skipped.
   */
  readonly now?: () => number;
}

/** Admits the full check at most once per `intervalMs`; the first call is always due. */
export function integrityCheckSchedule(options: IntegrityCheckScheduleOptions = {}): IntegrityCheckSchedule {
  const intervalMs = options.intervalMs ?? DEFAULT_INTEGRITY_CHECK_INTERVAL_MS;
  // `NaN` would silently make every tick due, so it is refused, as is a negative gap.
  if (!Number.isFinite(intervalMs) || intervalMs < 0) throw new RangeError(`Integrity check interval must be a finite, non-negative number of milliseconds; received ${String(intervalMs)}.`);
  const now = options.now ?? ((): number => performance.now());
  let lastAt: number | undefined;
  return {
    due(): boolean {
      const at = now();
      if (lastAt !== undefined && at >= lastAt && at - lastAt < intervalMs) return false;
      lastAt = at;
      return true;
    },
  };
}
