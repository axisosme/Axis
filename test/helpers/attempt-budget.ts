/**
 * Bound test waits by retry count so scheduling delays do not consume useful attempts.
 * A stuck operation exhausts its budget; a slow machine takes longer to perform the
 * same number of attempts. Callers inject the pause and the condition being observed.
 */

/**
 * Reference attempt budget and shared polling interval. Each wait supplies an explicit
 * maximum attempt count; the reference count is not an implicit default.
 */
export const DEFAULT_MAX_ATTEMPTS = 500;
export const DEFAULT_ATTEMPT_INTERVAL_MS = 10;

/**
 * Real pause between attempts. Callers pass it explicitly as the required sleep
 * collaborator, allowing deterministic waits to inject their own pause.
 */
export const realAttemptSleep = (milliseconds: number): Promise<void> =>
  new Promise<void>((resolve) => { setTimeout(resolve, milliseconds); });

export interface AttemptBudgetWait {
  /**
   * Whether the awaited condition has been observed.
   *
   * Called once before the first pause, so a condition that is already true
   * costs no attempts at all, and once more after each pause. May be async:
   * some conditions are only observable by doing work (driving a tick,
   * reading a child's stats), and forcing those into a synchronous shape would
   * push the work outside the budget that is supposed to bound it.
   */
  readonly settled: () => boolean | Promise<boolean>;
  /** Attempts spent before giving up. The budget, in the only unit the machine's load cannot erode. */
  readonly maxAttempts: number;
  /** The pause between attempts. Bounds how often `settled` runs; it never bounds the wait. */
  readonly intervalMs: number;
  /** The pause itself. Required: see `realAttemptSleep`. */
  readonly sleep: (milliseconds: number) => Promise<void>;
  /** What was being waited for, rendered into the failure. Called only on failure. */
  readonly describe: () => string;
}

/**
 * Wait until settled() reports true, checking once immediately and after each pause,
 * with at most maxAttempts pauses. No wall clock or independent deadline consumes the
 * attempt budget.
 */
export async function awaitWithinAttempts(wait: AttemptBudgetWait): Promise<void> {
  for (let attempt = 0; ; attempt += 1) {
    if (await wait.settled()) return;
    if (attempt >= wait.maxAttempts) {
      throw new Error(`${wait.describe()} within ${wait.maxAttempts} attempts of ${wait.intervalMs}ms`);
    }
    await wait.sleep(wait.intervalMs);
  }
}
