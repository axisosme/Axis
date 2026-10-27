/**
 * When a durable retry loop gives up.
 *
 * The stores that own an attempt count call `retryVerdict` rather than comparing numbers
 * themselves. Exhaustion is not a claim the failure is permanent: every failure seen here was
 * classified transient, and the budget only bounds how long Axis retries unattended. Giving up
 * quarantines the attempt visibly and never destroys the item; a later build, a fixed credential or
 * an operator can still act on what is retained.
 *
 * How the user is told, and which failures are worth retrying at all, stay with each channel.
 */

/**
 * How many attempts a durable item gets before it is quarantined. Also exported as
 * `CHANNEL_MAX_RETRYABLE_ATTEMPTS`. Both callers pace their attempts: the Linear inbox at its
 * 30-second lease, redispatch through `retryBackoffDelayMs`.
 */
export const RETRY_BUDGET_MAX_ATTEMPTS = 10;

/**
 * The failure code an item carries once its budget is spent, distinct from
 * `linear.ingress.unsupported_shape` in the inbox's `failure_code` column. The redispatch marker
 * records no code; its give-up is visible through `quarantined_at`.
 */
export const RETRY_BUDGET_EXHAUSTED_CODE = "axis.retry.attempts_exhausted";

/**
 * Exponential backoff capped at 60s between two attempts on the same item; also exported as
 * `channelBackoffDelayMs`. Ten attempts span about five minutes, so a transient outage inside that
 * window still succeeds. The argument is the attempt just spent, so it is never zero.
 */
export function retryBackoffDelayMs(attemptJustSpent: number): number {
  return Math.min(60_000, 1_000 * 2 ** Math.min(attemptJustSpent, 6));
}

/**
 * What a store should do with an item already attempted `attemptsAlreadyMade` times: `attempt`
 * carries the attempt number, `quarantine` how many were spent.
 */
export type RetryVerdict =
  /** This attempt is within budget. `attempt` is 1-based and counts this one. */
  | { readonly disposition: "attempt"; readonly attempt: number; readonly remaining: number }
  /** The budget is spent. No further attempt may be handed out; the item must be quarantined visibly, and retained. */
  | { readonly disposition: "quarantine"; readonly attempts: number; readonly maxAttempts: number };

/**
 * Whether `value` is usable as a ceiling: a positive safe integer. A boolean so each store refuses
 * in its own error vocabulary.
 */
export function isRetryBudget(value: number): boolean {
  return Number.isSafeInteger(value) && value >= 1;
}

/**
 * The verdict for an item attempted `attemptsAlreadyMade` times under `maxAttempts`. A count past
 * the ceiling reports `quarantine` rather than an error.
 */
export function retryVerdict(attemptsAlreadyMade: number, maxAttempts: number): RetryVerdict {
  if (attemptsAlreadyMade >= maxAttempts) {
    return Object.freeze({ disposition: "quarantine" as const, attempts: attemptsAlreadyMade, maxAttempts });
  }
  const attempt = attemptsAlreadyMade + 1;
  return Object.freeze({ disposition: "attempt" as const, attempt, remaining: maxAttempts - attempt });
}
