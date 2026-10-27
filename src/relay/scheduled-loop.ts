/**
 * An interval-scheduled async loop whose ticks never overlap, with an
 * awaitable close. `setInterval` alone would let a slow tick run concurrently
 * with the next one against the same durable state.
 */

export interface ScheduledLoopOptions {
  readonly intervalMs: number;
  readonly tick: () => Promise<void>;
  /** Called for a tick that threw. The loop keeps running regardless. */
  readonly onError?: (error: unknown) => void;
}

export interface ScheduledLoop {
  /** Arms the interval timer. A no-op if already started. */
  start(): void;
  /**
   * Disarms the timer and resolves once any in-flight tick has finished, so
   * the caller can then close what the tick depends on.
   */
  close(): Promise<void>;
}

export function scheduledLoop(options: ScheduledLoopOptions): ScheduledLoop {
  let timer: ReturnType<typeof setInterval> | undefined;
  let running = false;
  let inFlight: Promise<void> = Promise.resolve();

  function fire(): void {
    // A firing during an in-flight tick is skipped, not queued.
    if (running) return;
    running = true;
    inFlight = (async () => {
      try {
        await options.tick();
      } catch (error) {
        options.onError?.(error);
      } finally {
        running = false;
      }
    })();
  }

  return {
    start(): void {
      if (timer !== undefined) return;
      timer = setInterval(fire, options.intervalMs);
      timer.unref();
    },
    async close(): Promise<void> {
      if (timer !== undefined) {
        clearInterval(timer);
        timer = undefined;
      }
      await inFlight;
    },
  };
}
