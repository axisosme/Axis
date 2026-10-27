import { loadAxisConfiguration } from "./configuration.js";
import { parseRoleOptions } from "./configuration-cli.js";
import { safeError } from "./relay/errors.js";
import { startServe } from "./serve.js";

export async function runServeCli(argv: readonly string[]): Promise<number> {
  if (argv.length === 1 && argv[0] === "--help") {
    process.stdout.write("axis serve [--config <axis.toml>]\nRuns Relay, Router, Node and one configured Worker in one process.\nOnly the Relay listener may bind externally. SIGINT and SIGTERM stop all roles.\n");
    return 0;
  }
  const shutdown = new AbortController();
  let stop: () => void;
  const stopped = new Promise<void>(resolve => { stop = resolve; });
  const onSignal = (): void => { shutdown.abort(); stop(); };
  process.on("SIGINT", onSignal);
  process.on("SIGTERM", onSignal);
  let ready = false;
  try {
    const options = parseRoleOptions(argv, "relay");
    const runtime = await startServe(loadAxisConfiguration(options.filename), {
      signal: shutdown.signal,
      relay: {
        workerRouting: { audit: { record: entry => process.stdout.write(`${JSON.stringify(entry)}\n`) } },
        audit: event => process.stdout.write(`${JSON.stringify(event)}\n`),
        onTickError: ({ loop, error }) => {
          const failure = safeError(error);
          process.stderr.write(`axis.relay.linear_${loop}_tick_failed: ${failure.code}: ${failure.safeMessage}\n`);
        },
      },
    });
    ready = true;
    if (!shutdown.signal.aborted) process.stdout.write("Axis serve ready\n");
    await stopped;
    await runtime.close();
    return 0;
  } catch (error) {
    if (error === shutdown.signal.reason) return 0;
    process.stderr.write(ready ? "axis.serve.shutdown_failed\n" : "axis.serve.startup_failed\n");
    return 1;
  } finally {
    process.off("SIGINT", onSignal);
    process.off("SIGTERM", onSignal);
  }
}
