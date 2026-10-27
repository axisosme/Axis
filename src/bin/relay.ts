#!/usr/bin/env node

import { deriveRelayConfiguration } from "../configuration.js";
import { loadRoleConfiguration, parseRoleOptions } from "../configuration-cli.js";
import { startRelay } from "../relay/runtime.js";
import { safeError } from "../relay/errors.js";

async function main(): Promise<void> {
  const options = parseRoleOptions(process.argv.slice(2), "relay");
  const relay = await startRelay(deriveRelayConfiguration(loadRoleConfiguration(options, "relay")), {
    workerRouting: { audit: { record: entry => process.stdout.write(`${JSON.stringify(entry)}\n`) } },
    audit: event => { process.stdout.write(`${JSON.stringify(event)}\n`); },
    onTickError: ({ loop, error }) => {
      const failure = safeError(error);
      process.stderr.write(`axis.relay.linear_${loop}_tick_failed: ${failure.code}: ${failure.safeMessage}\n`);
    },
  });
  process.stdout.write("Axis Linear Relay ready\nAxis Router listener ready\n");
  let closing = false;
  const close = () => {
    if (closing) return;
    closing = true;
    void relay.close().then(() => { process.exitCode = 0; }, () => { process.exitCode = 1; });
  };
  process.once("SIGINT", close);
  process.once("SIGTERM", close);
}
void main().catch(() => { process.stderr.write("axis.relay.startup_failed\n"); process.exitCode = 1; });
