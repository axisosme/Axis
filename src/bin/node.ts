#!/usr/bin/env node

import { nodeStartupDiagnostics, startNode } from "../node/runtime.js";

import { deriveNodeConfiguration } from "../configuration.js";
import { loadRoleConfiguration, parseRoleOptions } from "../configuration-cli.js";

async function main(): Promise<void> {
  const options = parseRoleOptions(process.argv.slice(2), "node");
  const config = deriveNodeConfiguration(loadRoleConfiguration(options, "node"), options.nodeId);
  const runtime = await startNode(config);
  process.stdout.write("Axis Node ready\n");
  for (const line of nodeStartupDiagnostics(runtime)) process.stderr.write(`${line}\n`);
  let closing = false;
  const close = () => {
    if (closing) return;
    closing = true;
    void runtime.close().then(
      () => { process.exitCode = 0; },
      () => { process.stderr.write("axis.node.shutdown_failed\n"); process.exitCode = 1; },
    );
  };
  process.once("SIGINT", close);
  process.once("SIGTERM", close);
}

void main().catch(() => {
  process.stderr.write("axis.node.startup_failed\n");
  process.exitCode = 1;
});
