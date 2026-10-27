#!/usr/bin/env node

import { deriveWorkerConfiguration } from "../configuration.js";
import { loadRoleConfiguration, parseRoleOptions } from "../configuration-cli.js";
import { startClaudeWorker } from "../worker/claude-runtime.js";

async function main(): Promise<void> {
  const options = parseRoleOptions(process.argv.slice(2), "worker");
  const config = deriveWorkerConfiguration(loadRoleConfiguration(options, "worker"), options.workerId, options.nodeId);
  const runtime = await startClaudeWorker(config, process.env);
  process.stdout.write("Axis Claude Code Worker ready\n");
  let closing = false;
  const close = () => { if (closing) return; closing = true; void runtime.close().then(() => { process.exitCode = 0; }, () => { process.exitCode = 1; }); };
  process.once("SIGINT", close); process.once("SIGTERM", close);
}
void main().catch(() => { process.stderr.write("axis.worker.startup_failed\n"); process.exitCode = 1; });
