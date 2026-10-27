#!/usr/bin/env node

import { loadHostConfiguration } from "./configuration.js";

import { spawn } from "node:child_process";
import { readFile, realpath, stat } from "node:fs/promises";
import { resolve } from "node:path";

import { validateHostState } from "./host-state.js";

function bin(state: ReturnType<typeof validateHostState>): string { if (state.role === "controller") return "axis-linear-relay"; if (state.role === "node") return "axis-node"; if (state.workerRuntime === "codex") return "axis-codex-worker"; if (state.workerRuntime === "claude") return "axis-claude-worker"; throw new Error("invalid worker runtime"); }
async function privateFile(path: string): Promise<string> {
  const canonical = await realpath(path); const metadata = await stat(canonical); if (!metadata.isFile() || metadata.uid !== (process.getuid?.() ?? metadata.uid) || (metadata.mode & 0o077) !== 0) throw new Error("unsafe lifecycle file"); return canonical;
}
async function main(): Promise<void> {
  if (process.argv.length !== 4 || process.argv[2] !== "--state" || !process.argv[3]?.startsWith("/")) throw new Error("invalid runner arguments");
  const stateFile = await privateFile(process.argv[3]); const state = validateHostState(JSON.parse(await readFile(stateFile, "utf8")) as unknown); loadHostConfiguration(state.configuration);
  const packageJson = JSON.parse(await readFile(resolve(state.activeRelease.packagePath, "package.json"), "utf8")) as { bin?: Record<string, string> }; const target = packageJson.bin?.[bin(state)]; if (typeof target !== "string") throw new Error("missing package local service bin");
  const executable = resolve(state.activeRelease.packagePath, target); if (!executable.startsWith(`${state.activeRelease.packagePath}/`)) throw new Error("unsafe package local service bin");
  const child = spawn(process.execPath, [executable, "--config", state.configuration.filename, ...(state.configuration.nodeId === null ? [] : ["--node", state.configuration.nodeId]), ...(state.configuration.workerId === null ? [] : ["--worker", state.configuration.workerId])], { stdio: "inherit", env: process.env });
  const forward = (signal: NodeJS.Signals): void => { if (!child.killed) child.kill(signal); }; process.once("SIGTERM", forward); process.once("SIGINT", forward);
  try { const outcome = await new Promise<{ readonly code: number | null; readonly signal: NodeJS.Signals | null }>((resolveExit, rejectExit) => { child.once("exit", (code, signal) => { resolveExit({ code, signal }); }); child.once("error", rejectExit); }); process.exitCode = outcome.code ?? (outcome.signal === null ? 1 : 128); } finally { process.removeListener("SIGTERM", forward); process.removeListener("SIGINT", forward); }
}
void main().catch(() => { process.exitCode = 1; });
