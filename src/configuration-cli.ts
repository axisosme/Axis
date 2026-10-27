import { resolve } from "node:path";

import { AxisConfigurationError, loadAxisConfiguration, selectedNode, selectedWorker, type AxisConfiguration } from "./configuration.js";

export type ServiceRole = "relay" | "node" | "worker";
export interface RoleOptions { readonly filename: string; readonly nodeId?: string; readonly workerId?: string; }

/** All standalone entry points select the same topology and local identities. */
export function parseRoleOptions(argv: readonly string[], role: ServiceRole, extraFlags: readonly string[] = []): RoleOptions & { readonly flags: ReadonlyMap<string, string> } {
  const flags = new Map<string, string>();
  for (let i = 0; i < argv.length; i += 2) {
    const flag = argv[i], value = argv[i + 1];
    if (flag === undefined || !["--config", ...(role === "relay" ? [] : ["--node"]), ...(role === "worker" ? ["--worker"] : []), ...extraFlags].includes(flag)
      || flags.has(flag) || value === undefined || value.length === 0 || value.startsWith("--")) throw new AxisConfigurationError("arguments");
    flags.set(flag, value);
  }
  return { filename: resolve(flags.get("--config") ?? "axis.toml"),
    ...(flags.has("--node") ? { nodeId: flags.get("--node")! } : {}),
    ...(flags.has("--worker") ? { workerId: flags.get("--worker")! } : {}), flags };
}

export function loadRoleConfiguration(options: RoleOptions, role: ServiceRole): AxisConfiguration {
  const configuration = loadAxisConfiguration(options.filename);
  if (role === "node") selectedNode(configuration, options.nodeId);
  if (role === "worker") selectedWorker(configuration, options.workerId, options.nodeId);
  if (role === "relay" && configuration.linear === undefined) throw new AxisConfigurationError("linear", "required for Relay");
  return configuration;
}
