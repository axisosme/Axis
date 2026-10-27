import { constants, closeSync, fstatSync, lstatSync, openSync, readFileSync, realpathSync } from "node:fs";
import { resolve } from "node:path";

import { parseAxisConfiguration, selectedNode, selectedWorker, type AxisConfiguration } from "../configuration.js";
import { digestText, HostStateError, type HostConfigurationReference, type HostRole } from "./host-state.js";

function readConfiguration(filename: string): { readonly config: AxisConfiguration; readonly digest: string } {
  let fd: number | undefined;
  try {
    if (realpathSync(filename) !== resolve(filename) || lstatSync(filename).isSymbolicLink()) throw new HostStateError("unsafe configuration file");
    fd = openSync(filename, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
    const metadata = fstatSync(fd);
    if (!metadata.isFile() || metadata.uid !== process.getuid?.() || (metadata.mode & 0o077) !== 0) throw new HostStateError("configuration file must be owner-private");
    const source = readFileSync(fd, "utf8");
    return { config: parseAxisConfiguration(source, filename), digest: digestText(source) };
  } finally { if (fd !== undefined) closeSync(fd); }
}

/** Managed configuration is immutable for the lifetime of an installation. */
export function managedConfigurationReference(filename: string, role: HostRole, nodeId?: string, workerId?: string): HostConfigurationReference {
  const canonical = resolve(filename), { config, digest } = readConfiguration(canonical);
  if (role === "controller") {
    if (nodeId !== undefined || workerId !== undefined || config.linear === undefined) throw new HostStateError("invalid controller selection");
    return Object.freeze({ filename: canonical, digest, nodeId: null, workerId: null });
  }
  const node = selectedNode(config, nodeId);
  if (role === "node") {
    if (workerId !== undefined) throw new HostStateError("invalid Node selection");
    return Object.freeze({ filename: canonical, digest, nodeId: node.id, workerId: null });
  }
  const { worker } = selectedWorker(config, workerId, node.id);
  return Object.freeze({ filename: canonical, digest, nodeId: node.id, workerId: worker.id });
}
export function loadHostConfiguration(reference: HostConfigurationReference): AxisConfiguration {
  const { config, digest } = readConfiguration(reference.filename);
  if (digest !== reference.digest) throw new HostStateError("managed configuration changed; restore the pinned file or perform a fresh install");
  if (reference.nodeId !== null) selectedNode(config, reference.nodeId);
  if (reference.workerId !== null) selectedWorker(config, reference.workerId, reference.nodeId ?? undefined);
  return config;
}
