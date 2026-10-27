import { createHash } from "node:crypto";

export const HOST_STATE_VERSION = "axis.host.v1" as const;
export type HostRole = "controller" | "worker" | "node";
export type WorkerRuntime = "codex" | "claude";

export interface HostState {
  readonly schemaVersion: typeof HOST_STATE_VERSION;
  readonly role: HostRole;
  readonly workerRuntime: WorkerRuntime | null;
  readonly serviceLabel: string;
  readonly configuration: HostConfigurationReference;
  readonly managedPlist: string;
  readonly activeRelease: { readonly packagePath: string; readonly version: string; readonly manifestDigest: string };
  readonly installIdentity: string;
  readonly revision: number;
}

export function hostInstallIdentity(input: Pick<HostState, "role" | "workerRuntime" | "serviceLabel" | "configuration" | "managedPlist" | "activeRelease">): string {
  return digestText(`${input.role}\n${input.workerRuntime ?? ""}\n${input.serviceLabel}\n${input.configuration.filename}\n${input.configuration.digest}\n${input.configuration.nodeId ?? ""}\n${input.configuration.workerId ?? ""}\n${input.managedPlist}\n${input.activeRelease.version}\n${input.activeRelease.manifestDigest}`);
}

export class HostStateError extends Error {}

const label = /^[A-Za-z][A-Za-z0-9.-]{2,127}$/u;
const digest = /^sha256:[a-f0-9]{64}$/u;
const version = /^[0-9]+\.[0-9]+\.[0-9]+(?:[-+][A-Za-z0-9.-]+)?$/u;
export function assertAbsolutePath(value: unknown, name: string): string {
  if (typeof value !== "string" || !value.startsWith("/") || value.includes("\u0000")) throw new HostStateError(`invalid ${name}`);
  return value;
}

export interface HostConfigurationReference {
  readonly filename: string;
  readonly digest: string;
  readonly nodeId: string | null;
  readonly workerId: string | null;
}
export function validateConfigurationReference(value: unknown): HostConfigurationReference {
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new HostStateError("invalid configuration reference");
  const reference = value as Record<string, unknown>;
  const identifier = (value: unknown): boolean => value === null || typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u.test(value);
  if (Object.keys(reference).sort().join(",") !== "digest,filename,nodeId,workerId" || typeof reference.digest !== "string" || !digest.test(reference.digest)
    || !identifier(reference.nodeId) || !identifier(reference.workerId)) throw new HostStateError("invalid configuration reference");
  return Object.freeze({ filename: assertAbsolutePath(reference.filename, "configuration file"), digest: reference.digest,
    nodeId: reference.nodeId as string | null, workerId: reference.workerId as string | null });
}

export function validateHostState(value: unknown): HostState {
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new HostStateError("invalid host state");
  const state = value as Record<string, unknown>;
  const expected = ["activeRelease", "configuration", "installIdentity", "managedPlist", "revision", "role", "schemaVersion", "serviceLabel", "workerRuntime"];
  if (Object.keys(state).sort().join(",") !== expected.join(",")) throw new HostStateError("invalid host state keys");
  if (state.schemaVersion !== HOST_STATE_VERSION || (state.role !== "controller" && state.role !== "worker" && state.role !== "node")) throw new HostStateError("invalid host identity");
  if (((state.role === "controller" || state.role === "node") && state.workerRuntime !== null) || (state.role === "worker" && state.workerRuntime !== "codex" && state.workerRuntime !== "claude")) throw new HostStateError("invalid worker runtime");
  if (typeof state.serviceLabel !== "string" || !label.test(state.serviceLabel)) throw new HostStateError("invalid service label");
  if (typeof state.installIdentity !== "string" || !digest.test(state.installIdentity)) throw new HostStateError("invalid install identity");
  if (typeof state.revision !== "number" || !Number.isSafeInteger(state.revision) || state.revision < 1) throw new HostStateError("invalid revision");
  const active = state.activeRelease;
  if (active === null || typeof active !== "object" || Array.isArray(active)) throw new HostStateError("invalid active release");
  const release = active as Record<string, unknown>;
  if (Object.keys(release).sort().join(",") !== "manifestDigest,packagePath,version" || typeof release.version !== "string" || !version.test(release.version) || typeof release.manifestDigest !== "string" || !digest.test(release.manifestDigest)) throw new HostStateError("invalid release identity");
  const normalized: HostState = { schemaVersion: HOST_STATE_VERSION, role: state.role, workerRuntime: state.workerRuntime as WorkerRuntime | null, serviceLabel: state.serviceLabel, configuration: validateConfigurationReference(state.configuration), managedPlist: assertAbsolutePath(state.managedPlist, "managed plist"), activeRelease: Object.freeze({ packagePath: assertAbsolutePath(release.packagePath, "release package"), version: release.version, manifestDigest: release.manifestDigest }), installIdentity: state.installIdentity, revision: state.revision };
  if (normalized.role === "controller" && (normalized.configuration.nodeId !== null || normalized.configuration.workerId !== null)
    || normalized.role === "node" && (normalized.configuration.nodeId === null || normalized.configuration.workerId !== null)
    || normalized.role === "worker" && (normalized.configuration.nodeId === null || normalized.configuration.workerId === null)) throw new HostStateError("invalid role selection");
  if (normalized.installIdentity !== hostInstallIdentity(normalized)) throw new HostStateError("invalid install identity");
  return Object.freeze(normalized);
}

export function digestText(value: string): string { return `sha256:${createHash("sha256").update(value).digest("hex")}`; }

export interface LifecycleReceipt {
  readonly schemaVersion: "axis.host.receipt.v1";
  readonly operation: "install" | "upgrade";
  readonly role: HostRole;
  readonly version: string;
  readonly manifestDigest: string;
  /** A domain-separated identity reference; never the launchd label itself. */
  readonly serviceRef: string;
  readonly status: "installed" | "upgraded" | "rolled_back" | "failed";
  readonly rollback: boolean;
}

export function serviceRef(serviceLabel: string): string { return digestText(`axis.host.receipt.v1.service-ref\n${serviceLabel}`); }

export function receipt(operation: LifecycleReceipt["operation"], state: HostState, status: LifecycleReceipt["status"], rollback = false): LifecycleReceipt {
  return { schemaVersion: "axis.host.receipt.v1", operation, role: state.role, version: state.activeRelease.version, manifestDigest: state.activeRelease.manifestDigest, serviceRef: serviceRef(state.serviceLabel), status, rollback };
}
