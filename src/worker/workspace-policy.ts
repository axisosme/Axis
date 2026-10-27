/** Resolve trusted workspace roots for Linear organizations. */
import { constants } from "node:fs";
import { access, realpath, stat } from "node:fs/promises";
import { isAbsolute } from "node:path";

import type { WorkerVersionedReference, WorkerWorkspaceConfiguration } from "../shared/router-worker.js";

/** The provider a workspace mapping and Run belong to. Tenancy is provider-scoped. */
export type WorkerWorkspaceSystem = "linear";

export type WorkerWorkspaceResolutionOutcome = "mapped" | "default_root" | "fallback_root" | "unavailable";
export type WorkerWorkspaceResolutionReason = "catalog_match" | "no_ref" | "catalog_miss" | "mapped_path_unavailable" | "root_unavailable";
export interface WorkerWorkspaceResolution {
  readonly outcome: WorkerWorkspaceResolutionOutcome;
  readonly reason: WorkerWorkspaceResolutionReason;
  readonly workingDirectory: string | null;
}

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value) && Object.getPrototypeOf(value) === Object.prototype
    ? value as Record<string, unknown>
    : null;
}
function exactKeys(value: Record<string, unknown>, expected: readonly string[]): boolean {
  const actual = Object.keys(value).sort(), sorted = [...expected].sort();
  return actual.length === sorted.length && actual.every((key, index) => key === sorted[index]);
}
function identifier(value: unknown): string | null {
  return typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u.test(value) ? value : null;
}
function reference(value: unknown): WorkerVersionedReference | null {
  const candidate = record(value);
  if (candidate === null || !exactKeys(candidate, ["ref", "version"])) return null;
  const ref = identifier(candidate.ref), version = identifier(candidate.version);
  return ref === null || version === null ? null : { ref, version };
}
function path(value: unknown): string | null {
  return typeof value === "string" && value.trim().length > 0 && isAbsolute(value) ? value : null;
}
export function workspaceReferenceKey(value: WorkerVersionedReference): string { return `${value.ref}\u0000${value.version}`; }

export function workerWorkspaceKey(system: WorkerWorkspaceSystem, id: string): string { return `${system}\u0000${id}`; }

/** The key a configuration entry occupies. */
export function workerWorkspaceConfigurationKey(configuration: WorkerWorkspaceConfiguration): string {
  return workerWorkspaceKey("linear", configuration.organizationId);
}

export function validateWorkerWorkspaceConfigurations(parsed: unknown): readonly WorkerWorkspaceConfiguration[] {
  if (!Array.isArray(parsed) || parsed.length === 0) throw new Error("Worker workspace configuration is invalid.");
  const keys = new Set<string>();
  const configurations: WorkerWorkspaceConfiguration[] = [];
  for (const value of parsed) {
    const candidate = record(value);
    if (candidate === null || !([
      ["organizationId", "workspaceRoot"],
      ["organizationId", "workspaceRoot", "workspaceCatalog"],
    ].some((shape) => exactKeys(candidate, shape)))) throw new Error("Worker workspace configuration is invalid.");
    const scopeId = identifier(candidate.organizationId), workspaceRoot = path(candidate.workspaceRoot);
    if (scopeId === null || workspaceRoot === null) throw new Error("Worker workspace configuration is invalid.");
    const key = workerWorkspaceKey("linear", scopeId);
    if (keys.has(key)) throw new Error("Worker workspace configuration is invalid.");
    keys.add(key);
    const scope = { organizationId: scopeId };
    const catalogValue = candidate.workspaceCatalog;
    if (catalogValue === undefined) { configurations.push(Object.freeze({ ...scope, workspaceRoot })); continue; }
    if (!Array.isArray(catalogValue)) throw new Error("Worker workspace configuration is invalid.");
    const references = new Set<string>();
    const workspaceCatalog = catalogValue.map((entry) => {
      const item = record(entry);
      if (item === null || !exactKeys(item, ["workspacePolicyRef", "workspaceRoot"])) throw new Error("Worker workspace configuration is invalid.");
      const workspacePolicyRef = reference(item.workspacePolicyRef), mappedRoot = path(item.workspaceRoot);
      if (workspacePolicyRef === null || mappedRoot === null) throw new Error("Worker workspace configuration is invalid.");
      const key = workspaceReferenceKey(workspacePolicyRef);
      if (references.has(key)) throw new Error("Worker workspace configuration is invalid.");
      references.add(key);
      return Object.freeze({ workspacePolicyRef: Object.freeze(workspacePolicyRef), workspaceRoot: mappedRoot });
    });
    configurations.push(Object.freeze({ ...scope, workspaceRoot, workspaceCatalog: Object.freeze(workspaceCatalog) }));
  }
  return Object.freeze(configurations);
}

export function normalizeWorkerWorkspaceConfigurations(input: readonly WorkerWorkspaceConfiguration[]): ReadonlyMap<string, WorkerWorkspaceConfiguration> {
  return new Map(validateWorkerWorkspaceConfigurations(input).map((configuration) => [workerWorkspaceConfigurationKey(configuration), configuration]));
}

async function usableDirectory(candidate: string): Promise<string | null> {
  try {
    const canonical = await realpath(candidate);
    if (!(await stat(canonical)).isDirectory()) return null;
    await access(canonical, constants.R_OK | constants.W_OK | constants.X_OK);
    return canonical;
  } catch { return null; }
}

export async function resolveWorkerWorkspace(configuration: WorkerWorkspaceConfiguration, workspacePolicyRef: WorkerVersionedReference | null): Promise<WorkerWorkspaceResolution> {
  if (workspacePolicyRef === null) {
    const root = await usableDirectory(configuration.workspaceRoot);
    return root === null
      ? { outcome: "unavailable", reason: "root_unavailable", workingDirectory: null }
      : { outcome: "default_root", reason: "no_ref", workingDirectory: root };
  }
  const entry = configuration.workspaceCatalog?.find((candidate) => workspaceReferenceKey(candidate.workspacePolicyRef) === workspaceReferenceKey(workspacePolicyRef));
  if (entry !== undefined) {
    const mapped = await usableDirectory(entry.workspaceRoot);
    if (mapped !== null) return { outcome: "mapped", reason: "catalog_match", workingDirectory: mapped };
  }
  const root = await usableDirectory(configuration.workspaceRoot);
  if (root === null) return { outcome: "unavailable", reason: entry === undefined ? "catalog_miss" : "mapped_path_unavailable", workingDirectory: null };
  return { outcome: "fallback_root", reason: entry === undefined ? "catalog_miss" : "mapped_path_unavailable", workingDirectory: root };
}
