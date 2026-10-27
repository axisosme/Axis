import { createHash } from "node:crypto";

import { TeamBindingError } from "./team-bindings.js";
import { NODE_ROUTING_SNAPSHOT_VERSION, routerNodeBindingDigest, validateRouterNodeTargetSnapshot } from "../shared/node-routing-projection.js";
import type { ExecutionProfile, TeamBindingResolution } from "./team-bindings.js";
import type { ChannelRoutingScope, TenantRef } from "./channel-identity.js";
import type { RouterNodeTargetSnapshot } from "../shared/node-routing-projection.js";
import type { WorkerSelection } from "../shared/worker-selection.js";
import { validateWorkerSelection } from "../shared/worker-selection.js";

export const BINDING_RESOLVER_CONTRACT_VERSION = "1.0.0" as const;

export type ChannelBindingErrorCode =
  /** No binding exists for this scope, and no default applies. */
  | "unconfigured"
  /** A binding exists but is disabled. */
  | "disabled"
  /** The bound profile failed the operator's reference-liveness policy. */
  | "reference_missing"
  | "stale"
  | "incompatible"
  /** The resolved authority disagrees with the requested identity. */
  | "identity_mismatch"
  /** The store this adapter needs was not supplied. */
  | "resolver_unconfigured"
  /** The bound profile has no endpointRef, so no endpoint can serve it. */
  | "binding_reference_missing"
  /** A field of the bound profile is not a valid opaque identifier or versioned reference. */
  | "malformed"
  /** No configured endpoint matches the bound profile's endpointRef and identity. */
  | "endpoint_unknown";

const errorMessages: Readonly<Record<ChannelBindingErrorCode, string>> = {
  unconfigured: "Channel binding is not configured for this scope.",
  disabled: "Channel binding is disabled.",
  reference_missing: "Channel binding profile reference is missing.",
  stale: "Channel binding profile reference is stale.",
  incompatible: "Channel binding profile reference is incompatible.",
  identity_mismatch: "Channel binding authority does not match the requested identity.",
  resolver_unconfigured: "No binding source is configured for the requested channel.",
  binding_reference_missing: "Channel binding profile has no endpoint reference.",
  malformed: "Channel binding profile is malformed.",
  endpoint_unknown: "No configured endpoint matches the channel binding profile.",
};

export class ChannelBindingError extends Error {
  readonly code: ChannelBindingErrorCode;
  constructor(code: ChannelBindingErrorCode, options?: ErrorOptions) {
    super(errorMessages[code], options);
    this.code = code;
    this.name = "ChannelBindingError";
  }
}

/** The identity a resolution is requested for, closed and validated by the channel adapter. */
export interface ChannelBindingIdentity {
  readonly tenant: TenantRef;
  readonly scope: ChannelRoutingScope;
}

/** The neutral scope shape a resolution reports. */
export interface ResolvedRoutingScope {
  readonly system: TenantRef["system"];
  readonly tenantId: string;
  readonly scopeId: string;
}

/**
 * The persisted profile shape: `credentialRef` is dropped and `workspacePolicyRef`
 * is always present, `null` when unset. The digest is taken over this shape, not
 * over a raw `ExecutionProfile`.
 */
export interface ResolvedExecutionProfile {
  readonly nodeId: string;
  readonly workerId: string;
  readonly executorKind: string;
  readonly runtimeProfile: { readonly ref: string; readonly version: string };
  readonly endpointRef: { readonly ref: string; readonly version: string };
  readonly workspacePolicyRef: { readonly ref: string; readonly version: string } | null;
}

export interface ResolvedExecutionTarget {
  readonly contractVersion: typeof BINDING_RESOLVER_CONTRACT_VERSION;
  readonly scope: ResolvedRoutingScope;
  readonly bindingRevision: number;
  readonly profileDigest: string;
  readonly profile: ResolvedExecutionProfile;
  /** True when a Linear default binding supplied the profile rather than an exact binding. */
  readonly viaDefault: boolean;
}

/** The boundary Router depends on. */
export interface ChannelBindingResolver {
  resolveExecutionTarget(identity: ChannelBindingIdentity): ResolvedExecutionTarget;
  /** Resolve Task authority only. The returned snapshot contains no concrete Worker identity. */
  resolveNodeTarget(identity: ChannelBindingIdentity, workerSelection: WorkerSelection): RouterNodeTargetSnapshot;
}

/** The Linear team-binding surface this resolver needs. Structurally what `SqliteTeamBindingStore` provides. */
export interface LinearTeamBindingSource {
  resolve(input: { readonly scope: { readonly organizationId: string; readonly linearTeamId: string } }): TeamBindingResolution;
}
/**
 * A configured execution endpoint. Supplying these refuses a profile pointing at no
 * known endpoint as `endpoint_unknown`. The Node route resolves through
 * `nodeRoutes` and supplies none.
 */
export interface ResolverExecutionEndpoint {
  readonly endpointRef: { readonly ref: string; readonly version: string };
  readonly identity: { readonly nodeId: string; readonly workerId: string; readonly executorKind: string; readonly runtimeProfile: { readonly ref: string; readonly version: string } };
}

/** Volatile Router composition that maps one binding endpoint reference to one logical Node. */
export interface ResolverNodeRoute {
  readonly endpointRef: { readonly ref: string; readonly version: string };
  readonly nodeRef: { readonly ref: string; readonly version: string };
}

/**
 * Linear's default binding, reproduced from `RouterDefaultTeamBinding`.
 * `excludedTeamIds` opts specific teams out of the fallback.
 */
export interface LinearDefaultBinding {
  readonly profile: ExecutionProfile;
  readonly excludedTeamIds?: readonly string[];
}

export interface ChannelBindingResolverOptions {
  /** When supplied, every resolved target must match exactly one endpoint, as Router requires. */
  readonly endpoints?: readonly ResolverExecutionEndpoint[];
  /** Exact endpoint-reference to logical-Node projection for the Node Task path. */
  readonly nodeRoutes?: readonly ResolverNodeRoute[];
  readonly linearTeamBindings?: LinearTeamBindingSource;
  readonly linearDefaultBinding?: LinearDefaultBinding;
}

function assertConsistentSystem(identity: ChannelBindingIdentity): "linear" {
  if (identity.tenant.system !== "linear" || identity.scope.system !== "linear") throw new ChannelBindingError("identity_mismatch");
  return identity.tenant.system;
}

const identifierPattern = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u;
function resolverIdentifier(value: unknown): string {
  if (typeof value !== "string" || value.trim().length === 0 || !identifierPattern.test(value)) throw new ChannelBindingError("malformed");
  return value;
}
function resolverVersioned(value: unknown): { readonly ref: string; readonly version: string } {
  const candidate = value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
  if (candidate === undefined || Object.keys(candidate).sort().join(",") !== "ref,version") throw new ChannelBindingError("malformed");
  return Object.freeze({ ref: resolverIdentifier(candidate.ref), version: resolverIdentifier(candidate.version) });
}

/**
 * Requires an `endpointRef`, validates every identifier and versioned reference,
 * drops `credentialRef`, and normalizes an absent `workspacePolicyRef` to `null`,
 * so the result matches what Router's snapshot read-back accepts.
 */
function normalizedProfile(value: ExecutionProfile): ResolvedExecutionProfile {
  if (value.endpointRef === undefined) throw new ChannelBindingError("binding_reference_missing");
  return Object.freeze({
    nodeId: resolverIdentifier(value.nodeId),
    workerId: resolverIdentifier(value.workerId),
    executorKind: resolverIdentifier(value.executorKind),
    runtimeProfile: resolverVersioned(value.runtimeProfile),
    endpointRef: resolverVersioned(value.endpointRef),
    workspacePolicyRef: value.workspacePolicyRef === undefined ? null : resolverVersioned(value.workspacePolicyRef),
  });
}

function canonicalize(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalize).join(",")}]`;
  if (value !== null && typeof value === "object") {
    const object = value as Record<string, unknown>;
    return `{${Object.keys(object).sort().map((key) => `${JSON.stringify(key)}:${canonicalize(object[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}
/** The digest Router persists: sha256 over the *normalized* profile, not the raw one. */
function profileDigestOf(profile: ResolvedExecutionProfile): string {
  return `sha256:${createHash("sha256").update(canonicalize(profile)).digest("hex")}`;
}

function sameVersioned(left: { readonly ref: string; readonly version: string }, right: { readonly ref: string; readonly version: string }): boolean {
  return left.ref === right.ref && left.version === right.version;
}

function neutralScope(identity: ChannelBindingIdentity): ResolvedRoutingScope {
  return Object.freeze({ system: identity.tenant.system, tenantId: identity.tenant.tenantId, scopeId: identity.scope.scopeId });
}

/** Maps a store's error vocabulary onto this boundary's, preserving the distinction rather than flattening it. */
function bindingErrorCode(code: string): ChannelBindingErrorCode {
  switch (code) {
    case "unconfigured": case "disabled": case "reference_missing": case "stale": case "incompatible": case "identity_mismatch":
      return code;
    default:
      return "incompatible";
  }
}

/**
 * Builds the resolver from whichever channel sources are configured. A channel with
 * no source raises `resolver_unconfigured` rather than falling through to another
 * channel's store.
 */
export function channelBindingResolver(options: ChannelBindingResolverOptions = {}): ChannelBindingResolver {
  const excluded = new Set((options.linearDefaultBinding?.excludedTeamIds ?? []).map(resolverIdentifier));
  // Normalized once at construction, so the default's digest and shape match a stored
  // one and the caller's object cannot be mutated through a returned target.
  const defaultBinding = options.linearDefaultBinding === undefined ? undefined : (() => {
    const profile = normalizedProfile(options.linearDefaultBinding.profile);
    return { profile, profileDigest: profileDigestOf(profile) };
  })();
  const endpoints = options.endpoints;
  const nodeRoutes = Object.freeze((options.nodeRoutes ?? []).map((route) => Object.freeze({
    endpointRef: resolverVersioned(route.endpointRef),
    nodeRef: resolverVersioned(route.nodeRef),
  })));

  /** Router's `#endpoint`: exactly one configured endpoint must match both the ref and the identity. */
  function assertEndpoint(profile: ResolvedExecutionProfile): void {
    if (endpoints === undefined) return;
    const matches = endpoints.filter((endpoint) => sameVersioned(endpoint.endpointRef, profile.endpointRef)
      && endpoint.identity.nodeId === profile.nodeId && endpoint.identity.workerId === profile.workerId
      && endpoint.identity.executorKind === profile.executorKind && sameVersioned(endpoint.identity.runtimeProfile, profile.runtimeProfile));
    if (matches.length !== 1) throw new ChannelBindingError("endpoint_unknown");
  }

  function resolveLinearBinding(identity: ChannelBindingIdentity): ResolvedExecutionTarget {
    const source = options.linearTeamBindings;
    if (source === undefined) throw new ChannelBindingError("resolver_unconfigured");
    const scope = { organizationId: identity.tenant.tenantId, linearTeamId: identity.scope.scopeId };
    let resolved: TeamBindingResolution;
    try {
      resolved = source.resolve({ scope });
    } catch (error) {
      // Only an unconfigured team falls back, only when a default exists, and only when
      // the team is not excluded. A disabled or stale binding never falls back.
      const code = error instanceof TeamBindingError ? error.code : "incompatible";
      const fallback = defaultBinding;
      if (code === "unconfigured" && fallback !== undefined && !excluded.has(identity.scope.scopeId)) {
        return Object.freeze({
          contractVersion: BINDING_RESOLVER_CONTRACT_VERSION,
          scope: neutralScope(identity),
          bindingRevision: 1,
          profileDigest: fallback.profileDigest,
          profile: fallback.profile,
          viaDefault: true,
        });
      }
      throw new ChannelBindingError(bindingErrorCode(code), { cause: error });
    }
    // Normalized before the scope check, so a malformed profile raises `malformed`
    // (which throws rather than becoming a stored rejection) even when its scope
    // also mismatches.
    const profile = normalizedProfile(resolved.profile);
    if (resolved.scope.organizationId !== scope.organizationId || resolved.scope.linearTeamId !== scope.linearTeamId) throw new ChannelBindingError("identity_mismatch");
    return Object.freeze({
      contractVersion: BINDING_RESOLVER_CONTRACT_VERSION,
      scope: neutralScope(identity),
      bindingRevision: resolved.revision,
      // An exact binding persists the store's own digest; only the default is digested
      // here.
      profileDigest: resolved.profileDigest,
      profile,
      viaDefault: false,
    });
  }

  return Object.freeze({
    resolveExecutionTarget(identity: ChannelBindingIdentity): ResolvedExecutionTarget {
      assertConsistentSystem(identity);
      const resolved = resolveLinearBinding(identity);
      assertEndpoint(resolved.profile);
      return resolved;
    },

    resolveNodeTarget(identity: ChannelBindingIdentity, workerSelection: WorkerSelection): RouterNodeTargetSnapshot {
      assertConsistentSystem(identity);
      const resolved = resolveLinearBinding(identity);
      let selection: WorkerSelection;
      try { selection = validateWorkerSelection(workerSelection); }
      catch (error) { throw new ChannelBindingError("malformed", { cause: error }); }
      const matches = nodeRoutes.filter((route) => sameVersioned(route.endpointRef, resolved.profile.endpointRef));
      if (matches.length !== 1) throw new ChannelBindingError("endpoint_unknown");
      try {
        const fields: Omit<RouterNodeTargetSnapshot, "bindingDigest"> = {
          contractVersion: NODE_ROUTING_SNAPSHOT_VERSION,
          scope: resolved.scope,
          bindingRevision: resolved.bindingRevision,
          nodeRef: matches[0]!.nodeRef,
          workerSelection: selection,
          workspacePolicyRef: resolved.profile.workspacePolicyRef,
        };
        return validateRouterNodeTargetSnapshot({ ...fields, bindingDigest: routerNodeBindingDigest(fields) });
      } catch (error) {
        throw new ChannelBindingError("malformed", { cause: error });
      }
    },
  });
}
