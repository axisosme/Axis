/** Channel identity separates tenant, source and binding scope. */
import type { LinearRouterTaskRef } from "./router.js";

export type System = "linear";

/** Axis's own isolation/authority identity. Never a provider account ID reused verbatim. */
export interface TenantRef {
  readonly system: System;
  readonly tenantId: string;
}

/** Closed, provider-discriminated source/conversation subject. Linear-backed today. */
export interface LinearSourceRef {
  readonly system: "linear";
  readonly issueId: string;
  readonly agentSessionId?: string;
}

export type ChannelSourceRef = LinearSourceRef;

/** Closed, binding-resolution-only key. Deliberately narrower than SourceRef. */
export interface ChannelRoutingScope {
  readonly system: System;
  readonly scopeId: string;
}

export interface LinearChannelIdentity {
  readonly tenant: TenantRef;
  readonly source: LinearSourceRef;
  readonly scope: ChannelRoutingScope;
}

export class ChannelIdentityError extends Error {
  constructor(message: string) { super(message); this.name = "ChannelIdentityError"; }
}

function opaqueIdentifier(value: unknown): string {
  if (typeof value !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u.test(value)) {
    throw new ChannelIdentityError("expected an opaque identifier");
  }
  return value;
}

export function linearTenantRef(organizationId: string): TenantRef {
  return Object.freeze({ system: "linear" as const, tenantId: opaqueIdentifier(organizationId) });
}

/** Closed Linear source/conversation subject. `teamId` is NOT carried here -- it lives only in routingScope. */
export function linearSourceRef(input: { readonly issueId: string; readonly agentSessionId?: string }): LinearSourceRef {
  return Object.freeze({
    system: "linear" as const,
    issueId: opaqueIdentifier(input.issueId),
    ...(input.agentSessionId === undefined ? {} : { agentSessionId: opaqueIdentifier(input.agentSessionId) }),
  });
}

/** Closed Linear routing scope. Binding resolution only -- never used as a conversation/source key. */
export function linearRoutingScope(linearTeamId: string): ChannelRoutingScope {
  return Object.freeze({ system: "linear" as const, scopeId: opaqueIdentifier(linearTeamId) });
}

/**
 * Restates a Linear `RouterTaskRef` under the taxonomy. Lossless: `teamId` becomes
 * `scope.scopeId`, and `channelIdentityToRouterTaskRef` reproduces the input.
 */
export function linearChannelIdentity(ref: LinearRouterTaskRef): LinearChannelIdentity {
  return Object.freeze({
    tenant: linearTenantRef(ref.organizationId),
    source: linearSourceRef({ issueId: ref.issueId, agentSessionId: ref.agentSessionId }),
    scope: linearRoutingScope(ref.teamId),
  });
}

/** Inverse of `linearChannelIdentity`. Throws if the identity is not Linear-system across every identity it reads from. */
export function channelIdentityToRouterTaskRef(identity: LinearChannelIdentity): LinearRouterTaskRef {
  if (identity.tenant.system !== "linear" || identity.source.system !== "linear" || identity.scope.system !== "linear") {
    throw new ChannelIdentityError("expected a Linear-system identity");
  }
  if (identity.source.agentSessionId === undefined) {
    throw new ChannelIdentityError("RouterTaskRef requires agentSessionId");
  }
  return Object.freeze({
    organizationId: identity.tenant.tenantId,
    issueId: identity.source.issueId,
    teamId: identity.scope.scopeId,
    agentSessionId: identity.source.agentSessionId,
  });
}
