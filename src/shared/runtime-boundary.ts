/** Runtime schemas and boundary types for the current contract version. */
import {
  runtimeContractVersion,
  validateRuntimeBoundaryMessage,
} from "@axisosme/axis-contracts/runtime";
import type { RunAcceptedEnvelope, RunStartEnvelope, SourceRef } from "@axisosme/axis-contracts/runtime";

export { validateRuntimeBoundaryMessage };
export type {
  LinearSourceRef,
  MethodRef,
  Run,
  RunAcceptedEnvelope,
  RunFailedEnvelope,
  RunProgressedEnvelope,
  RunStartEnvelope,
  RunSucceededEnvelope,
  RuntimeError,
  RuntimeIdentityGraph,
  SourceRef,
  Task,
  TaskAcceptedEnvelope,
  TaskFailedEnvelope,
  TaskProgressedEnvelope,
  TaskSucceededEnvelope,
} from "@axisosme/axis-contracts/runtime";

export const RUNTIME_BOUNDARY_VERSIONS = Object.freeze([runtimeContractVersion] as const);
export type RuntimeBoundaryVersion = typeof runtimeContractVersion;
export type Attempt = RunAcceptedEnvelope["attempt"];
export type SessionBinding = RunStartEnvelope["identity"]["sessionBinding"];
export type RuntimeSourceSystem = SourceRef["system"];
export type RuntimeBoundaryValidation = Awaited<ReturnType<typeof validateRuntimeBoundaryMessage>>;

/** Narrow an unvalidated value to the runtime contract version this boundary accepts. */
export function runtimeBoundaryVersion(value: unknown): RuntimeBoundaryVersion | null {
  return value === runtimeContractVersion ? runtimeContractVersion : null;
}

export function runtimeSourceSessionIdentity(sourceRef: SourceRef): { readonly system: RuntimeSourceSystem; readonly sessionId: string | undefined } {
  return { system: "linear", sessionId: sourceRef.agentSessionId };
}
