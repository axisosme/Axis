import { createHash } from "node:crypto";
import { types } from "node:util";

/** A closed versioned reference. Values are opaque IDs, never URLs, paths, or credentials. */
export interface ExecutionProfileVersionedRef { readonly ref: string; readonly version: string; }

export interface ExecutionProfile {
  readonly nodeId: string;
  readonly workerId: string;
  readonly executorKind: string;
  readonly runtimeProfile: ExecutionProfileVersionedRef;
  readonly endpointRef?: ExecutionProfileVersionedRef;
  readonly credentialRef?: ExecutionProfileVersionedRef;
  readonly workspacePolicyRef?: ExecutionProfileVersionedRef;
}

export interface ExecutionProfileValidatorValid { readonly status: "valid"; }
export interface ExecutionProfileValidatorInvalid { readonly status: "invalid"; readonly code: "reference_missing" | "stale" | "incompatible"; }
export type ExecutionProfileValidatorResult = ExecutionProfileValidatorValid | ExecutionProfileValidatorInvalid;
export type ExecutionProfileValidator = (profile: Readonly<ExecutionProfile>) => ExecutionProfileValidatorResult;

export class ExecutionProfileInputError extends TypeError { constructor() { super("Execution profile input is malformed."); } }
function malformed(): never { throw new ExecutionProfileInputError(); }

const identifier = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const validatorCodes = new Set(["reference_missing", "stale", "incompatible"]);
const isObject = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === "object" && !Array.isArray(value);

function plainData(value: unknown): Record<string, unknown> {
  try {
    if (!isObject(value) || types.isProxy(value) || Object.getPrototypeOf(value) !== Object.prototype || Object.getOwnPropertySymbols(value).length !== 0) malformed();
    const descriptors = Object.getOwnPropertyDescriptors(value), actual = Object.keys(descriptors).sort();
    if (Object.values(descriptors).some((descriptor) => !descriptor.enumerable || descriptor.get !== undefined || descriptor.set !== undefined)) malformed();
    return Object.fromEntries(actual.map((key) => [key, descriptors[key]?.value]));
  } catch (error) { if (error instanceof ExecutionProfileInputError) throw error; malformed(); }
}
function opaque(value: unknown): string { if (typeof value !== "string" || !identifier.test(value)) malformed(); return value; }
function ref(value: unknown): ExecutionProfileVersionedRef {
  const data = plainData(value);
  const keys = Object.keys(data);
  if (keys.length !== 2 || !keys.includes("ref") || !keys.includes("version")) malformed();
  return { ref: opaque(data.ref), version: opaque(data.version) };
}
function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (isObject(value)) return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`).join(",")}}`;
  return JSON.stringify(value);
}
function copy<T>(value: T): T { return JSON.parse(JSON.stringify(value)) as T; }

/**
 * Normalizes an untrusted value into an `ExecutionProfile`, rejecting unknown
 * keys, accessors, proxies, symbols and non-opaque identifiers. Throws
 * `ExecutionProfileInputError` (a `TypeError`); callers translate that into
 * their own vocabulary.
 */
export function executionProfile(value: unknown): ExecutionProfile {
  const data = plainData(value);
  const required = ["nodeId", "workerId", "executorKind", "runtimeProfile"];
  const optional = ["endpointRef", "credentialRef", "workspacePolicyRef"];
  const keys = Object.keys(data);
  if (!required.every((key) => keys.includes(key)) || keys.some((key) => ![...required, ...optional].includes(key))) malformed();
  return {
    nodeId: opaque(data.nodeId), workerId: opaque(data.workerId), executorKind: opaque(data.executorKind), runtimeProfile: ref(data.runtimeProfile),
    ...(Object.hasOwn(data, "endpointRef") ? { endpointRef: ref(data.endpointRef) } : {}),
    ...(Object.hasOwn(data, "credentialRef") ? { credentialRef: ref(data.credentialRef) } : {}),
    ...(Object.hasOwn(data, "workspacePolicyRef") ? { workspacePolicyRef: ref(data.workspacePolicyRef) } : {}),
  };
}

/** The `sha256:`-prefixed profile digest both binding stores persist and compare. */
export function executionProfileDigest(value: ExecutionProfile): string { return `sha256:${createHash("sha256").update(canonical(value)).digest("hex")}`; }

/**
 * Runs an operator's validator and reduces its answer to a closed outcome.
 *
 * The validator's result must be a *closed fact*: exactly `{ status: "valid" }`
 * or exactly `{ status: "invalid", code }` with a known code. Anything else --
 * an extra key, an accessor, a proxy, a bogus code, a thrown exception, a
 * non-object -- is reported as `"incompatible"`, never as valid. Failing
 * closed here is the point: a validator that cannot state a well-formed fact
 * must not be read as approval.
 */
export function executionProfileValidatorOutcome(validator: ExecutionProfileValidator, profile: ExecutionProfile): "valid" | ExecutionProfileValidatorInvalid["code"] {
  try {
    const result: unknown = validator(copy(profile));
    if (!isObject(result) || types.isProxy(result) || Object.getPrototypeOf(result) !== Object.prototype) return "incompatible";
    const descriptors = Object.getOwnPropertyDescriptors(result);
    const keys = Object.keys(descriptors).sort();
    if (keys.some((key) => descriptors[key]?.get !== undefined || descriptors[key]?.set !== undefined)) return "incompatible";
    const status = descriptors.status?.value;
    if (status === "valid" && keys.length === 1 && keys[0] === "status") return "valid";
    const code = descriptors.code?.value;
    if (status === "invalid" && keys.length === 2 && keys[0] === "code" && keys[1] === "status" && typeof code === "string" && validatorCodes.has(code)) return code as ExecutionProfileValidatorInvalid["code"];
    return "incompatible";
  } catch { return "incompatible"; }
}
