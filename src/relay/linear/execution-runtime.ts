import { createHash, randomUUID, timingSafeEqual } from "node:crypto";
import type { IncomingMessage, ServerResponse } from "node:http";
import { isDeepStrictEqual } from "node:util";
import {
  linearExecutionBodyLimitBytes, linearExecutionContractVersion, linearExecutionIdentitiesEqual,
  validateLinearExecutionCredentialRequest,
  type LinearExecutionCredentialRequest, type LinearExecutionCredentialResponse, type LinearExecutionIdentity,
} from "@axisosme/axis-contracts/linear-execution";
import type { Router } from "../../router/router.js";
import type { RouterNodePort } from "../../router/node-port.js";
import { NODE_COMMAND_CONTRACT_VERSION, nodeReceiptExpectation, validateNodeReceipt, type NodeCommandRequest, type NodePriorRoute } from "../../shared/router-node-commands.js";
import { parseStrictRouterWorkerJson, validWorkerExecutionIdentity, type WorkerExecutionIdentity } from "../../shared/router-worker.js";
import type { LinearExecutionAccess } from "./execution-access.js";
import type { LinearExecutionAuthorityPort, LinearExecutionGrant } from "./execution-access-contract.js";
import type { LinearExecutionInstallationPort } from "./installation.js";

export interface LinearExecutionConsent { readonly organizationId: string; readonly appUserId: string; readonly scope: "installation-workspace"; }
export interface LinearExecutionReceiver { readonly bearerToken: string; readonly identity: WorkerExecutionIdentity; }
export interface LinearExecutionRuntimeConfig { readonly grants: readonly LinearExecutionConsent[]; readonly receivers: readonly LinearExecutionReceiver[]; }
interface PreparedGrant {
  readonly grant: LinearExecutionGrant;
  readonly receiver: () => Promise<WorkerExecutionIdentity | null>;
  boundReceiver?: WorkerExecutionIdentity;
  closed: boolean;
}

/** Memory-only grants are captured at trusted dispatch. A request can only consume that prior approval. */
export class LinearExecutionAuthority implements LinearExecutionAuthorityPort {
  readonly #grants = new Map<string, PreparedGrant>();
  readonly #byRun = new Map<string, PreparedGrant>();
  #closed = false;
  constructor(readonly options: {
    readonly config: LinearExecutionRuntimeConfig;
    readonly router: Pick<Router, "readLinearExecution">;
    readonly installations: LinearExecutionInstallationPort;
  }) {}

  prepare(runId: string, receiver: () => Promise<WorkerExecutionIdentity | null>): void {
    if (this.#closed || this.#byRun.has(runId) || this.#grants.size >= 10_000) return;
    const current = this.options.router.readLinearExecution(runId);
    if (current === null) return;
    const consent = this.options.config.grants.find(entry => entry.organizationId === current.organizationId);
    if (consent === undefined) return;
    const installation = this.options.installations.executionAuthority(current.organizationId, consent.appUserId, current.sourceTeamId);
    if (installation === null || !installation.allowed) return;
    const grant: LinearExecutionGrant = Object.freeze({ ...current, executionId: randomUUID(), appUserId: consent.appUserId,
      installationRevision: installation.revision, state: "active", scope: consent.scope });
    const prepared = { grant, receiver, closed: false };
    this.#grants.set(grant.executionId, prepared); this.#byRun.set(runId, prepared);
  }

  /** Node command chooses the Worker. Read its authenticated, retained reservation before issuing. */
  prepareNode(request: NodeCommandRequest, port: RouterNodePort, prior: NodePriorRoute | undefined): void {
    // Both start and continue mint a new Run whose Worker needs its own
    // credential. Cancel and steer carry no Run start identity and create no grant.
    if (request.command.command === "cancel" || request.command.command === "steer") return;
    this.prepare(request.command.start.identity.run.runId, async () => {
      const query = { contractVersion: NODE_COMMAND_CONTRACT_VERSION, nodeRef: request.nodeRef, commandId: request.command.commandId };
      const result = await port.readReceipt(query);
      if (result.status !== 200) return null;
      const expectation = await nodeReceiptExpectation(request, prior);
      const receipt = await validateNodeReceipt(result.body, query, { authenticatedNode: result.authenticatedNode, expectation,
        ...(prior === undefined ? {} : { priorRoute: prior }) });
      if (receipt.status === "accepted") return receipt.evidence.target.identity;
      return receipt.status === "rejected" || receipt.status === "not_found" ? null : receipt.reservation.target.identity;
    });
  }

  resolve(executionId: string): LinearExecutionGrant | null {
    const record = this.#grants.get(executionId);
    if (this.#closed || record === undefined || record.closed) return null;
    const current = this.options.router.readLinearExecution(record.grant.runId);
    if (current === null || !linearExecutionIdentitiesEqual(current, record.grant) || current.sourceTeamId !== record.grant.sourceTeamId) {
      record.closed = true; return null;
    }
    return record.grant;
  }

  async authorize(execution: LinearExecutionIdentity, receiver: WorkerExecutionIdentity, operation: "issue" | "revoke"): Promise<string | null> {
    const record = this.#byRun.get(execution.runId);
    if (this.#closed || record === undefined || !linearExecutionIdentitiesEqual(record.grant, execution)) return null;
    try {
      const bound = record.boundReceiver ?? await record.receiver();
      if (bound === null || !isDeepStrictEqual(bound, receiver)) return null;
      record.boundReceiver = structuredClone(bound);
      if (operation === "revoke") { record.closed = true; return record.grant.executionId; }
      return this.resolve(record.grant.executionId) === null ? null : record.grant.executionId;
    } catch { return null; }
  }
  close(): void { this.#closed = true; this.#grants.clear(); this.#byRun.clear(); }
}

function hash(value: string): Buffer { return createHash("sha256").update(value).digest(); }

/** Protected credential transport. Tokens never enter ordinary Relay/Router/Worker payloads or logs. */
export class LinearExecutionCredentialEndpoint {
  constructor(readonly options: { readonly authority: LinearExecutionAuthority; readonly access: LinearExecutionAccess; readonly receivers: readonly LinearExecutionReceiver[] }) {}
  async handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
    response.setHeader("cache-control", "no-store"); response.setHeader("content-type", "application/json");
    const deny = (code: "linear.execution.invalid_request" | "linear.execution.execution_denied" | "linear.execution.unavailable", requestId = "invalid-request", status = 403) => {
      response.statusCode = status; response.end(JSON.stringify({ contractVersion: linearExecutionContractVersion, requestId, status: "denied", code }));
    };
    const authorization = request.headers.authorization;
    const token = typeof authorization === "string" && authorization.startsWith("Bearer ") ? authorization.slice(7) : "";
    const receiver = this.options.receivers.find(entry => timingSafeEqual(hash(entry.bearerToken), hash(token)));
    if (receiver === undefined) { request.resume(); deny("linear.execution.execution_denied"); return; }
    if (request.method !== "POST" || new URL(request.url ?? "/", "http://relay.invalid").search !== "" || request.headers["content-encoding"] !== undefined
      || !/^application\/json(?:\s*;\s*charset=utf-8)?$/iu.test(request.headers["content-type"] ?? "")) {
      request.resume(); deny("linear.execution.invalid_request", undefined, 400); return;
    }
    let input: LinearExecutionCredentialRequest;
    try {
      const chunks: Buffer[] = []; let length = 0;
      for await (const chunk of request) {
        length += Buffer.byteLength(chunk);
        if (length > linearExecutionBodyLimitBytes) throw new Error();
        chunks.push(Buffer.from(chunk));
      }
      input = validateLinearExecutionCredentialRequest(parseStrictRouterWorkerJson(Buffer.concat(chunks)));
    } catch { deny("linear.execution.invalid_request", undefined, 400); return; }
    try {
      const executionId = await this.options.authority.authorize(input.execution, receiver.identity, input.operation);
      if (executionId === null) { deny("linear.execution.execution_denied", input.requestId); return; }
      let reply: LinearExecutionCredentialResponse;
      if (input.operation === "revoke") {
        this.options.access.revokeExecution(executionId);
        reply = { contractVersion: linearExecutionContractVersion, requestId: input.requestId, status: "revoked", execution: input.execution };
      } else {
        const result = this.options.access.issue(executionId);
        if (!result.authorized) {
          response.statusCode = result.status;
          response.end(JSON.stringify({ contractVersion: linearExecutionContractVersion, requestId: input.requestId, status: "denied", code: result.error.code })); return;
        }
        reply = { contractVersion: linearExecutionContractVersion, requestId: input.requestId, status: "issued", execution: input.execution,
          credentialId: result.value.context.credentialId, credential: result.value.credential, expiresAt: result.value.context.expiresAt };
      }
      response.statusCode = 200; response.end(JSON.stringify(reply));
    } catch { deny("linear.execution.unavailable", input.requestId, 503); }
  }
}
