import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";

import { WorkerCommandError, WorkerCommandService } from "./command.js";
import type { CodexAppServerReadiness } from "../executor/codex.js";
import { WORKER_COMMAND_VERSION } from "../shared/router-worker.js";

export interface WorkerHttpOptions {
  readonly commandService: WorkerCommandService;
  /** Optional because non-Codex Worker variants retain their existing private command surface. */
  readonly readiness?: () => Promise<CodexAppServerReadiness> | CodexAppServerReadiness;
  readonly host?: string;
  readonly port?: number;
}

const WORKER_BODY_LIMIT_BYTES = 64 * 1024;

export interface WorkerHttpService {
  readonly server: Server;
  readonly url: URL;
  close(): Promise<void>;
}

type WorkerHttpErrorCode =
  | "axis.worker.malformed"
  | "axis.worker.unauthorized"
  | "axis.worker.policy_forbidden"
  | "axis.worker.identity_mismatch"
  | "axis.worker.method_not_allowed"
  | "axis.worker.body_too_large"
  | "axis.worker.unsupported_media"
  | "axis.worker.adapter_invalid"
  | "axis.worker.launch_failed"
  | "axis.worker.launch_ambiguous"
  | "axis.worker.intent_conflict"
  | "axis.worker.recovery_required"
  | "axis.worker.operation_unsupported"
  | "axis.worker.runtime_missing"
  | "axis.worker.runtime_incompatible"
  | "axis.worker.authentication_required"
  | "axis.worker.probe_timeout"
  | "axis.worker.capability_unavailable"
  | "axis.worker.workspace_unavailable"
  | "axis.worker.continuation_unresumable";

class WorkerHttpError extends Error {
  constructor(readonly code: WorkerHttpErrorCode, readonly statusCode: 400 | 401 | 403 | 413 | 415, message: string) {
    super(message);
  }
}

/**
 * The wire text for each refusal. Every entry must stay byte-identical to the same code's
 * `message` in `workerFailureTuples` (`../shared/router-worker.ts`): `classifiedFailure` in
 * `../node/worker-client.ts` compares `error.message === tuple.message` before it will treat a
 * refusal as a KNOWN Worker failure at all. A drift here does not surface as a mismatch --
 * the failure silently degrades to an unknown one, which is classified ambiguous, which on a
 * continuation means the conversation is told nothing and keeps its command slot.
 */
const safeMessages: Readonly<Record<WorkerHttpErrorCode, string>> = {
  "axis.worker.malformed": "The Worker run request is malformed.",
  "axis.worker.unauthorized": "Worker authorization is required.",
  "axis.worker.policy_forbidden": "The Worker run is not permitted by policy.",
  "axis.worker.identity_mismatch": "The Worker's configured identity does not match the command's execution target.",
  "axis.worker.method_not_allowed": "The HTTP method is not allowed.",
  "axis.worker.body_too_large": "The Worker run body exceeds the accepted limit.",
  "axis.worker.unsupported_media": "The Worker run media type is unsupported.",
  "axis.worker.adapter_invalid": "The executor adapter returned an invalid acceptance.",
  "axis.worker.launch_ambiguous": "The executor launch outcome is ambiguous.",
  "axis.worker.intent_conflict": "The Worker run ID is already bound to different start intent.",
  "axis.worker.recovery_required": "The Worker retained an observational run that cannot be relaunched.",
  "axis.worker.operation_unsupported": "The executor operation is not supported.",
  "axis.worker.runtime_missing": "The configured executor runtime is unavailable.",
  "axis.worker.runtime_incompatible": "The configured executor runtime is incompatible.",
  "axis.worker.authentication_required": "The configured executor runtime requires authentication.",
  "axis.worker.probe_timeout": "The executor preflight probe did not respond in time.",
  "axis.worker.capability_unavailable": "The configured executor capability is unavailable.",
  "axis.worker.workspace_unavailable": "No trusted Worker workspace is available for this run.",
  "axis.worker.launch_failed": "The executor launch failed before execution began.",
  "axis.worker.continuation_unresumable": "The prior native session for this continuation cannot be resumed.",
};

function sendJson(response: ServerResponse, status: number, body: unknown): void {
  const encoded = JSON.stringify(body);
  response.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(encoded),
    "cache-control": "no-store",
  });
  response.end(encoded);
}

function sendFailure(response: ServerResponse, code: WorkerHttpErrorCode, status: number): void {
  sendJson(response, status, { error: { code, message: safeMessages[code] } });
}

function rawHeaderValues(request: IncomingMessage, name: string): readonly string[] {
  const values: string[] = [];
  for (let index = 0; index < request.rawHeaders.length; index += 2) {
    if (request.rawHeaders[index]?.toLowerCase() === name) values.push(request.rawHeaders[index + 1] ?? "");
  }
  return values;
}

function authenticate(request: IncomingMessage, worker: { authenticate(candidate: string): boolean }): void {
  const values = rawHeaderValues(request, "authorization");
  if (values.length !== 1) throw new WorkerHttpError("axis.worker.unauthorized", 401, safeMessages["axis.worker.unauthorized"]);
  const authorization = values[0];
  const bearerPrefix = "Bearer ";
  if (authorization === undefined || !authorization.startsWith(bearerPrefix) || authorization.length === bearerPrefix.length) {
    throw new WorkerHttpError("axis.worker.unauthorized", 401, safeMessages["axis.worker.unauthorized"]);
  }
  if (!worker.authenticate(authorization.slice(bearerPrefix.length))) {
    throw new WorkerHttpError("axis.worker.unauthorized", 401, safeMessages["axis.worker.unauthorized"]);
  }
}

function assertMedia(request: IncomingMessage): void {
  const contentType = rawHeaderValues(request, "content-type");
  const normalized = (contentType[0] ?? "").trim().toLowerCase();
  if (contentType.length !== 1 || !/^application\/json(?:\s*;\s*charset=utf-8)?$/u.test(normalized)) {
    throw new WorkerHttpError("axis.worker.unsupported_media", 415, safeMessages["axis.worker.unsupported_media"]);
  }
  if (rawHeaderValues(request, "content-encoding").length !== 0) {
    throw new WorkerHttpError("axis.worker.unsupported_media", 415, safeMessages["axis.worker.unsupported_media"]);
  }
}

async function readBody(request: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let bytes = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    bytes += buffer.length;
    if (bytes > WORKER_BODY_LIMIT_BYTES) {
      throw new WorkerHttpError("axis.worker.body_too_large", 413, safeMessages["axis.worker.body_too_large"]);
    }
    chunks.push(buffer);
  }
  return Buffer.concat(chunks);
}

class StrictJson {
  #index = 0;
  readonly #text: string;

  constructor(text: string) {
    this.#text = text;
  }

  parse(): unknown {
    this.space();
    this.value();
    this.space();
    if (this.#index !== this.#text.length) this.invalid();
    return JSON.parse(this.#text) as unknown;
  }

  private value(): void {
    const character = this.#text[this.#index];
    if (character === "{") return this.object();
    if (character === "[") return this.array();
    if (character === "\"") return void this.string();
    if (this.#text.startsWith("true", this.#index)) return void (this.#index += 4);
    if (this.#text.startsWith("false", this.#index)) return void (this.#index += 5);
    if (this.#text.startsWith("null", this.#index)) return void (this.#index += 4);
    this.number();
  }

  private object(): void {
    this.#index += 1;
    this.space();
    const keys = new Set<string>();
    if (this.#text[this.#index] === "}") return void (this.#index += 1);
    for (;;) {
      this.space();
      if (this.#text[this.#index] !== "\"") this.invalid();
      const key = this.string();
      if (keys.has(key)) this.invalid();
      keys.add(key);
      this.space();
      if (this.#text[this.#index] !== ":") this.invalid();
      this.#index += 1;
      this.space();
      this.value();
      this.space();
      const separator = this.#text[this.#index];
      if (separator === "}") return void (this.#index += 1);
      if (separator !== ",") this.invalid();
      this.#index += 1;
    }
  }

  private array(): void {
    this.#index += 1;
    this.space();
    if (this.#text[this.#index] === "]") return void (this.#index += 1);
    for (;;) {
      this.value();
      this.space();
      const separator = this.#text[this.#index];
      if (separator === "]") return void (this.#index += 1);
      if (separator !== ",") this.invalid();
      this.#index += 1;
      this.space();
    }
  }

  private string(): string {
    const start = this.#index;
    this.#index += 1;
    while (this.#index < this.#text.length) {
      const character = this.#text[this.#index] ?? "";
      if (character === "\"") {
        this.#index += 1;
        return JSON.parse(this.#text.slice(start, this.#index)) as string;
      }
      if (character === "\\") {
        this.#index += 1;
        const escape = this.#text[this.#index];
        if (escape === "u") {
          const hexadecimal = this.#text.slice(this.#index + 1, this.#index + 5);
          if (!/^[0-9a-f]{4}$/iu.test(hexadecimal)) this.invalid();
          this.#index += 5;
          continue;
        }
        if (escape === undefined || !"\"\\/bfnrt".includes(escape)) this.invalid();
        this.#index += 1;
        continue;
      }
      if (character < " ") this.invalid();
      this.#index += 1;
    }
    this.invalid();
  }

  private number(): void {
    const match = /-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?/u.exec(this.#text.slice(this.#index));
    if (match?.[0] === undefined) this.invalid();
    this.#index += match[0].length;
  }

  private space(): void {
    while (/\s/u.test(this.#text[this.#index] ?? "")) this.#index += 1;
  }

  private invalid(): never {
    throw new WorkerHttpError("axis.worker.malformed", 400, safeMessages["axis.worker.malformed"]);
  }
}

function parseStrictJson(body: Buffer): unknown {
  let text: string;
  try {
    text = new TextDecoder("utf-8", { fatal: true }).decode(body);
  } catch {
    throw new WorkerHttpError("axis.worker.malformed", 400, safeMessages["axis.worker.malformed"]);
  }
  try {
    return new StrictJson(text).parse();
  } catch (error) {
    if (error instanceof WorkerHttpError) throw error;
    throw new WorkerHttpError("axis.worker.malformed", 400, safeMessages["axis.worker.malformed"]);
  }
}

function workerFailure(error: unknown): { readonly code: WorkerHttpErrorCode; readonly status: number } {
  if (error instanceof WorkerHttpError) return { code: error.code, status: error.statusCode };
  if (error instanceof WorkerCommandError) return { code: error.code, status: error.statusCode };
  return { code: "axis.worker.launch_ambiguous", status: 503 };
}

function unavailableReadiness(): CodexAppServerReadiness {
  return {
    status: "degraded",
    modelAvailability: "not_checked",
    profiles: {
      explorer: { availability: "unavailable", attempt: "not_attempted" },
      architect: { availability: "unavailable", attempt: "not_attempted" },
      reviewer: { availability: "unavailable", attempt: "not_attempted" },
      worker: { availability: "unavailable", attempt: "not_attempted" },
    },
    degradedReasons: ["app_server_unavailable"],
  };
}

export async function startWorkerHttpService(options: WorkerHttpOptions): Promise<WorkerHttpService> {
  const server = createServer((request, response) => {
    void handle(request, response).catch((error: unknown) => {
      if (!response.headersSent) {
        const failure = workerFailure(error);
        sendFailure(response, failure.code, failure.status);
      } else response.destroy();
    });
  });

  async function handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
    const url = new URL(request.url ?? "/", "http://worker.invalid");
    if (url.pathname === "/v1/worker/readiness" && options.readiness !== undefined) {
      if (request.method !== "GET") {
        response.setHeader("allow", "GET");
        sendFailure(response, "axis.worker.method_not_allowed", 405);
        return;
      }
      authenticate(request, options.commandService);
      if (url.search !== "") throw new WorkerHttpError("axis.worker.malformed", 400, safeMessages["axis.worker.malformed"]);
      let readiness: CodexAppServerReadiness;
      try { readiness = await options.readiness(); } catch { readiness = unavailableReadiness(); }
      sendJson(response, readiness.status === "ready" ? 200 : 503, readiness);
      return;
    }
    // The read-only command record. It answers only with the response this Worker already
    // recorded for that command -- the same bytes its dispatch returned -- or `not_recorded`. It never
    // begins, reserves or executes a command, and exposes no other Worker state.
    const recorded = /^\/v1\/worker\/commands\/([^/]+)$/u.exec(url.pathname);
    if (recorded !== null) {
      if (request.method !== "GET") {
        response.setHeader("allow", "GET");
        sendFailure(response, "axis.worker.method_not_allowed", 405);
        return;
      }
      authenticate(request, options.commandService);
      if (url.search !== "") throw new WorkerHttpError("axis.worker.malformed", 400, safeMessages["axis.worker.malformed"]);
      let commandId: string;
      try { commandId = decodeURIComponent(recorded[1]!); } catch { throw new WorkerHttpError("axis.worker.malformed", 400, safeMessages["axis.worker.malformed"]); }
      const recordedResponse = options.commandService.readRecordedResponse(commandId);
      sendJson(response, 200, recordedResponse === undefined ? { commandId, status: "not_recorded" } : { commandId, status: "recorded", response: recordedResponse });
      return;
    }
    const commandVersion = url.pathname === "/v1/worker/commands" ? WORKER_COMMAND_VERSION : undefined;
    if (commandVersion === undefined) {
      sendFailure(response, "axis.worker.malformed", 404);
      return;
    }
    if (request.method !== "POST") {
      response.setHeader("allow", "POST");
      sendFailure(response, "axis.worker.method_not_allowed", 405);
      return;
    }
    authenticate(request, options.commandService);
    if (url.search !== "") throw new WorkerHttpError("axis.worker.malformed", 400, safeMessages["axis.worker.malformed"]);
    assertMedia(request);
    const body = await readBody(request);
    const result = await options.commandService.execute(parseStrictJson(body), commandVersion);
    sendJson(response, result.replayed ? 200 : 202, result.response);
  }

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(options.port ?? 0, options.host ?? "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("The Worker listener address is unavailable.");
  const url = new URL(`http://${address.address.includes(":") ? `[${address.address}]` : address.address}:${address.port}`);
  return {
    server,
    url,
    close: async () => new Promise<void>((resolve, reject) => server.close((error) => error === undefined ? resolve() : reject(error))),
  };
}
