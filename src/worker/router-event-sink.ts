import { RouterEventRefusedError, RouterHttpTransportError, readRouterEventRefusal, readRouterWorkerTransportResponse, routerWorkerEndpoint, type RouterRunEventSink, type RouterRunLifecycleEnvelope } from "../shared/router-worker.js";

export interface RouterHttpEventSinkOptions { readonly endpoint: URL | string; readonly bearerToken: string; readonly fetch?: typeof fetch; readonly now?: () => Date; }

/**
 * Keeps a permanent refusal distinct from a transient outage: only a 409 whose
 * body carries one of the Router's own refusal codes becomes
 * `RouterEventRefusedError`. Every other status, and a 409 that cannot be
 * proven, is a `RouterHttpTransportError`.
 */
export class RouterHttpEventSink implements RouterRunEventSink {
  readonly #endpoint: URL; readonly #token: string; readonly #fetch: typeof fetch; readonly #now: () => Date;
  constructor(options: RouterHttpEventSinkOptions) { this.#endpoint = routerWorkerEndpoint(options.endpoint, "/v1/router/run-events"); this.#token = options.bearerToken; this.#fetch = options.fetch ?? fetch; this.#now = options.now ?? (() => new Date()); }
  async emit(event: RouterRunLifecycleEnvelope): Promise<void> { let response: Response; try { response = await this.#fetch(this.#endpoint, { method: "POST", headers: { authorization: `Bearer ${this.#token}`, "content-type": "application/json" }, body: JSON.stringify({ envelope: event, recordedAt: this.#now().toISOString() }) }); } catch { throw new RouterHttpTransportError(); } if (response.status !== 200 && response.status !== 202) { const refusal = await readRouterEventRefusal(response); if (refusal !== null) throw new RouterEventRefusedError(refusal); throw new RouterHttpTransportError(); } await readRouterWorkerTransportResponse(response); }
}
