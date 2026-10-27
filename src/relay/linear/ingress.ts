import { RelayError } from "../errors.js";
import { parseLinearWebhook, verifyPayloadTimestamp, webhookSemanticDigest } from "./payload.js";
import { sha256, verifyLinearWebhook, type EnvelopeCipher } from "../security.js";
import type { LinearHeaders } from "../types.js";
import type { LinearInboxWriter } from "./inbox.js";

/**
 * Linear webhook ingress: verify, parse, encrypt and persist exactly one inbox row, then answer
 * `{accepted, duplicate}`. Processing is asynchronous behind `LinearInboxPort#claim`; nothing here
 * dispatches, authorizes or reads Linear.
 *
 * The order is the security boundary:
 * 1. `verifyLinearWebhook`: HMAC over the raw bytes, delivery id and header timestamp, so an
 *    unauthenticated body never reaches the JSON parser.
 * 2. `parseLinearWebhook`: an unknown envelope becomes `Unsupported`, so it is still recorded as quarantined.
 * 3. `verifyPayloadTimestamp`: the body's signed `webhookTimestamp`; the header timestamp is not covered
 *    by the HMAC, so it alone proves nothing about replay age.
 * 4. Persist, encrypted.
 *
 * The body-size limit belongs to the HTTP layer (`../http.ts`), which can refuse bytes before buffering.
 */

export interface LinearIngressOptions {
  /** Only the accepting half of the inbox: this module must not reach a processing method. */
  readonly inbox: LinearInboxWriter;
  /** Encrypts the raw body. The inbox never receives plaintext. */
  readonly cipher: EnvelopeCipher;
  readonly signingSecret: string;
  readonly now: () => Date;
  /** Replay window for both the header and the signed body timestamp; validated in `ingestLinearWebhook`. */
  readonly toleranceMs?: number;
}
export interface LinearIngressRequest {
  readonly headers: LinearHeaders;
  readonly rawBody: Buffer;
}

/** `accepted` is always true when this returns: every rejection is a thrown `RelayError` carrying its status. */
export interface LinearIngressResult {
  readonly accepted: true;
  readonly duplicate: boolean;
}

/**
 * Verifies, parses and durably records one Linear webhook delivery.
 *
 * Throws `RelayError` with `linear.ingress.invalid_signature` (401),
 * `linear.ingress.stale_timestamp` (401), `linear.ingress.malformed` (400),
 * `linear.ingress.delivery_conflict` (409), or
 * `linear.ingress.storage_unavailable` (503, retryable).
 */
export function ingestLinearWebhook(options: LinearIngressOptions, request: LinearIngressRequest): LinearIngressResult {
  const nowMs = options.now().getTime();
  // Both freshness checks compare `Math.abs(now - ts) > toleranceMs`, which is false for `NaN`, so a
  // non-finite tolerance would remove the replay window entirely.
  if (options.toleranceMs !== undefined && (!Number.isFinite(options.toleranceMs) || options.toleranceMs < 0)) {
    throw new RelayError("linear.ingress.stale_timestamp", "The webhook timestamp is outside the accepted window.", 401);
  }
  // `undefined` selects both callees' shared default window.
  verifyLinearWebhook(request.rawBody, request.headers, options.signingSecret, nowMs, options.toleranceMs);

  const payload = parseLinearWebhook(request.rawBody);
  verifyPayloadTimestamp(payload, nowMs, options.toleranceMs);

  let acceptance;
  try {
    acceptance = options.inbox.accept({
      deliveryId: request.headers.deliveryId,
      webhookId: payload.webhookId,
      // The `Linear-Event` header as supplied; never compared against the parsed body type.
      eventName: request.headers.event,
      bodyDigest: sha256(request.rawBody),
      semanticDigest: webhookSemanticDigest(request.rawBody),
      organizationId: payload.organizationId,
      payloadCiphertext: options.cipher.encrypt(request.rawBody.toString("utf8")),
      unsupportedShape: payload.type === "Unsupported"
        ? { type: payload.originalType, action: payload.originalAction }
        : null,
      // Every `AgentSessionEvent` has a required, non-blank session id; other envelopes have no session.
      sessionKey: payload.type === "AgentSessionEvent" ? payload.agentSession.id : null,
      preempts: payload.type === "AgentSessionEvent" && payload.agentActivity?.signal === "stop",
    });
  } catch (error) {
    // Classified storage failures are already `RelayError`s; anything else must not leak its message.
    if (error instanceof RelayError) throw error;
    throw new RelayError("linear.ingress.storage_unavailable", "The webhook could not be accepted durably.", 503, true);
  }
  return { accepted: true, duplicate: acceptance.duplicate };
}
