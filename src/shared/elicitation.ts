import { createHash } from "node:crypto";

import type { StructuredElicitation as ContractsStructuredElicitation } from "@axisosme/axis-contracts/runtime";

export const ELICITATION_CONTRACT_VERSION = "1.0.0-fixed-choice" as const;
export const ELICITATION_OTHER_CONTRACT_VERSION = "1.0.0-other" as const;

export interface ElicitationOption {
  readonly optionId: string;
  readonly replyValue: string;
  readonly label: string;
  readonly description: string;
}

/** Axis-owned, provider-neutral decision projected across private boundaries. */
export interface ElicitationOtherOption {
  readonly optionId: string;
  readonly replyValue: string;
}

interface ElicitationQuestion {
  readonly decisionId: string;
  readonly sourceEventId: string;
  readonly recommendedOptionId: string;
  readonly header: string;
  readonly question: string;
  readonly options: readonly [ElicitationOption, ElicitationOption] | readonly [ElicitationOption, ElicitationOption, ElicitationOption];
}

/** Fixed-choice and Other requests have distinct closed shapes. */
export type StructuredElicitation = ElicitationQuestion & (
  | { readonly contractVersion: typeof ELICITATION_CONTRACT_VERSION; readonly other?: never }
  | { readonly contractVersion: typeof ELICITATION_OTHER_CONTRACT_VERSION; readonly other: ElicitationOtherOption }
);

type JsonRecord = Readonly<Record<string, unknown>>;
function record(value: unknown): JsonRecord | null { return value !== null && typeof value === "object" && !Array.isArray(value) ? value as JsonRecord : null; }
function exact(value: JsonRecord, keys: readonly string[]): boolean { const actual = Object.keys(value).sort(), expected = [...keys].sort(); return actual.length === expected.length && actual.every((key, index) => key === expected[index]); }
function containsProtocolRelativeAbsolutePath(value: string): boolean {
  for (let index = value.indexOf("//"); index >= 0; index = value.indexOf("//", index + 2)) {
    if (index + 2 >= value.length) continue;
    const prefix = value.slice(0, index);
    if (/[A-Za-z][A-Za-z0-9+.-]*:$/u.test(prefix)) continue;
    if (index === 0 || !/[\p{L}\p{N}]/u.test(value[index - 1]!)) return true;
  }
  return false;
}
function containsUnsafeDisplayContent(value: string): boolean {
  return /(?<![\p{L}\p{N}/])\/(?!\/)[^\s)\]}>"']+/u.test(value)
    || /(?<![\p{L}\p{N}])[A-Za-z]:[\\/][^\s)\]}>"']+/u.test(value)
    || /(?<![\p{L}\p{N}\\])\\\\[^\\\s)\]}>"']+[\\/][^\s)\]}>"']+/u.test(value)
    || containsProtocolRelativeAbsolutePath(value)
    || /\bfile:\/\/[^\s)\]}>"']*/iu.test(value)
    || /\b[A-Za-z][A-Za-z0-9+.-]*:(?:\/\/)?[^/\s?#]*@/u.test(value)
    || /(?<![\p{L}\p{N}/])\/\/[^/\s?#]*@/u.test(value)
    || /\b(?:authorization\s*:\s*(?:bearer|basic)\s+\S+|bearer\s+[A-Za-z0-9._~+/-]{8,}|(?:api[_-]?key|access[_-]?token|refresh[_-]?token|client[_-]?secret|code[_-]?verifier|cookie|password|private[_-]?key|secret|token)\s*[:=]\s*\S+)/iu.test(value)
    || /\b(?:sk_(?:live|test)_[A-Za-z0-9]{16,}|ghp_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|AKIA[0-9A-Z]{16}|xox[a-zA-Z]-[A-Za-z0-9-]{10,}|glpat-[A-Za-z0-9_-]{16,})\b/u.test(value)
    || /axlea1_[A-Za-z0-9_-]{43}/u.test(value)
    || /-----BEGIN (?:[A-Z0-9 ]+ )?PRIVATE KEY-----/u.test(value);
}
function safeText(value: unknown, maximum: number): string | null {
  if (typeof value !== "string") return null;
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) { const next = value.charCodeAt(++index); if (!(next >= 0xdc00 && next <= 0xdfff)) return null; }
    else if (code >= 0xdc00 && code <= 0xdfff) return null;
  }
  if (/[\u0000-\u001F\u007F-\u009F\u061C\u200E\u200F\u202A-\u202E\u2066-\u2069]/u.test(value)) return null;
  const normalized = value.normalize("NFC").replace(/\s+/gu, " ").trim();
  return normalized.length > 0 && [...normalized].length <= maximum && !containsUnsafeDisplayContent(normalized) ? normalized : null;
}
function identifier(value: unknown): string | null { return typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u.test(value) ? value : null; }
function hashedIdentifier(value: unknown, prefix: "axis-decision" | "axis-elicitation" | "axis-option"): string | null {
  return typeof value === "string" && new RegExp(`^${prefix}-[a-f0-9]{48}$`, "u").test(value) ? value : null;
}
function opaque(prefix: string, material: string): string { return `${prefix}-${createHash("sha256").update(material).digest("hex").slice(0, 48)}`; }
export function deterministicElicitationReplyValue(decisionId: string, optionId: string): string { return opaque("axis-reply", `${decisionId}\u0000${optionId}`); }
export function deterministicElicitationOtherOption(decisionId: string): ElicitationOtherOption {
  const optionId = opaque("axis-option", `${decisionId}\u0000other`);
  return { optionId, replyValue: deterministicElicitationReplyValue(decisionId, optionId) };
}
function escapeMarkdown(value: string): string { return value.replace(/[\\`*_[\]{}()#+.!|<>-]/gu, "\\$&"); }

/**
 * Digest of an elicitation in either the Axis-owned shape or the contracts package's generated
 * shape. Stable under the observe tick's replay because every read parses the same immutable
 * `router_receipts.relay_json` text, so key order is identical on every call.
 */
export function elicitationDigest(elicitation: StructuredElicitation | ContractsStructuredElicitation): `sha256:${string}` {
  return `sha256:${createHash("sha256").update(JSON.stringify(elicitation)).digest("hex")}`;
}

export function restoreElicitation(elicitation: ContractsStructuredElicitation): StructuredElicitation {
  return {
    contractVersion: "other" in elicitation ? ELICITATION_OTHER_CONTRACT_VERSION : ELICITATION_CONTRACT_VERSION,
    ...elicitation,
  } as unknown as StructuredElicitation;
}

export function deterministicElicitationIds(binding: { readonly threadId: string; readonly turnId: string }, itemId: string, questionId: string): { readonly decisionId: string; readonly sourceEventId: string } {
  const boundItem = `${binding.threadId}\u0000${binding.turnId}\u0000${itemId}`;
  return {
    decisionId: opaque("axis-decision", `${boundItem}\u0000${questionId}`),
    sourceEventId: opaque("axis-elicitation", boundItem),
  };
}

/** Validate the exact blocking request_user_input subset supported by Axis. */
export function elicitationFromCodexRequest(value: unknown, binding: { readonly threadId: string; readonly turnId: string }): StructuredElicitation | null {
  const params = record(value);
  if (params === null || !exact(params, ["threadId", "turnId", "itemId", "questions", "isBlocking", "autoResolutionMs"])) return null;
  const threadId = identifier(params.threadId), turnId = identifier(params.turnId), itemId = identifier(params.itemId);
  if (threadId !== binding.threadId || turnId !== binding.turnId || itemId === null || params.isBlocking !== true || params.autoResolutionMs !== null || !Array.isArray(params.questions) || params.questions.length !== 1) return null;
  const question = record(params.questions[0]);
  if (question === null || !exact(question, ["id", "header", "question", "isOther", "isSecret", "options"]) || typeof question.isOther !== "boolean" || question.isSecret !== false || !Array.isArray(question.options) || (question.options.length !== 2 && question.options.length !== 3)) return null;
  const questionId = identifier(question.id), header = safeText(question.header, 64), prompt = safeText(question.question, 512);
  if (questionId === null || header === null || prompt === null) return null;
  const ids = deterministicElicitationIds({ threadId, turnId }, itemId, questionId);
  const options: ElicitationOption[] = [];
  const labels = new Set<string>();
  for (let index = 0; index < question.options.length; index += 1) {
    const option = record(question.options[index]);
    if (option === null || !exact(option, ["label", "description"])) return null;
    const label = safeText(option.label, 80), description = safeText(option.description, 256);
    if (label === null || description === null || labels.has(label)) return null;
    labels.add(label);
    const optionId = opaque("axis-option", `${threadId}\u0000${turnId}\u0000${itemId}\u0000${questionId}\u0000${index}\u0000${label}`);
    options.push({ optionId, replyValue: deterministicElicitationReplyValue(ids.decisionId, optionId), label, description });
  }
  return validateStructuredElicitation({
    ...(question.isOther
      ? { contractVersion: ELICITATION_OTHER_CONTRACT_VERSION, other: deterministicElicitationOtherOption(ids.decisionId) }
      : { contractVersion: ELICITATION_CONTRACT_VERSION }),
    ...ids,
    recommendedOptionId: options[0]!.optionId,
    header,
    question: prompt,
    options: options as unknown as StructuredElicitation["options"],
  });
}

export function validateStructuredElicitation(value: unknown): StructuredElicitation | null {
  const root = record(value);
  if (root === null) return null;
  const withOther = root.contractVersion === ELICITATION_OTHER_CONTRACT_VERSION;
  if (!exact(root, ["contractVersion", "decisionId", "sourceEventId", "recommendedOptionId", "header", "question", "options", ...(withOther ? ["other"] : [])]) || (!withOther && root.contractVersion !== ELICITATION_CONTRACT_VERSION)) return null;
  const decisionId = hashedIdentifier(root.decisionId, "axis-decision"), sourceEventId = hashedIdentifier(root.sourceEventId, "axis-elicitation"), header = safeText(root.header, 64), question = safeText(root.question, 512);
  if (decisionId === null || sourceEventId === null || header === null || question === null || !Array.isArray(root.options) || (root.options.length !== 2 && root.options.length !== 3)) return null;
  const options: ElicitationOption[] = [];
  const ids = new Set<string>(), replies = new Set<string>(), labels = new Set<string>();
  for (const value of root.options) {
    const option = record(value);
    if (option === null || !exact(option, ["optionId", "replyValue", "label", "description"])) return null;
    const optionId = hashedIdentifier(option.optionId, "axis-option"), reply = typeof option.replyValue === "string" && /^axis-reply-[a-f0-9]{48}$/u.test(option.replyValue) ? option.replyValue : null, label = safeText(option.label, 80), description = safeText(option.description, 256);
    if (optionId === null || reply === null || reply !== deterministicElicitationReplyValue(decisionId, optionId) || label === null || description === null || ids.has(optionId) || replies.has(reply) || labels.has(label)) return null;
    ids.add(optionId); replies.add(reply); labels.add(label); options.push({ optionId, replyValue: reply, label, description });
  }
  const recommendedOptionId = hashedIdentifier(root.recommendedOptionId, "axis-option");
  if (recommendedOptionId === null || recommendedOptionId !== options[0]?.optionId) return null;
  const questionFields = { decisionId, sourceEventId, recommendedOptionId, header, question, options: options as unknown as StructuredElicitation["options"] };
  if (!withOther) return { contractVersion: ELICITATION_CONTRACT_VERSION, ...questionFields };
  const other = record(root.other), expectedOther = deterministicElicitationOtherOption(decisionId);
  if (other === null || !exact(other, ["optionId", "replyValue"]) || other.optionId !== expectedOther.optionId || other.replyValue !== expectedOther.replyValue || ids.has(expectedOther.optionId) || replies.has(expectedOther.replyValue)) return null;
  return { contractVersion: ELICITATION_OTHER_CONTRACT_VERSION, ...questionFields, other: expectedOther };
}

export function validateCanonicalLinearUserUrl(value: unknown): string | null {
  if (typeof value !== "string" || value.length > 2_048) return null;
  try { const url = new URL(value); return url.protocol === "https:" && url.username === "" && url.password === "" && url.search === "" && url.hash === "" && (url.hostname === "linear.app" || url.hostname.endsWith(".linear.app")) ? url.toString() : null; } catch { return null; }
}

/**
 * The native `signalMetadata.options` projection for Linear's `select` signal: each option becomes
 * `{ label, value }`, with `value` the decision-bound token. A selection comes back as an ordinary
 * `prompt` Activity, which `normalizeElicitationDecisionReply` resolves exactly like a typed token.
 * `other` is absent because its reply is a token prefix followed by free text. Throws on an invalid
 * projection, matching `renderElicitationMarkdown`.
 */
export function nativeElicitationOptions(elicitationValue: StructuredElicitation): readonly { readonly label: string; readonly value: string }[] {
  const elicitation = validateStructuredElicitation(elicitationValue);
  if (elicitation === null) throw new Error("The elicitation projection is invalid.");
  return Object.freeze(elicitation.options.map((option) => Object.freeze({ label: option.label, value: option.replyValue })));
}

export function renderElicitationMarkdown(elicitationValue: StructuredElicitation, ownerUrlValue: string): string {
  const elicitation = validateStructuredElicitation(elicitationValue), ownerUrl = validateCanonicalLinearUserUrl(ownerUrlValue);
  if (elicitation === null || ownerUrl === null) throw new Error("The elicitation projection is invalid.");
  const recommended = elicitation.options[0];
  const options = elicitation.options.map((option) => `- **${escapeMarkdown(option.label)}**${option.optionId === elicitation.recommendedOptionId ? " (Recommended)" : ""} — ${escapeMarkdown(option.description)}`).join("\n");
  const other = elicitation.other === undefined ? "" : `\n- **Other** — Provide your own non-secret answer.\n  Reply: \`${elicitation.other.replyValue} your answer\``;
  const instruction = elicitation.other === undefined
    ? "Choose one of the options above."
    : "Choose one of the options above, or reply with the Other value followed by a space and your answer (up to 4096 characters).";
  return `### ${escapeMarkdown(elicitation.header)}\n\n${escapeMarkdown(elicitation.question)}\n\n**Recommended:** ${escapeMarkdown(recommended.label)}\n\n${options}${other}\n\nOnly the decision owner ${ownerUrl} may reply. ${instruction}`;
}

export function selectElicitationOption(elicitation: StructuredElicitation, reply: string): ElicitationOption | null {
  return elicitation.options.find((option) => option.replyValue === reply) ?? null;
}

export type ElicitationReply =
  | { readonly kind: "option"; readonly optionId: string; readonly label: string }
  | { readonly kind: "other"; readonly optionId: string; readonly label: "Other"; readonly answer: string };

/** Private input, never display text: preserve multiline answers and local paths. */
function freeformAnswer(value: string): string | null {
  const normalized = value.replace(/\r\n?/gu, "\n").normalize("NFC").trim();
  if (normalized.length === 0 || [...normalized].length > 4_096
    || /[\u0000-\u0008\u000B-\u001F\u007F-\u009F\u061C\u200E\u200F\u202A-\u202E\u2066-\u2069]/u.test(normalized)
    || /[\uD800-\uDFFF]/u.test(normalized)) return null;
  return normalized;
}

/** A decision-bound Other token is mandatory; arbitrary text never claims a decision. */
export function normalizeElicitationDecisionReply(value: unknown): string | null {
  const option = validateElicitationReplyValue(value);
  if (option !== null) return option;
  if (typeof value !== "string") return null;
  const match = /^\s*(axis-reply-[a-f0-9]{48})[ \t\r\n]+([\s\S]*)$/u.exec(value);
  if (match === null) return null;
  const answer = freeformAnswer(match[2]!);
  return answer === null ? null : `${match[1]} ${answer}`;
}

export function selectElicitationReply(elicitation: StructuredElicitation, reply: string): ElicitationReply | null {
  const option = selectElicitationOption(elicitation, reply);
  if (option !== null) return { kind: "option", optionId: option.optionId, label: option.label };
  if (elicitation.other === undefined || !reply.startsWith(`${elicitation.other.replyValue} `)) return null;
  const answer = freeformAnswer(reply.slice(elicitation.other.replyValue.length + 1));
  return answer === null ? null : { kind: "other", optionId: elicitation.other.optionId, label: "Other", answer };
}

/** Keep the opaque claim out of the native answer; ordinary fixed replies remain unchanged. */
export function elicitationReplyInstruction(elicitation: StructuredElicitation, reply: string): string | null {
  const selection = selectElicitationReply(elicitation, reply);
  return selection === null ? null : selection.kind === "option" ? reply : `Other: ${selection.answer}`;
}

/**
 * The instruction a free-text reply to a waiting question is dispatched as, so the model resolves
 * what the text meant. It restates the question and every raised option, but never the reply
 * tokens, which are the closed surface the relay matches on. Its rules: choose only among these
 * options, ask again when unsure, and do not revisit owner or authorization (the relay has already
 * established the reply came from the decision's owner). An Other-bearing question is never
 * interpreted; `interpretationClaim` refuses it.
 *
 * Returns `null` for a body outside the `freeformAnswer` constraints; the caller re-asks rather
 * than dispatching the raw body.
 */
export function elicitationInterpretationInstruction(elicitationValue: StructuredElicitation, body: string): string | null {
  const elicitation = validateStructuredElicitation(elicitationValue);
  if (elicitation === null) return null;
  const answer = freeformAnswer(body);
  if (answer === null) return null;
  const options = elicitation.options.map((option) => `- ${option.label} \u2014 ${option.description}`).join("\n");
  return [
    `The user was asked: ${elicitation.question}`,
    "",
    "The options offered were:",
    options,
    "",
    "The user replied, in their own words:",
    answer,
    "",
    "Read that reply against the options above. If it clearly names one of them, proceed with that option.",
    "Only those options are available; do not treat the reply as a new answer the question did not offer.",
    "If the reply is unclear, is not about this question, or could mean more than one of them, ask the question again instead of choosing -- quote back what they said and ask which one they meant.",
    "Who may answer this question has already been decided; do not re-decide the owner, the authorization, or which question this reply belongs to.",
  ].join("\n");
}

/** Normalize the closed reply surface without accepting free-form or unsafe content. */
export function normalizeElicitationReply(value: unknown): string | null {
  return safeText(value, 80);
}

export function validateElicitationReplyValue(value: unknown): string | null {
  const normalized = normalizeElicitationReply(value);
  return normalized !== null && /^axis-reply-[a-f0-9]{48}$/u.test(normalized) ? normalized : null;
}
