import assert from "node:assert/strict";

import {
  deterministicElicitationOtherOption,
  deterministicElicitationReplyValue,
  validateStructuredElicitation,
  type StructuredElicitation,
} from "../../src/shared/elicitation.js";

/**
 * A valid `StructuredElicitation`.
 *
 * `seed` must be a hex character: the ids are `axis-<kind>-<48 hex>` and
 * `validateStructuredElicitation` refuses anything else, so a non-hex seed
 * silently produces an invalid fixture whose only symptom is a confusing
 * "elicitation projection is invalid" throw from whatever consumes it. The
 * assertions below make a bad seed fail here, where the cause is visible.
 */
export function elicitationFixture(seed: string, other = false): StructuredElicitation {
  assert.match(seed, /^[a-f0-9]$/u, "elicitation fixture seeds must be a single hex character");
  const decisionId = `axis-decision-${seed.repeat(48).slice(0, 48)}`;
  const fast = `axis-option-${"f".repeat(48)}`, thorough = `axis-option-${"0".repeat(48)}`;
  const question = {
    decisionId,
    sourceEventId: `axis-elicitation-${seed.repeat(48).slice(0, 48)}`,
    recommendedOptionId: fast,
    header: "Choose an approach",
    question: "Should Axis go fast or thorough?",
    options: [
      { optionId: fast, replyValue: deterministicElicitationReplyValue(decisionId, fast), label: "Fast", description: "Skim the obvious cases." },
      { optionId: thorough, replyValue: deterministicElicitationReplyValue(decisionId, thorough), label: "Thorough", description: "Check every case." },
    ],
  } as const;
  if (!other) return assertValid({ contractVersion: "1.0.0-fixed-choice", ...question });
  // The Other option is decision-derived, not caller-chosen:
  // `validateStructuredElicitation` recomputes it and refuses anything else.
  return assertValid({ contractVersion: "1.0.0-other", ...question, other: deterministicElicitationOtherOption(decisionId) });
}

/** Refuses to hand back a fixture the real validator would reject. */
function assertValid(candidate: unknown): StructuredElicitation {
  const validated = validateStructuredElicitation(candidate);
  assert.ok(validated, "elicitation fixture is not a valid StructuredElicitation");
  return validated;
}
