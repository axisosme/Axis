const MAX_BODY_DATA_BYTES = 128 * 1024;
const MAX_DOCUMENT_DEPTH = 16;
const MAX_DOCUMENT_NODES = 2_048;

export type CommentInstructionResult =
  | { readonly disposition: "accepted"; readonly instruction: string }
  | { readonly disposition: "ignored"; readonly reason: "not_mentioned" }
  | { readonly disposition: "rejected"; readonly reason: "malformed" };

interface WalkState {
  nodes: number;
  exactMentions: number;
  invalid: boolean;
}

function object(value: unknown): Readonly<Record<string, unknown>> | null {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? value as Readonly<Record<string, unknown>>
    : null;
}

function keys(value: Readonly<Record<string, unknown>>, required: readonly string[], optional: readonly string[] = []): boolean {
  const allowed = new Set([...required, ...optional]);
  return required.every((key) => Object.hasOwn(value, key)) && Object.keys(value).every((key) => allowed.has(key));
}

function validMarks(value: unknown): boolean {
  if (!Array.isArray(value)) return false;
  const allowed = new Set(["bold", "strong", "italic", "em", "strike", "underline", "code", "link"]);
  return value.every((candidate) => {
    const mark = object(candidate);
    if (mark === null || !keys(mark, ["type"], ["attrs"]) || typeof mark.type !== "string" || !allowed.has(mark.type)) return false;
    return mark.attrs === undefined || object(mark.attrs) !== null;
  });
}

// ProseMirror omits `content` for a childless container, so an absent key
// means no children; a present non-array `content` is malformed.
function content(value: Readonly<Record<string, unknown>>): readonly unknown[] | null {
  if (value.content === undefined) return [];
  return Array.isArray(value.content) ? value.content : null;
}

function renderChildren(
  children: readonly unknown[],
  appUserId: string,
  state: WalkState,
  depth: number,
  expected: "block" | "inline",
  separator: string,
): string {
  const pieces: string[] = [];
  for (const child of children) pieces.push(renderNode(child, appUserId, state, depth, expected));
  return pieces.join(separator);
}

function renderNode(
  value: unknown,
  appUserId: string,
  state: WalkState,
  depth: number,
  expected: "block" | "inline",
): string {
  if (state.invalid) return "";
  state.nodes += 1;
  if (depth > MAX_DOCUMENT_DEPTH || state.nodes > MAX_DOCUMENT_NODES) {
    state.invalid = true;
    return "";
  }
  const node = object(value);
  if (node === null || typeof node.type !== "string") {
    state.invalid = true;
    return "";
  }

  if (expected === "inline") {
    if (node.type === "text") {
      if (!keys(node, ["type", "text"], ["marks"]) || typeof node.text !== "string" || (node.marks !== undefined && !validMarks(node.marks))) {
        state.invalid = true;
        return "";
      }
      return node.text;
    }
    // Linear emits both `hardBreak` and `hard_break`. Any unrecognized node
    // type rejects the whole comment as `malformed`.
    if (node.type === "hardBreak" || node.type === "hard_break") {
      if (!keys(node, ["type"])) state.invalid = true;
      return "\n";
    }
    if (node.type === "suggestion_userMentions") {
      const attrs = object(node.attrs);
      if (!keys(node, ["type", "attrs"]) || attrs === null || typeof attrs.id !== "string") {
        state.invalid = true;
        return "";
      }
      if (attrs.id === appUserId) state.exactMentions += 1;
      return "";
    }
    // Linear rewrites a pasted document or agent-session link into an
    // `entityMention`. Its label is instruction text, never mention authority.
    if (node.type === "entityMention") {
      const attrs = object(node.attrs);
      if (!keys(node, ["type", "attrs"]) || attrs === null || !keys(attrs, ["label"], ["id", "href"]) || typeof attrs.label !== "string") {
        state.invalid = true;
        return "";
      }
      return attrs.label;
    }
    // An issue reference is its own `issueMention` node; rendered like
    // `entityMention`, as text without authority.
    if (node.type === "issueMention") {
      const attrs = object(node.attrs);
      if (!keys(node, ["type", "attrs"]) || attrs === null || !keys(attrs, ["label"], ["id", "href", "title"]) || typeof attrs.label !== "string") {
        state.invalid = true;
        return "";
      }
      return attrs.label;
    }
    state.invalid = true;
    return "";
  }

  const children = content(node);
  if (node.type === "paragraph") {
    if (!keys(node, ["type"], ["content"]) || children === null) state.invalid = true;
    return children === null ? "" : renderChildren(children, appUserId, state, depth + 1, "inline", "");
  }
  if (node.type === "heading") {
    const attrs = object(node.attrs);
    if (!keys(node, ["type", "attrs"], ["content"]) || attrs === null || !keys(attrs, ["level"])
      || !Number.isSafeInteger(attrs.level) || Number(attrs.level) < 1 || Number(attrs.level) > 6 || children === null) state.invalid = true;
    return children === null ? "" : renderChildren(children, appUserId, state, depth + 1, "inline", "");
  }
  // Multi-word block types are accepted in both camelCase and snake_case,
  // since Linear's payloads use snake_case for at least the list family.
  if (node.type === "codeBlock" || node.type === "code_block") {
    if (!keys(node, ["type"], ["content", "attrs"]) || (node.attrs !== undefined && object(node.attrs) === null) || children === null) state.invalid = true;
    return children === null ? "" : renderChildren(children, appUserId, state, depth + 1, "inline", "");
  }
  if (node.type === "blockquote" || node.type === "listItem" || node.type === "list_item") {
    if (!keys(node, ["type"], ["content"]) || children === null) state.invalid = true;
    return children === null ? "" : renderChildren(children, appUserId, state, depth + 1, "block", "\n");
  }
  if (node.type === "taskItem" || node.type === "task_item") {
    const attrs = object(node.attrs);
    if (!keys(node, ["type", "attrs"], ["content"]) || attrs === null || !keys(attrs, ["checked"])
      || typeof attrs.checked !== "boolean" || children === null) state.invalid = true;
    return children === null ? "" : renderChildren(children, appUserId, state, depth + 1, "block", "\n");
  }
  if (node.type === "bulletList" || node.type === "bullet_list" || node.type === "taskList" || node.type === "task_list") {
    if (!keys(node, ["type"], ["content"]) || children === null) state.invalid = true;
    return children === null ? "" : renderChildren(children, appUserId, state, depth + 1, "block", "\n");
  }
  if (node.type === "orderedList" || node.type === "ordered_list") {
    const attrs = object(node.attrs);
    if (!keys(node, ["type"], ["content", "attrs"]) || children === null
      || (node.attrs !== undefined && attrs === null)
      || (attrs !== null && (!keys(attrs, ["order"]) || !Number.isSafeInteger(attrs.order) || Number(attrs.order) < 1))) state.invalid = true;
    return children === null ? "" : renderChildren(children, appUserId, state, depth + 1, "block", "\n");
  }
  // A pasted image is a top-level block that contributes no text.
  if (node.type === "image") {
    const attrs = object(node.attrs);
    if (!keys(node, ["type", "attrs"]) || attrs === null) state.invalid = true;
    return "";
  }
  state.invalid = true;
  return "";
}

/**
 * Validates a bounded, explicit subset of Linear's ProseMirror comment shape.
 * Only `suggestion_userMentions.attrs.id` is mention authority; labels are
 * presentation and plain `@axis` text never authorizes work. A separately
 * authorized AgentSession creation can opt out of the mention requirement.
 */
export function validateCommentInstruction(bodyData: string, appUserId: string, requireMention = true): CommentInstructionResult {
  if (Buffer.byteLength(bodyData, "utf8") > MAX_BODY_DATA_BYTES) return requireMention ? { disposition: "ignored", reason: "not_mentioned" } : { disposition: "rejected", reason: "malformed" };
  let decoded: unknown;
  try {
    decoded = JSON.parse(bodyData);
  } catch {
    return requireMention ? { disposition: "ignored", reason: "not_mentioned" } : { disposition: "rejected", reason: "malformed" };
  }
  const document = object(decoded);
  if (document === null || document.type !== "doc" || !keys(document, ["type", "content"]) || !Array.isArray(document.content)) {
    return requireMention ? { disposition: "ignored", reason: "not_mentioned" } : { disposition: "rejected", reason: "malformed" };
  }
  const state: WalkState = { nodes: 1, exactMentions: 0, invalid: false };
  const instruction = renderChildren(document.content, appUserId, state, 2, "block", "\n").trim();
  if (requireMention && state.exactMentions === 0) return { disposition: "ignored", reason: "not_mentioned" };
  if (state.invalid) return { disposition: "rejected", reason: "malformed" };
  return { disposition: "accepted", instruction };
}
