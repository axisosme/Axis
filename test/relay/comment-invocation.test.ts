import assert from "node:assert/strict";
import test from "node:test";

import { validateCommentInstruction } from "../../src/relay/comment-invocation.js";

const axisId = "20000000-0000-4000-8000-000000000001";

function document(content: readonly unknown[]): string {
  return JSON.stringify({ type: "doc", content: [{ type: "paragraph", content }] });
}

test("structured comment validation removes one exact Axis mention and preserves the instruction", () => {
  assert.deepEqual(validateCommentInstruction(document([
    { type: "suggestion_userMentions", attrs: { id: axisId, label: "axis" } },
    { type: "text", text: " status please" },
  ]), axisId), { disposition: "accepted", instruction: "status please" });
});

test("plain text and lookalike mention identities cannot authorize a comment; label presentation is ignored", () => {
  assert.deepEqual(validateCommentInstruction(document([{ type: "text", text: "@axis status" }]), axisId), {
    disposition: "ignored", reason: "not_mentioned",
  });
  assert.deepEqual(validateCommentInstruction(document([
    { type: "suggestion_userMentions", attrs: { id: "other-user", label: "axis" } },
    { type: "text", text: " status" },
  ]), axisId), { disposition: "ignored", reason: "not_mentioned" });
  assert.deepEqual(validateCommentInstruction(document([
    { type: "suggestion_userMentions", attrs: { id: axisId, label: "Axis" } },
    { type: "text", text: " status" },
  ]), axisId), { disposition: "accepted", instruction: "status" });
});

test("exact mentions with malformed instructions reject closed while blank, long, and duplicate-mention instructions are accepted", () => {
  const mention = { type: "suggestion_userMentions", attrs: { id: axisId, label: "axis" } };
  assert.deepEqual(validateCommentInstruction(document([mention]), axisId), { disposition: "accepted", instruction: "" });
  assert.deepEqual(validateCommentInstruction(document([mention, mention, { type: "text", text: "status" }]), axisId), {
    disposition: "accepted", instruction: "status",
  });
  assert.deepEqual(validateCommentInstruction(document([mention, { type: "emoji", attrs: { name: "private" } }]), axisId), {
    disposition: "rejected", reason: "malformed",
  });
  assert.deepEqual(validateCommentInstruction(document([mention, { type: "text", text: "x".repeat(16_385) }]), axisId), {
    disposition: "accepted", instruction: "x".repeat(16_385),
  });
});

test("bounded formatted blocks, lists, and marks retain mention-excluded textual eligibility", () => {
  const bodyData = JSON.stringify({ type: "doc", content: [
    { type: "heading", attrs: { level: 2 }, content: [
      { type: "suggestion_userMentions", attrs: { id: axisId, label: "renamed presentation" } },
      { type: "text", text: " status", marks: [{ type: "bold" }] },
    ] },
    { type: "bulletList", content: [{ type: "listItem", content: [{ type: "paragraph", content: [
      { type: "text", text: "include tests", marks: [{ type: "link", attrs: { href: "https://example.invalid" } }] },
    ] }] }] },
  ] });
  assert.deepEqual(validateCommentInstruction(bodyData, axisId), {
    disposition: "accepted", instruction: "status\ninclude tests",
  });
});

test("document depth and node-count bounds fail closed after an exact mention", () => {
  const mentionParagraph = { type: "paragraph", content: [
    { type: "suggestion_userMentions", attrs: { id: axisId, label: "axis" } },
    { type: "text", text: " status" },
  ] };
  let nested: unknown = { type: "paragraph", content: [{ type: "text", text: "deep" }] };
  for (let index = 0; index < 20; index += 1) nested = { type: "blockquote", content: [nested] };
  assert.deepEqual(validateCommentInstruction(JSON.stringify({ type: "doc", content: [mentionParagraph, nested] }), axisId), {
    disposition: "rejected", reason: "malformed",
  });

  const manyNodes = Array.from({ length: 2_100 }, () => ({ type: "paragraph", content: [] }));
  assert.deepEqual(validateCommentInstruction(JSON.stringify({ type: "doc", content: [mentionParagraph, ...manyNodes] }), axisId), {
    disposition: "rejected", reason: "malformed",
  });
});

test("an entityMention (pasted issue/document/agent-session link) renders its label as plain instruction text, not authority", () => {
  const mention = { type: "suggestion_userMentions", attrs: { id: axisId, label: "axis" } };
  const bodyData = document([
    mention,
    { type: "text", text: " " },
    { type: "entityMention", attrs: { id: "20000000-0000-4000-8000-000000000002", label: "Draft sample customer emails", href: "https://linear.app/example-workspace/agent-session/synthetic-agent-session" } },
    { type: "text", text: " look at that and tell me what you think" },
  ]);
  assert.deepEqual(validateCommentInstruction(bodyData, axisId), {
    disposition: "accepted",
    instruction: "Draft sample customer emails look at that and tell me what you think",
  });
});

test("an entityMention missing a string label, or with unexpected extra attrs, still rejects closed", () => {
  const mention = { type: "suggestion_userMentions", attrs: { id: axisId, label: "axis" } };
  assert.deepEqual(validateCommentInstruction(document([
    mention, { type: "entityMention", attrs: { id: "x", href: "https://linear.app/example-workspace/issue/DEMO-1" } },
  ]), axisId), { disposition: "rejected", reason: "malformed" });
  assert.deepEqual(validateCommentInstruction(document([
    mention, { type: "entityMention", attrs: { label: "ok", unexpected: true } },
  ]), axisId), { disposition: "rejected", reason: "malformed" });
});

test("an issueMention (pasted/autocompleted issue link) renders its label as plain instruction text, not authority", () => {
  // Linear bodyData represents pasted or autocompleted issue links with issueMention,
  // distinct from entityMention document and agent-session links. Render its label as
  // instruction text without treating its identifiers or URL as authority.
  const mention = { type: "suggestion_userMentions", attrs: { id: axisId, label: "axis" } };
  const bodyData = document([
    mention,
    { type: "text", text: " 把这两个子 ticket 的结论同步到 " },
    { type: "issueMention", attrs: { id: "20000000-0000-4000-8000-000000000003", label: "DEMO-297", href: "https://linear.app/example-workspace/issue/DEMO-297/sample-release", title: "Sample release" } },
    { type: "text", text: " 更新到 desc 里面。" },
  ]);
  assert.deepEqual(validateCommentInstruction(bodyData, axisId), {
    disposition: "accepted",
    instruction: "把这两个子 ticket 的结论同步到 DEMO-297 更新到 desc 里面。",
  });
});

test("an issueMention without the optional href/title attrs is still accepted; missing label or unexpected attrs still reject closed", () => {
  // An issueMention can occupy its own paragraph with the instruction in a second
  // paragraph. The issueMention needs no trailing text in its paragraph.
  const mention = { type: "suggestion_userMentions", attrs: { id: axisId, label: "axis" } };
  const bodyData = JSON.stringify({ type: "doc", content: [
    { type: "paragraph", content: [
      mention,
      { type: "text", text: " " },
      { type: "issueMention", attrs: { id: "20000000-0000-4000-8000-000000000004", label: "DEMO-626", href: "https://linear.app/example-workspace/issue/DEMO-626/sample-workflow" } },
    ] },
    { type: "paragraph", content: [{ type: "text", text: "记录一个新的 ticket 来记录这个问题。" }] },
  ] });
  assert.deepEqual(validateCommentInstruction(bodyData, axisId), {
    disposition: "accepted",
    instruction: "DEMO-626\n记录一个新的 ticket 来记录这个问题。",
  });
  assert.deepEqual(validateCommentInstruction(document([
    mention, { type: "issueMention", attrs: { id: "x", href: "https://linear.app/example-workspace/issue/DEMO-1" } },
  ]), axisId), { disposition: "rejected", reason: "malformed" });
  assert.deepEqual(validateCommentInstruction(document([
    mention, { type: "issueMention", attrs: { label: "ok", unexpected: true } },
  ]), axisId), { disposition: "rejected", reason: "malformed" });
});

test("top-level image nodes (pasted/dropped screenshots) preceding the mention paragraph are accepted, not malformed", () => {
  // Linear bodyData places image blocks beside paragraphs at the document root. Use
  // that shape to check that screenshots preceding an exact Axis mention do not
  // invalidate the instruction.
  const bodyData = JSON.stringify({
    type: "doc",
    content: [
      { type: "image", attrs: {
        uploadState: "finished", uploadId: "upload-1", src: "https://uploads.linear.app/a",
        alt: null, title: "CleanShot.jpg", attribution: null, originalSrc: null,
        width: 1568, height: 1022, displayWidth: null,
        contentAttribution: { userId: "20000000-0000-4000-8000-000000000005", actorType: "user" },
      } },
      { type: "image", attrs: {
        uploadState: "finished", uploadId: "upload-2", src: "https://uploads.linear.app/b",
        alt: null, title: "CleanShot2.jpg", attribution: null, originalSrc: null,
        width: 1568, height: 1022, displayWidth: null,
      } },
      { type: "paragraph", content: [
        { type: "suggestion_userMentions", attrs: { id: axisId, label: "axis" } },
        { type: "text", text: " look at these charts and tell me what you think" },
      ] },
      { type: "paragraph", content: [{ type: "text", text: "this is the real issue" }] },
    ],
  });
  assert.deepEqual(validateCommentInstruction(bodyData, axisId), {
    disposition: "accepted",
    instruction: "look at these charts and tell me what you think\nthis is the real issue",
  });
});

test("an image node without a plain-object attrs value still rejects closed", () => {
  // The mention must precede the malformed node: `state.invalid` short-
  // circuits every later node (see `renderNode`'s `if (state.invalid) return
  // "";` guard), so a malformed node before the mention would never let
  // `exactMentions` increment and the result would be `ignored`, not
  // `malformed` -- same ordering the existing entityMention test above uses.
  const bodyData = JSON.stringify({
    type: "doc",
    content: [
      { type: "paragraph", content: [
        { type: "suggestion_userMentions", attrs: { id: axisId, label: "axis" } },
        { type: "text", text: " hello" },
      ] },
      { type: "image", attrs: "not-an-object" },
    ],
  });
  assert.deepEqual(validateCommentInstruction(bodyData, axisId), { disposition: "rejected", reason: "malformed" });
});

test("Linear's real ordered_list/list_item snake_case shape is accepted identically to the camelCase spelling", () => {
  // Linear bodyData uses ordered_list and list_item snake_case nodes. This document
  // shape must render identically to the camelCase form.
  const bodyData = JSON.stringify({ type: "doc", content: [
    { type: "paragraph", content: [
      { type: "suggestion_userMentions", attrs: { id: axisId, label: "axis" } },
      { type: "text", text: " 你应该有这个 ticket 相关的上下文" },
    ] },
    { type: "paragraph", content: [{ type: "text", text: "分成 4 个象限：" }] },
    { type: "ordered_list", attrs: { order: 1 }, content: [
      { type: "list_item", content: [{ type: "paragraph", content: [{ type: "text", text: "High New ARR x High NDR：主力渠道" }] }] },
      { type: "list_item", content: [{ type: "paragraph", content: [{ type: "text", text: "Low New ARR x Low NDR：垃圾渠道" }] }] },
    ] },
  ] });
  assert.deepEqual(validateCommentInstruction(bodyData, axisId), {
    disposition: "accepted",
    instruction: "你应该有这个 ticket 相关的上下文\n分成 4 个象限：\nHigh New ARR x High NDR：主力渠道\nLow New ARR x Low NDR：垃圾渠道",
  });
});

test("bullet_list/task_list/task_item/code_block snake_case spellings are also accepted", () => {
  // Block-level node types (bullet_list, task_list, code_block) are doc-level
  // siblings of a paragraph, not paragraph children -- `document()` wraps its
  // argument as one paragraph's inline content, so these are built by hand.
  const mentionParagraph = { type: "paragraph", content: [
    { type: "suggestion_userMentions", attrs: { id: axisId, label: "axis" } },
  ] };
  const withMentionAnd = (block: unknown, instruction: string) => {
    assert.deepEqual(
      validateCommentInstruction(JSON.stringify({ type: "doc", content: [mentionParagraph, block] }), axisId),
      { disposition: "accepted", instruction },
    );
  };
  withMentionAnd(
    { type: "bullet_list", content: [{ type: "list_item", content: [{ type: "paragraph", content: [{ type: "text", text: "bulleted" }] }] }] },
    "bulleted",
  );
  withMentionAnd(
    { type: "task_list", content: [{ type: "task_item", attrs: { checked: false }, content: [{ type: "paragraph", content: [{ type: "text", text: "todo" }] }] }] },
    "todo",
  );
  withMentionAnd(
    { type: "code_block", content: [{ type: "text", text: "npm test" }] },
    "npm test",
  );
});

test("a childless container with the content key omitted entirely (not content: []) is accepted, not malformed", () => {
  // A childless paragraph after a code_block can omit content entirely. Accept that
  // empty container while rejecting a content member whose value is not an array.
  const mention = { type: "suggestion_userMentions", attrs: { id: axisId, label: "axis" } };
  const bodyData = JSON.stringify({ type: "doc", content: [
    { type: "paragraph", content: [mention, { type: "text", text: " " }] },
    { type: "code_block", content: [{ type: "text", text: "some code" }], attrs: { language: null, id: "x" } },
    { type: "paragraph" }, // No content key: a valid empty paragraph.
  ] });
  assert.deepEqual(validateCommentInstruction(bodyData, axisId), {
    disposition: "accepted", instruction: "some code",
  });
});

test("content present but not an array is still malformed, and every container type tolerates an omitted content key", () => {
  const mention = { type: "suggestion_userMentions", attrs: { id: axisId, label: "axis" } };
  assert.deepEqual(validateCommentInstruction(document([mention, { type: "paragraph", content: "not-an-array" }]), axisId), {
    disposition: "rejected", reason: "malformed",
  });
  // Each tested container itself is childless (renders no text) and sits
  // alongside a separate mention+text paragraph, so the overall instruction
  // is just that paragraph's text -- the point under test is that the
  // childless container is NOT "malformed": an omitted `content` key must
  // not, by itself, be treated as an invalid node shape.
  const mentionParagraph = { type: "paragraph", content: [mention, { type: "text", text: "x" }] };
  for (const type of ["blockquote", "listItem", "list_item", "bulletList", "bullet_list", "taskList", "task_list", "codeBlock", "code_block"]) {
    assert.deepEqual(
      validateCommentInstruction(JSON.stringify({ type: "doc", content: [mentionParagraph, { type }] }), axisId),
      { disposition: "accepted", instruction: "x" },
      `${type} with no content key should be a childless-but-valid container`,
    );
  }
});

test("Linear's snake_case hard_break is accepted the same as camelCase hardBreak", () => {
  // An Enter-key line break within a Linear bodyData paragraph is a hard_break node.
  // Render it like hardBreak while preserving the surrounding instruction.
  const mention = { type: "suggestion_userMentions", attrs: { id: axisId, label: "axis" } };
  const liveBodyData = JSON.stringify({
    type: "doc",
    content: [
      {
        type: "paragraph",
        content: [
          mention,
          { type: "text", text: " 关于 Tracking 的 T2P：" },
          { type: "hard_break" },
          { type: "text", text: "接下来，你来写一个 HTML" },
        ],
      },
    ],
  });
  assert.deepEqual(validateCommentInstruction(liveBodyData, axisId), {
    disposition: "accepted",
    instruction: "关于 Tracking 的 T2P：\n接下来，你来写一个 HTML",
  });

  // Negative control: camelCase hardBreak is accepted.
  assert.deepEqual(
    validateCommentInstruction(document([mention, { type: "text", text: "a" }, { type: "hardBreak" }, { type: "text", text: "b" }]), axisId),
    { disposition: "accepted", instruction: "a\nb" },
  );

  // A hard_break/hardBreak node with any extra/unexpected key is malformed:
  // both spellings are accepted, but only in the exact node shape.
  assert.deepEqual(
    validateCommentInstruction(document([mention, { type: "text", text: "a" }, { type: "hard_break", attrs: { unexpected: true } }]), axisId),
    { disposition: "rejected", reason: "malformed" },
  );
});
