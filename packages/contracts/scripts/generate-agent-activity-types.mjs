#!/usr/bin/env node
import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const version = "1.0.0";
const schemaDirectory = path.join(root, "schemas", "agent-activity");
const outputPath = path.join(root, "src", "agent-activity", "types.generated.ts");
const names = ["agent-activity-elicitation-receipt", "executor-agent-waiting-delivery", "run-agent-waiting-delivery", "task-agent-waiting-delivery", "agent-activity-response-receipt", "executor-agent-response-delivery", "run-agent-response-delivery", "task-agent-response-delivery"];

const schemas = Object.fromEntries(await Promise.all(names.map(async (name) => {
  const schema = JSON.parse(await readFile(path.join(schemaDirectory, `${name}.schema.json`), "utf8"));
  const expectedId = `https://github.com/axisosme/Axis/schemas/agent-activity/${name}.schema.json`;
  if (schema.$schema !== "https://json-schema.org/draft/2020-12/schema" || schema.$id !== expectedId) throw new Error(`Invalid agent-activity schema metadata in ${name}`);
  return [name, schema];
})));
const receipt = schemas["agent-activity-elicitation-receipt"];
if (!receipt.$defs.elicitationReceipt) throw new Error("Agent-activity elicitation receipt definition is missing");
for (const [name, type] of [["executor-agent-waiting-delivery", "executor_agent_waiting"], ["run-agent-waiting-delivery", "run_agent_waiting"], ["task-agent-waiting-delivery", "task_agent_waiting"]]) {
  if (schemas[name].properties.deliveryType.const !== type || schemas[name].properties.contractVersion.const !== version) throw new Error(`Agent-waiting delivery discriminator changed in ${name}`);
}

const responseReceipt = schemas["agent-activity-response-receipt"];
if (!responseReceipt.$defs.responseReceipt) throw new Error("Agent-activity response receipt definition is missing");

// Assert the envelope reference literally so generation rejects a response composite for another Runtime version.
for (const [name, type, envelopeSchema] of [
  ["executor-agent-response-delivery", "executor_agent_response", "executor-succeeded-envelope"],
  ["run-agent-response-delivery", "run_agent_response", "run-succeeded-envelope"],
  ["task-agent-response-delivery", "task_agent_response", "task-succeeded-envelope"],
]) {
  const schema = schemas[name];
  if (schema.properties.deliveryType.const !== type || schema.properties.contractVersion.const !== version) throw new Error(`Agent-response delivery discriminator changed in ${name}`);
  const expectedEnvelopeRef = `https://github.com/axisosme/Axis/schemas/runtime/${envelopeSchema}.schema.json`;
  if (schema.properties.envelope.$ref !== expectedEnvelopeRef) throw new Error(`Agent-response delivery envelope must $ref ${expectedEnvelopeRef} in ${name}`);
}

const maxDisplayScalars = responseReceipt.$defs.displayText.maxLength;
const fallbackBody = responseReceipt.$defs.fallbackResponse.properties.content.properties.body.const;
if (typeof maxDisplayScalars !== "number" || typeof fallbackBody !== "string") throw new Error("Agent-activity display bounds or fallback body are missing");
if (/codex/iu.test(fallbackBody)) throw new Error("The fallback response body must stay provider-neutral");

const source = `// Generated from schemas/agent-activity/ agent-activity family. Do not edit.
import type { ExecutorWaitingForInputEnvelope, RunWaitingForInputEnvelope, TaskWaitingForInputEnvelope, StructuredElicitation, ExecutorSucceededEnvelope, RunSucceededEnvelope, TaskSucceededEnvelope } from "../runtime/types.generated.js";

export const agentActivityContractVersion = ${JSON.stringify(version)} as const;
export type AgentActivityContractVersion = typeof agentActivityContractVersion;

export interface LinearConversationIdentity { system: "linear"; agentSessionId: string; }
export type ConversationIdentity = LinearConversationIdentity;

export interface AgentActivityElicitationReceipt {
  contractVersion: AgentActivityContractVersion;
  activityId: string;
  taskId: string;
  attemptId: string;
  runId: string;
  conversation: ConversationIdentity;
  sourceEventId: string;
  operationId: string;
  elicitation: StructuredElicitation;
}

export interface ExecutorAgentWaitingDelivery { contractVersion: AgentActivityContractVersion; deliveryType: "executor_agent_waiting"; envelope: ExecutorWaitingForInputEnvelope; agentActivity: AgentActivityElicitationReceipt; }
export interface RunAgentWaitingDelivery { contractVersion: AgentActivityContractVersion; deliveryType: "run_agent_waiting"; envelope: RunWaitingForInputEnvelope; agentActivity: AgentActivityElicitationReceipt; }
export interface TaskAgentWaitingDelivery { contractVersion: AgentActivityContractVersion; deliveryType: "task_agent_waiting"; envelope: TaskWaitingForInputEnvelope; agentActivity: AgentActivityElicitationReceipt; }
export type AgentWaitingDelivery = ExecutorAgentWaitingDelivery | RunAgentWaitingDelivery | TaskAgentWaitingDelivery;

export const agentActivityMaxDisplayScalars = ${JSON.stringify(maxDisplayScalars)} as const;
// The fallback response body is provider-neutral; generation verifies its exact wording.
export const agentActivityFallbackResponseBody = ${JSON.stringify(fallbackBody)} as const;


// A response receipt has exactly one display field: /content/body.
// Action receipt fields are outside this surface.
export interface AgentActivityTruncation {
  fieldPath: "/content/body";
  truncated: true;
  originalScalarCount: number;
  omittedScalarCount: number;
}
export interface ResponseActivityContent { type: "response"; body: string; }

export interface AgentActivityResponseReceipt {
  contractVersion: AgentActivityContractVersion;
  activityId: string;
  taskId: string;
  attemptId: string;
  runId: string;
  conversation: ConversationIdentity;
  sourceEventId: string;
  operationId: string;
  truncated: boolean;
  truncations: AgentActivityTruncation[];
  content: ResponseActivityContent;
  projectionStatus: "projected" | "fallback";
  resultRef: string;
}

export interface ExecutorAgentResponseDelivery { contractVersion: AgentActivityContractVersion; deliveryType: "executor_agent_response"; envelope: ExecutorSucceededEnvelope; agentActivity: AgentActivityResponseReceipt; }
export interface RunAgentResponseDelivery { contractVersion: AgentActivityContractVersion; deliveryType: "run_agent_response"; envelope: RunSucceededEnvelope; agentActivity: AgentActivityResponseReceipt; }
export interface TaskAgentResponseDelivery { contractVersion: AgentActivityContractVersion; deliveryType: "task_agent_response"; envelope: TaskSucceededEnvelope; agentActivity: AgentActivityResponseReceipt; }
export type AgentResponseDelivery = ExecutorAgentResponseDelivery | RunAgentResponseDelivery | TaskAgentResponseDelivery;
`;

if (process.argv.includes("--print")) process.stdout.write(source);
else if (process.argv.includes("--write")) await writeFile(outputPath, source);
else if (await readFile(outputPath, "utf8") !== source) {
  process.stderr.write("Agent-activity v1 type output is stale; run node scripts/generate-agent-activity-types.mjs --write.\n");
  process.exitCode = 1;
}
