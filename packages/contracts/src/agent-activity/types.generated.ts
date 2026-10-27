// Generated from schemas/agent-activity/ agent-activity family. Do not edit.
import type { ExecutorWaitingForInputEnvelope, RunWaitingForInputEnvelope, TaskWaitingForInputEnvelope, StructuredElicitation, ExecutorSucceededEnvelope, RunSucceededEnvelope, TaskSucceededEnvelope } from "../runtime/types.generated.js";

export const agentActivityContractVersion = "1.0.0" as const;
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

export const agentActivityMaxDisplayScalars = 4096 as const;
// The fallback response body is provider-neutral; generation verifies its exact wording.
export const agentActivityFallbackResponseBody = "The agent completed the turn, but no safe response could be projected." as const;


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
