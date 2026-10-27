import type { LinearActivityContent, OutboxRecord } from "../types.js";
import { RelayError } from "../errors.js";
import { sha256 } from "../security.js";

export type ProviderCreateResult =
  | { readonly disposition: "confirmed"; readonly activityId: string }
  | { readonly disposition: "rejected"; readonly safeCode: string; readonly retryable?: boolean }
  | { readonly disposition: "ambiguous"; readonly safeCode: string };

export type ProviderReconcileResult =
  | { readonly disposition: "confirmed"; readonly activityId: string }
  | { readonly disposition: "conflict"; readonly safeCode: string }
  | { readonly disposition: "unknown" };

export interface ImmutableLinearActivity {
  readonly id: string;
  readonly updatedAt: string;
  readonly userId: string | null;
  readonly user?: VerifiedLinearUser | null;
  readonly content: LinearActivityContent | { readonly type: "prompt"; readonly body: string };
  readonly sourceCommentId?: string | null;
}

export interface VerifiedLinearUser { readonly id: string; readonly active: boolean; readonly app: boolean; readonly url: string; }

export interface ImmutableLinearComment {
  readonly id: string;
  readonly parentId: string | null;
  /** Optional for source-compatible adapters; created comment starts require a canonical value. */
  readonly url?: string;
  readonly body: string;
  readonly bodyData: string;
  readonly issueId: string;
  readonly teamId: string;
  /** True when Linear generated the AgentSession root rather than a human comment. */
  readonly isArtificialAgentSessionRoot?: boolean;
  readonly user: VerifiedLinearUser | null;
  readonly botAuthored: boolean;
  readonly member: { readonly id: string; readonly active: boolean; readonly app: boolean } | null;
  readonly membershipComplete: boolean;
}

export interface ImmutableLinearIssue {
  readonly id: string;
  readonly teamId: string;
  readonly identifier: string;
  readonly title: string;
  readonly description: string;
}

export interface LinearTeam {
  readonly id: string;
  readonly name: string;
  readonly key: string;
}

export interface IssueExecutionContext {
  readonly issueId: string;
  readonly teamId: string;
  /**
   * Label names from the same Issue request as the execution context. A grouped label is qualified as
   * `<group>:<name>` (the `WORKER` group's `CODEX` child is `WORKER:CODEX`). Read only by Worker selection.
   */
  readonly labels: readonly string[];
  readonly stateType: string;
  readonly delegateId: string | null;
  readonly firstStartedStateId: string | null;
}

/**
 * The native signal an Activity carries, or `null` for the ordinary case. A required argument rather
 * than an optional `OutboxRecord` field, so every caller must state which case it is. The
 * `signalMetadata` shape is ours; `docs/LINEAR-PROVIDER.md` records it.
 */
export type LinearNativeSignal = {
  readonly signal: "select";
  readonly options: readonly { readonly label: string; readonly value: string }[];
};

export interface LinearProviderPort {
  createAgentActivity(accessToken: string, record: OutboxRecord, native: LinearNativeSignal | null): Promise<ProviderCreateResult>;
  reconcileAgentActivity(accessToken: string, record: OutboxRecord): Promise<ProviderReconcileResult>;
  loadSessionActivities(accessToken: string, agentSessionId: string): Promise<readonly ImmutableLinearActivity[]>;
  loadExactAgentActivity?(accessToken: string, activityId: string): Promise<{ readonly agentSessionId: string; readonly activity: ImmutableLinearActivity }>;
  loadExactUser?(accessToken: string, userId: string): Promise<VerifiedLinearUser>;
  loadExactComment?(accessToken: string, commentId: string, userId: string): Promise<ImmutableLinearComment>;
  /** Optional capability; Relay fails a created start closed when it is absent. */
  loadExactIssue?(accessToken: string, issueId: string): Promise<ImmutableLinearIssue>;
  loadIssueExecutionContext(accessToken: string, issueId: string): Promise<IssueExecutionContext>;
  markIssueStartedAndDelegated(accessToken: string, context: IssueExecutionContext, appUserId: string): Promise<void>;
  resolveViewer(accessToken: string): Promise<{ readonly appUserId: string; readonly organizationId: string }>;
  /** Optional discovery capability for installation setup. */
  listTeams?(accessToken: string): Promise<readonly LinearTeam[]>;
}

interface GraphQlError {
  readonly message?: string;
  readonly extensions?: { readonly code?: string; readonly validationErrors?: unknown };
}

interface GraphQlResponse<T> {
  readonly data?: T;
  readonly errors?: readonly GraphQlError[];
}

function object(value: unknown): Readonly<Record<string, unknown>> | null {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? value as Readonly<Record<string, unknown>>
    : null;
}

function safeGraphQlCode(errors: readonly GraphQlError[] | undefined): string {
  const code = errors?.find((error) => typeof error.extensions?.code === "string")?.extensions?.code;
  return typeof code === "string" && /^[A-Z0-9_]{1,64}$/.test(code) ? `linear.graphql.${code.toLowerCase()}` : "linear.graphql.rejected";
}

// Linear's class-validator errors contain request values/targets and arbitrary
// messages. Only closed field/constraint pairs may become durable diagnostics.
function safeActivityCreateCode(errors: readonly GraphQlError[] | undefined): string {
  const fallback = safeGraphQlCode(errors);
  if (fallback !== "linear.graphql.invalid_input") return fallback;
  const fields = ["id", "agentSessionId", "content", "ephemeral", "signal"] as const;
  const constraints = { isUuid: "is_uuid", isString: "is_string", isNotEmpty: "is_not_empty", isObject: "is_object", isBoolean: "is_boolean", isEnum: "is_enum" } as const;
  for (const error of errors?.slice(0, 10) ?? []) {
    if (error.extensions?.code !== "INVALID_INPUT") continue;
    const validation = error.extensions.validationErrors;
    if (!Array.isArray(validation)) continue;
    for (const entry of validation.slice(0, 20)) {
      const value = object(entry);
      const field = fields.find((candidate) => candidate === value?.property);
      const rules = object(value?.constraints);
      if (field === undefined || rules === null) continue;
      for (const key of Object.keys(constraints) as (keyof typeof constraints)[]) {
        if (Object.hasOwn(rules, key)) return `${fallback}.${field}.${constraints[key]}`;
      }
    }
  }
  return fallback;
}

export class LinearGraphQlProvider implements LinearProviderPort {
  readonly #endpoint: string;
  readonly #fetch: typeof fetch;
  readonly #timeoutMs: number;

  constructor(options: { readonly endpoint?: string; readonly fetch?: typeof fetch; readonly timeoutMs?: number } = {}) {
    this.#endpoint = options.endpoint ?? "https://api.linear.app/graphql";
    this.#fetch = options.fetch ?? fetch;
    this.#timeoutMs = options.timeoutMs ?? 4_500;
  }

  async createAgentActivity(accessToken: string, record: OutboxRecord, native: LinearNativeSignal | null): Promise<ProviderCreateResult> {
    const operation = `mutation AxisAgentActivityCreate($input: AgentActivityCreateInput!) {
      agentActivityCreate(input: $input) { success agentActivity { id } }
    }`;
    try {
      const result = await this.#request<{
        readonly agentActivityCreate?: { readonly success?: boolean; readonly agentActivity?: { readonly id?: string } | null };
      }>(accessToken, operation, {
        // `signal`/`signalMetadata` are siblings of `content`: reconciliation digests `content` as Linear
        // echoes it, and Linear does not echo `signalMetadata`.
        input: {
          id: record.id, agentSessionId: record.agentSessionId, content: record.content,
          ...(native === null ? {} : { signal: native.signal, signalMetadata: { options: native.options } }),
        },
      });
      if (result.errors !== undefined || result.data?.agentActivityCreate?.success !== true) {
        const safeCode = safeActivityCreateCode(result.errors);
        return {
          disposition: "rejected",
          safeCode,
          ...(safeCode === "linear.graphql.ratelimited" ? { retryable: true } : {}),
        };
      }
      const id = result.data.agentActivityCreate.agentActivity?.id;
      return typeof id === "string" ? { disposition: "confirmed", activityId: id }
        : { disposition: "ambiguous", safeCode: "linear.provider.missing_activity_id" };
    } catch {
      return { disposition: "ambiguous", safeCode: "linear.provider.transport_ambiguous" };
    }
  }

  async reconcileAgentActivity(accessToken: string, record: OutboxRecord): Promise<ProviderReconcileResult> {
    let identity: { readonly agentSessionId: string; readonly activity: ImmutableLinearActivity };
    try {
      identity = await this.#loadActivity(accessToken, record.id);
    } catch {
      return { disposition: "unknown" };
    }
    return identity.activity.id === record.id
      && identity.agentSessionId === record.agentSessionId
      && sha256(JSON.stringify(identity.activity.content)) === record.contentDigest
      ? { disposition: "confirmed", activityId: identity.activity.id }
      : { disposition: "conflict", safeCode: "linear.provider.activity_identity_conflict" };
  }

  async loadSessionActivities(accessToken: string, agentSessionId: string): Promise<readonly ImmutableLinearActivity[]> {
    const operation = `query AxisAgentSessionActivities($agentSessionId: String!) {
      agentSession(id: $agentSessionId) {
        activities(first: 250) {
          edges {
            node {
              id updatedAt user { id active app url }
              content {
                __typename
                ... on AgentActivityThoughtContent { body }
                ... on AgentActivityActionContent { action parameter result }
                ... on AgentActivityElicitationContent { body }
                ... on AgentActivityResponseContent { body }
                ... on AgentActivityErrorContent { body }
                ... on AgentActivityPromptContent { body }
              }
            }
          }
          pageInfo { hasNextPage }
        }
      }
    }`;
    const result = await this.#request<{
      readonly agentSession?: {
        readonly activities?: {
          readonly edges?: readonly { readonly node?: unknown }[];
          readonly pageInfo?: { readonly hasNextPage?: boolean };
        };
      } | null;
    }>(accessToken, operation, { agentSessionId });
    if (result.errors !== undefined) throw new Error(safeGraphQlCode(result.errors));
    if (result.data?.agentSession === null || result.data?.agentSession === undefined) {
      throw new Error("linear.provider.session_unavailable");
    }
    const edges = result.data.agentSession.activities?.edges;
    if (!Array.isArray(edges) || result.data.agentSession.activities?.pageInfo?.hasNextPage !== false) {
      throw new Error("linear.provider.session_activities_incomplete");
    }
    return edges.map((edge) => this.#activity(edge.node));
  }

  async loadExactAgentActivity(accessToken: string, activityId: string): Promise<{ readonly agentSessionId: string; readonly activity: ImmutableLinearActivity }> {
    return this.#loadActivity(accessToken, activityId);
  }

  async loadExactUser(accessToken: string, userId: string): Promise<VerifiedLinearUser> {
    const result = await this.#request<{ readonly user?: unknown | null }>(accessToken, "query AxisDecisionOwner($userId: String!) { user(id: $userId) { id active app url } }", { userId });
    if (result.errors !== undefined) throw new Error(safeGraphQlCode(result.errors));
    return this.#workspaceUser(object(result.data?.user), "decision_owner");
  }

  async loadExactComment(accessToken: string, commentId: string, userId: string): Promise<ImmutableLinearComment> {
    const operation = `query AxisCommentInvocation($commentId: String!, $userId: ID!) {
      comment(id: $commentId) {
        id parentId url body bodyData isArtificialAgentSessionRoot
        issue {
          id
          team {
            id
            members(first: 2, filter: { id: { eq: $userId } }) {
              nodes { id active app url }
              pageInfo { hasNextPage }
            }
          }
        }
        user { id active app url }
        botActor { id }
      }
    }`;
    const result = await this.#request<{ readonly comment?: unknown | null }>(accessToken, operation, { commentId, userId }, true);
    if (result.errors !== undefined) throw this.#graphQlError(result.errors);
    const comment = object(result.data?.comment);
    const issue = object(comment?.issue);
    const team = object(issue?.team);
    const user = object(comment?.user);
    const members = object(team?.members);
    const nodes = Array.isArray(members?.nodes) ? members.nodes : null;
    const pageInfo = object(members?.pageInfo);
    if (
      typeof comment?.id !== "string"
      || (comment.parentId !== null && typeof comment.parentId !== "string")
      || typeof comment.url !== "string"
      || typeof comment.body !== "string"
      || typeof comment.bodyData !== "string"
      || typeof comment.isArtificialAgentSessionRoot !== "boolean"
      || typeof issue?.id !== "string"
      || typeof team?.id !== "string"
      || !Object.hasOwn(comment, "user")
      || !Object.hasOwn(comment, "botActor")
      || nodes === null
      || typeof pageInfo?.hasNextPage !== "boolean"
    ) throw new Error("linear.provider.invalid_comment_shape");
    const author = user === null ? null : this.#workspaceUser(user, "comment");
    const member = nodes.length === 0 ? null : nodes.length === 1 ? this.#workspaceUser(object(nodes[0]), "member") : null;
    if (nodes.length > 1 || (member !== null && member.id !== userId)) {
      throw new Error("linear.provider.invalid_membership_shape");
    }
    return {
      id: comment.id,
      parentId: comment.parentId as string | null,
      url: comment.url,
      body: comment.body,
      bodyData: comment.bodyData,
      issueId: issue.id,
      teamId: team.id,
      isArtificialAgentSessionRoot: comment.isArtificialAgentSessionRoot,
      user: author,
      botAuthored: object(comment.botActor) !== null,
      member,
      membershipComplete: pageInfo.hasNextPage === false,
    };
  }

  async loadExactIssue(accessToken: string, issueId: string): Promise<ImmutableLinearIssue> {
    const operation = `query AxisExecutionIssue($issueId: String!) {
      issue(id: $issueId) { id identifier title description team { id } }
    }`;
    const result = await this.#request<{ readonly issue?: unknown | null }>(accessToken, operation, { issueId }, true);
    if (result.errors !== undefined) throw this.#graphQlError(result.errors);
    const issue = object(result.data?.issue);
    const team = object(issue?.team);
    // `Issue.description` is nullable, and no dispatch reads it, so `null` is the empty description.
    if (
      typeof issue?.id !== "string"
      || typeof issue.identifier !== "string"
      || typeof issue.title !== "string"
      || (issue.description !== null && typeof issue.description !== "string")
      || typeof team?.id !== "string"
    ) throw new Error("linear.provider.invalid_issue_shape");
    return {
      id: issue.id,
      teamId: team.id,
      identifier: issue.identifier,
      title: issue.title,
      description: issue.description ?? "",
    };
  }

  async loadIssueExecutionContext(accessToken: string, issueId: string): Promise<IssueExecutionContext> {
    const operation = `query AxisIssueExecutionContext($issueId: String!) {
      issue(id: $issueId) {
        id labels(first: 250) { nodes { name parent { name } } pageInfo { hasNextPage } } state { type } delegate { id } team { id states(filter: { type: { eq: "started" } }) { nodes { id position } } }
      }
    }`;
    const result = await this.#request<{
      readonly issue?: {
        readonly id: string;
        readonly labels?: { readonly nodes?: readonly { readonly name?: string; readonly parent?: { readonly name?: string } | null }[]; readonly pageInfo?: { readonly hasNextPage?: boolean } } | null;
        readonly state?: { readonly type?: string } | null;
        readonly delegate?: { readonly id?: string } | null;
        readonly team?: { readonly id?: string; readonly states?: { readonly nodes?: readonly { readonly id: string; readonly position: number }[] } } | null;
      } | null;
    }>(accessToken, operation, { issueId }, true);
    if (result.errors !== undefined) throw this.#graphQlError(result.errors);
    if (result.data?.issue === null || result.data?.issue === undefined) {
      throw new Error("linear.provider.issue_not_found");
    }
    const issue = result.data.issue;
    const labels = issue.labels?.nodes;
    const started = [...(issue.team?.states?.nodes ?? [])].sort((left, right) => left.position - right.position)[0];
    if (
      typeof issue.id !== "string"
      || typeof issue.team?.id !== "string"
      || !Array.isArray(labels)
      || issue.labels?.pageInfo?.hasNextPage !== false
      || labels.some((label) => typeof label?.name !== "string" || label.name.trim().length === 0
        // `parent` is requested, so Linear answers `null` (ungrouped) or a group; a missing key or a
        // group without a usable name is a shape Axis cannot verify.
        || (label.parent !== null && (typeof label.parent !== "object" || typeof label.parent.name !== "string" || label.parent.name.trim().length === 0)))
    ) {
      throw new Error("linear.provider.malformed_issue_context");
    }
    const labelNames: string[] = [];
    // A grouped label is qualified by its group, so a `CODEX` child of `WORKER` is not an ungrouped `CODEX`.
    for (const label of labels) labelNames.push(label.parent === null || label.parent === undefined ? label.name : `${label.parent.name}:${label.name}`);
    return {
      issueId: issue.id,
      teamId: issue.team.id,
      labels: Object.freeze(labelNames),
      stateType: issue.state?.type ?? "unknown",
      delegateId: issue.delegate?.id ?? null,
      firstStartedStateId: started?.id ?? null,
    };
  }

  /**
   * Transport loss, a 5xx and a rate limit throw retryably even though the outcome is unknown. That is
   * safe because a retry rereads the execution context and sends only fields still unset, so replaying an
   * applied update sends nothing.
   */
  async markIssueStartedAndDelegated(accessToken: string, context: IssueExecutionContext, appUserId: string): Promise<void> {
    const terminalOrStarted = ["started", "completed", "canceled"].includes(context.stateType);
    const input: { stateId?: string; delegateId?: string } = {};
    if (!terminalOrStarted && context.firstStartedStateId !== null) input.stateId = context.firstStartedStateId;
    if (context.delegateId === null) input.delegateId = appUserId;
    if (Object.keys(input).length === 0) return;
    const operation = `mutation AxisIssueUpdate($id: String!, $input: IssueUpdateInput!) {
      issueUpdate(id: $id, input: $input) { success }
    }`;
    const result = await this.#request<{ readonly issueUpdate?: { readonly success?: boolean } }>(accessToken, operation, {
      id: context.issueId,
      input,
    }, true);
    if (result.errors !== undefined) throw this.#graphQlError(result.errors);
    if (result.data?.issueUpdate?.success !== true) throw new Error(safeGraphQlCode(undefined));
  }

  async resolveViewer(accessToken: string): Promise<{ readonly appUserId: string; readonly organizationId: string }> {
    const result = await this.#request<{
      readonly viewer?: { readonly id?: string; readonly organization?: { readonly id?: string } };
    }>(accessToken, "query AxisViewer { viewer { id organization { id } } }", {});
    const appUserId = result.data?.viewer?.id;
    const organizationId = result.data?.viewer?.organization?.id;
    if (result.errors !== undefined || typeof appUserId !== "string" || typeof organizationId !== "string") {
      throw new Error(safeGraphQlCode(result.errors));
    }
    return { appUserId, organizationId };
  }

  async listTeams(accessToken: string): Promise<readonly LinearTeam[]> {
    const operation = `query AxisTeams($after: String) {
      teams(first: 250, after: $after) {
        nodes { id name key }
        pageInfo { hasNextPage endCursor }
      }
    }`;
    const teams: LinearTeam[] = [];
    const teamIds = new Set<string>();
    const cursors = new Set<string>();
    let after: string | null = null;
    while (true) {
      const result = object(await this.#request<unknown>(accessToken, operation, { after }));
      if (result?.errors !== undefined) {
        if (!Array.isArray(result.errors) || result.errors.some((error) => object(error) === null)) {
          throw new Error("linear.provider.invalid_teams_shape");
        }
        throw new Error(safeGraphQlCode(result.errors));
      }
      const connection = object(object(result?.data)?.teams);
      const nodes = connection?.nodes;
      const pageInfo = object(connection?.pageInfo);
      if (
        !Array.isArray(nodes)
        || typeof pageInfo?.hasNextPage !== "boolean"
        || (pageInfo.endCursor !== null && typeof pageInfo.endCursor !== "string")
      ) throw new Error("linear.provider.invalid_teams_shape");
      for (const node of nodes) {
        const team = object(node);
        if (
          typeof team?.id !== "string" || team.id.trim().length === 0
          || typeof team.name !== "string" || team.name.trim().length === 0
          || typeof team.key !== "string" || team.key.trim().length === 0
          || teamIds.has(team.id)
        ) throw new Error("linear.provider.invalid_team_shape");
        teamIds.add(team.id);
        teams.push({ id: team.id, name: team.name, key: team.key });
      }
      if (!pageInfo.hasNextPage) return teams;
      const cursor = pageInfo.endCursor;
      if (typeof cursor !== "string" || cursor.trim().length === 0 || cursors.has(cursor)) {
        throw new Error("linear.provider.teams_incomplete");
      }
      cursors.add(cursor);
      after = cursor;
    }
  }

  /**
   * `retryable` marks callers whose transient failures must stay retryable: the exact reads and the two
   * issue-sync calls. For them transport loss (including a timeout mid-body) and a 5xx throw a retryable
   * `RelayError`; `#graphQlError` handles the rate limit. A non-JSON body is a definite answer and throws.
   */
  async #request<T>(accessToken: string, query: string, variables: Readonly<Record<string, unknown>>, retryable = false): Promise<GraphQlResponse<T>> {
    let response: Response;
    try {
      response = await this.#fetch(this.#endpoint, {
        method: "POST",
        headers: { authorization: `Bearer ${accessToken}`, "content-type": "application/json" },
        body: JSON.stringify({ query, variables }),
        signal: AbortSignal.timeout(this.#timeoutMs),
      });
    } catch (error) {
      if (retryable) throw this.#retryableTransportError();
      throw error;
    }
    if (response.status >= 500) {
      if (retryable) throw this.#retryableTransportError();
      throw new Error("linear.provider.server_outcome_ambiguous");
    }
    let value: unknown;
    try {
      value = await response.json();
    } catch (error) {
      if (retryable && !(error instanceof SyntaxError)) throw this.#retryableTransportError();
      throw error;
    }
    if (!response.ok && object(value) === null) throw new Error("linear.provider.http_failure");
    return value as GraphQlResponse<T>;
  }

  #graphQlError(errors: readonly GraphQlError[]): Error {
    return errors.some((error) => error.extensions?.code === "RATELIMITED")
      ? this.#retryableTransportError()
      : new Error(safeGraphQlCode(errors));
  }

  #retryableTransportError(): RelayError {
    return new RelayError("linear.provider.rejected", "Linear transport is temporarily unavailable.", 503, true);
  }

  async #loadActivity(
    accessToken: string,
    activityId: string,
  ): Promise<{ readonly agentSessionId: string; readonly activity: ImmutableLinearActivity }> {
    const operation = `query AxisAgentActivity($activityId: String!) {
      agentActivity(id: $activityId) {
        id updatedAt user { id active app url } agentSession { id } sourceComment { id }
        content {
          __typename
          ... on AgentActivityThoughtContent { body }
          ... on AgentActivityActionContent { action parameter result }
          ... on AgentActivityElicitationContent { body }
          ... on AgentActivityResponseContent { body }
          ... on AgentActivityErrorContent { body }
          ... on AgentActivityPromptContent { body }
        }
      }
    }`;
    const result = await this.#request<{ readonly agentActivity?: unknown | null }>(
      accessToken,
      operation,
      { activityId },
    );
    if (result.errors !== undefined) throw new Error(safeGraphQlCode(result.errors));
    const value = object(result.data?.agentActivity);
    const session = object(value?.agentSession);
    if (typeof session?.id !== "string") throw new Error("linear.provider.invalid_activity_shape");
    const sourceComment = object(value?.sourceComment);
    if (sourceComment !== null && typeof sourceComment.id !== "string") throw new Error("linear.provider.invalid_activity_shape");
    return {
      agentSessionId: session.id,
      activity: { ...this.#activity(value), sourceCommentId: sourceComment === null ? null : sourceComment.id as string },
    };
  }

  #workspaceUser(value: Readonly<Record<string, unknown>> | null, context: string): VerifiedLinearUser {
    if (value === null || typeof value.id !== "string" || typeof value.active !== "boolean" || typeof value.app !== "boolean" || typeof value.url !== "string") {
      throw new Error(`linear.provider.invalid_${context}_user_shape`);
    }
    return { id: value.id, active: value.active, app: value.app, url: value.url };
  }

  #activity(value: unknown): ImmutableLinearActivity {
    const activity = object(value);
    const content = object(activity?.content);
    const id = activity?.id;
    const updatedAt = activity?.updatedAt;
    const typename = content?.__typename;
    if (typeof id !== "string" || typeof updatedAt !== "string" || content === null || typeof typename !== "string") {
      throw new Error("linear.provider.invalid_activity_shape");
    }
    const user = object(activity?.user);
    const userId = typeof user?.id === "string" ? user.id : null;
    const verifiedUser = user === null ? null : this.#workspaceUser(user, "activity");
    if (typename === "AgentActivityActionContent") {
      if (typeof content?.action !== "string" || typeof content.parameter !== "string") throw new Error("linear.provider.invalid_activity_shape");
      return {
        id, updatedAt, userId, user: verifiedUser,
        content: {
          type: "action",
          action: content.action,
          parameter: content.parameter,
          ...(typeof content.result === "string" ? { result: content.result } : {}),
        },
      };
    }
    const mapping: Readonly<Record<string, LinearActivityContent["type"] | "prompt">> = {
      AgentActivityThoughtContent: "thought",
      AgentActivityElicitationContent: "elicitation",
      AgentActivityResponseContent: "response",
      AgentActivityErrorContent: "error",
      AgentActivityPromptContent: "prompt",
    };
    const type = mapping[typename];
    if (type === undefined || typeof content.body !== "string") throw new Error("linear.provider.invalid_activity_shape");
    return { id, updatedAt, userId, user: verifiedUser, content: { type, body: content.body } } as ImmutableLinearActivity;
  }
}
