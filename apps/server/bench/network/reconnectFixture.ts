import {
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  OrchestrationV2ThreadDetailSnapshot,
  ThreadId,
  TurnItemId,
  type OrchestrationV2DomainEvent,
  type ServerConfig,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";

export const operationSettings = {
  textCharacters: 2000,
  textGenerator: { multiplier: 1664525, increment: 1013904223, initialSeed: "message-index+1" },
  timestamps: { createdAt: "2026-10-14T00:00:00.000Z", stepMs: 1000 },
} as const;

export const OperationControlResponse = Schema.Struct({
  startedAt: Schema.Number,
  sequence: Schema.Number,
  messages: Schema.Number,
  windowMessageIds: Schema.Array(Schema.String),
  windowRunIds: Schema.Array(Schema.String),
  hasMoreHistory: Schema.Boolean,
  latestLocalTurnOrdinal: Schema.NullOr(Schema.Number),
  initialSnapshot: Schema.optional(OrchestrationV2ThreadDetailSnapshot),
});
export type OperationControlResponse = typeof OperationControlResponse.Type;
export const operationThreadId = ThreadId.make("network-operation-thread");
const provider = ProviderInstanceId.make("codex");
const now = DateTime.makeUnsafe(operationSettings.timestamps.createdAt);

/** Unique, reproducible ASCII text avoids repeating one body across every message. */
export function operationMessageText(index: number) {
  let seed = index + 1;
  let text = `Message ${index}. `;
  while (text.length < operationSettings.textCharacters) {
    seed =
      (Math.imul(seed, operationSettings.textGenerator.multiplier) +
        operationSettings.textGenerator.increment) >>>
      0;
    text += `${seed.toString(36)} `;
  }
  return text.slice(0, operationSettings.textCharacters);
}

export const operationThreadCreated: OrchestrationV2DomainEvent = {
  id: EventId.make("operation-thread-created"),
  type: "thread.created",
  threadId: operationThreadId,
  occurredAt: now,
  payload: {
    id: operationThreadId,
    projectId: ProjectId.make("operation-project"),
    title: "Network operation",
    providerInstanceId: provider,
    modelSelection: { instanceId: provider, model: "gpt-5.4" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: operationThreadId },
    forkedFrom: null,
    createdBy: "user",
    creationSource: "web",
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  },
};

/** One completed run per user/assistant pair, including runs introduced while offline. */
export function operationMessageEvents(start: number, count: number) {
  const events: OrchestrationV2DomainEvent[] = [];
  for (let index = start; index < start + count; index++) {
    const now = DateTime.makeUnsafe(
      DateTime.toEpochMillis(operationThreadCreated.occurredAt) +
        index * operationSettings.timestamps.stepMs,
    );
    const runId = RunId.make(`operation-run-${Math.floor(index / 2)}`);
    const nodeId = NodeId.make(`operation-node-${Math.floor(index / 2)}`);
    if (index % 2 === 0) {
      events.push({
        id: EventId.make(`operation-run-${index}`),
        type: "run.created",
        threadId: operationThreadId,
        occurredAt: now,
        payload: {
          id: runId,
          threadId: operationThreadId,
          ordinal: Math.floor(index / 2) + 1,
          providerInstanceId: provider,
          modelSelection: { instanceId: provider, model: "gpt-5.4" },
          providerThreadId: null,
          userMessageId: MessageId.make(`operation-message-${index}`),
          rootNodeId: nodeId,
          activeAttemptId: null,
          status: "completed",
          requestedAt: now,
          startedAt: now,
          completedAt: now,
          checkpointId: null,
          contextHandoffId: null,
        },
      });
    }
    events.push({
      id: EventId.make(`operation-message-${index}`),
      type: "message.updated",
      threadId: operationThreadId,
      runId,
      occurredAt: now,
      payload: {
        id: MessageId.make(`operation-message-${index}`),
        threadId: operationThreadId,
        runId,
        nodeId,
        role: index % 2 === 0 ? "user" : "assistant",
        text: operationMessageText(index),
        attachments: [],
        streaming: false,
        createdBy: index % 2 === 0 ? "user" : "agent",
        creationSource: "web",
        createdAt: now,
        updatedAt: now,
      },
    });
    const base = {
      id: TurnItemId.make(`operation-item-${index}`),
      threadId: operationThreadId,
      runId,
      nodeId,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: index + 1,
      status: "completed" as const,
      title: null,
      startedAt: now,
      completedAt: now,
      updatedAt: now,
      messageId: MessageId.make(`operation-message-${index}`),
      text: operationMessageText(index),
    };
    events.push({
      id: EventId.make(`operation-item-${index}`),
      type: "turn-item.updated",
      threadId: operationThreadId,
      runId,
      nodeId,
      occurredAt: now,
      payload:
        index % 2 === 0
          ? {
              ...base,
              type: "user_message",
              createdBy: "user",
              creationSource: "web",
              inputIntent: "turn_start",
              attachments: [],
            }
          : { ...base, type: "assistant_message", streaming: false, attachments: [] },
    });
  }
  return events;
}

// Configuration travels over the tested transport. Authorization and provider discovery
// are deliberately fixed so these cases isolate thread synchronization.
export const operationServerConfig: ServerConfig = {
  environment: {
    environmentId: EnvironmentId.make("network-operation-environment"),
    label: "Network benchmark",
    platform: { os: "linux", arch: "x64" },
    serverVersion: "0.0.0-bench",
    capabilities: { repositoryIdentity: true },
  },
  auth: {
    policy: "loopback-browser",
    bootstrapMethods: ["one-time-token"],
    sessionMethods: ["browser-session-cookie", "bearer-access-token"],
    sessionCookieName: "t3_session",
  },
  cwd: "/benchmark",
  keybindingsConfigPath: "/benchmark/keybindings.json",
  keybindings: [],
  issues: [],
  providers: [],
  availableEditors: [],
  observability: {
    logsDirectoryPath: "/benchmark/logs",
    localTracingEnabled: false,
    otlpTracesEnabled: false,
    otlpMetricsEnabled: false,
    otlpLogsEnabled: false,
  },
  settings: DEFAULT_SERVER_SETTINGS,
  threadResumeCompletionMarker: true,
};
