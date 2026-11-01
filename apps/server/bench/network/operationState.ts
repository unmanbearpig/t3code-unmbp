import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import type { EnvironmentThreadState } from "../../../../packages/client-runtime/src/state/threads.ts";
import {
  operationMessageText,
  operationThreadId,
  type OperationControlResponse,
} from "./operationsFixture.ts";

/** Checks exact message contents and order, run membership, and progressive history. */
export function operationStateError(
  state: EnvironmentThreadState,
  expected: OperationControlResponse,
  bounded: boolean,
  status: "live" | "cached" = "live",
) {
  if (state.status !== status || Option.isNone(state.data) || Option.isSome(state.error))
    return `Client did not become ${status}`;
  const projection = state.data.value;
  if (new Set(projection.messages.map((message) => message.id)).size !== projection.messages.length)
    return "Duplicated message";
  const runIdsInState = new Set(projection.runs.map((run) => run.id));
  if (runIdsInState.size !== projection.runs.length) return "Duplicated run";
  if (
    projection.messages.some(
      (message) => message.runId === null || !runIdsInState.has(message.runId),
    )
  )
    return "Missing message run";
  const ids = bounded
    ? expected.windowMessageIds
    : Array.from({ length: expected.messages }, (_, i) => `operation-message-${i}`);
  if (ids.at(-1) !== `operation-message-${expected.messages - 1}`)
    return "Snapshot is missing the newest message";
  if (projection.thread.id !== operationThreadId || projection.messages.length !== ids.length)
    return "Wrong thread or message count";
  for (const [position, message] of projection.messages.entries()) {
    const index = Number(ids[position]!.replace("operation-message-", ""));
    if (
      message.id !== ids[position] ||
      message.threadId !== operationThreadId ||
      message.text !== operationMessageText(index) ||
      message.streaming ||
      message.role !== (index % 2 === 0 ? "user" : "assistant") ||
      message.runId !== `operation-run-${Math.floor(index / 2)}`
    )
      return "Missing, reordered, duplicated, or corrupted message";
  }
  const runIds = bounded
    ? expected.windowRunIds
    : Array.from({ length: Math.ceil(expected.messages / 2) }, (_, i) => `operation-run-${i}`);
  if (
    projection.runs.length !== runIds.length ||
    projection.runs.some((run, i) => run.id !== runIds[i] || run.status !== "completed")
  )
    return "Wrong runs after sync";
  if (state.history.hasMoreHistory !== (bounded && expected.hasMoreHistory))
    return "Wrong progressive history state";
  if (
    bounded &&
    (state.history.latestLocalTurnOrdinal !== expected.latestLocalTurnOrdinal ||
      (expected.hasMoreHistory && state.history.historyCursor === null))
  )
    return "Wrong progressive history cursor";
  if (ids.length === 0 || projection.visibleTurnItems.length !== ids.length)
    return "Missing visible conversation items";
  if (
    projection.visibleTurnItems.some(
      (row, i) =>
        row.sourceItemId !==
          `operation-item-${Number(ids[i]!.replace("operation-message-", ""))}` ||
        !(
          (row.item.type === "user_message" || row.item.type === "assistant_message") &&
          row.item.messageId === ids[i] &&
          row.item.text === projection.messages[i]!.text
        ),
    )
  )
    return "Wrong visible conversation items";
  return undefined;
}

/** Waits for all expected rows; content validation runs after recording completion time. */
export const waitForOperationState = (
  state: SubscriptionRef.SubscriptionRef<EnvironmentThreadState>,
  expected: OperationControlResponse,
  bounded: boolean,
  status: "live" | "cached" = "live",
) =>
  SubscriptionRef.changes(state).pipe(
    Stream.filter(
      (current) =>
        current.status === status &&
        Option.isSome(current.data) &&
        Option.isNone(current.error) &&
        current.data.value.messages.length ===
          (bounded ? expected.windowMessageIds.length : expected.messages) &&
        current.data.value.visibleTurnItems.length ===
          (bounded ? expected.windowMessageIds.length : expected.messages) &&
        current.data.value.runs.length ===
          (bounded ? expected.windowRunIds.length : Math.ceil(expected.messages / 2)),
    ),
    Stream.runHead,
    Effect.map(Option.getOrThrow),
  );
