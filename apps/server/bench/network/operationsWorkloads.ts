// @effect-diagnostics nodeBuiltinImport:off - Reads counters from the benchmark's private network namespace.
import * as NodeFS from "node:fs";

import { ORCHESTRATION_V2_WS_METHODS, WsRpcGroup, type ServerConfig } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { HttpClient } from "effect/http";
import { RpcClient } from "effect/rpc";

import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type PreparedConnection,
  type SupervisorConnectionState,
} from "../../../../packages/client-runtime/src/connection/model.ts";
import * as EnvironmentSupervisor from "../../../../packages/client-runtime/src/connection/supervisor.ts";
import * as Persistence from "../../../../packages/client-runtime/src/platform/persistence.ts";
import type { RpcSession } from "../../../../packages/client-runtime/src/rpc/session.ts";
import * as BoundedSnapshot from "../../../../packages/client-runtime/src/state/boundedThreadSnapshotHttp.ts";
import * as ThreadHistoryController from "../../../../packages/client-runtime/src/state/threadHistoryController.ts";
import {
  makeEnvironmentThreadState,
  type EnvironmentThreadState,
} from "../../../../packages/client-runtime/src/state/threads.ts";

import type { TlsMaterial } from "./client.ts";
import { wallClockMs } from "./protocol.ts";
import { makeRpcProtocol, type RpcTransportName } from "./rpcClient.ts";
import {
  operationMessageText,
  operationServerConfig,
  operationSettings,
  operationThreadId,
  type OperationControl,
  type OperationControlResponse,
  type OperationName,
} from "./operationsFixture.ts";
import type { Recorder } from "./workloads.ts";
import { makeSnapshotHttpClient, snapshotPort, type SnapshotHttpProtocol } from "./http3.ts";
import { operationRpcCompression, operationRpcSerialization } from "./operationsRpc.ts";

/** Counts IP traffic in both directions, including TCP/QUIC headers and retransmissions. */
function readLinkCounters() {
  const line = NodeFS.readFileSync("/proc/net/dev", "utf8")
    .split("\n")
    .find((line) => line.trimStart().startsWith("lo:"));
  if (!line) throw new Error("No loopback counters in benchmark namespace");
  const columns = line.split(":")[1]!.trim().split(/\s+/).map(Number);
  return { bytes: columns[8]!, packets: columns[9]! };
}

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

export const runOperation = Effect.fnUntraced(function* (
  name: OperationName,
  transport: RpcTransportName,
  tls: TlsMaterial,
  record: Recorder,
  control: OperationControl,
  httpProtocol?: SnapshotHttpProtocol,
) {
  const scope = yield* Effect.scope;
  record.set("phase", "setup");
  record.set(
    "rpcCompression",
    transport.startsWith("rpc-wt")
      ? operationRpcCompression
      : transport === "rpc-ws"
        ? "permessage-deflate"
        : "none",
  );
  let snapshots = 0;
  let events = 0;
  let streamItems = 0;
  let httpLoads = 0;
  let lastAfterSequence: number | undefined;
  let coldStartedAt = 0;
  let coldCounters: ReturnType<typeof readLinkCounters> | undefined;
  let deliveredSequence: number | undefined;
  let snapshotSequence: number | undefined;
  let sequenceError = false;
  const expectedInitial = yield* control(0);
  const cold = name === "open-new" || name === "open-long";
  const snapshotClient = yield* makeSnapshotHttpClient(
    httpProtocol ?? (transport.startsWith("rpc-wt") ? "h3" : "h1"),
    tls,
    record,
  );
  const target = new PrimaryConnectionTarget({
    environmentId: operationServerConfig.environment.environmentId,
    label: "Benchmark",
    httpBaseUrl: "https://127.0.0.1:" + snapshotPort,
    wsBaseUrl: "ws://127.0.0.1:4404",
  });
  const prepared: PreparedConnection = {
    environmentId: target.environmentId,
    label: target.label,
    httpBaseUrl: target.httpBaseUrl,
    socketUrl: target.wsBaseUrl,
    httpAuthorization: null,
    target,
  };
  const supervisorState = yield* SubscriptionRef.make<SupervisorConnectionState>({
    ...AVAILABLE_CONNECTION_STATE,
    desired: true,
    phase: "connecting",
  });
  const sessions = yield* SubscriptionRef.make<Option.Option<RpcSession>>(Option.none());
  const preparedRef = yield* SubscriptionRef.make(Option.some(prepared));
  const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
    target,
    state: supervisorState,
    session: sessions,
    prepared: preparedRef,
    connect: Effect.void,
    disconnect: Effect.void,
    retryNow: Effect.void,
  });
  const cache = Layer.mock(Persistence.EnvironmentCacheStore)({
    loadThread: () => Effect.succeed(Option.fromUndefinedOr(expectedInitial.initialSnapshot)),
    saveThread: () => Effect.void,
    removeThread: () => Effect.void,
  });
  const clientContext = yield* Layer.buildWithScope(
    Layer.mergeAll(cache, BoundedSnapshot.layer, ThreadHistoryController.layer).pipe(
      Layer.provideMerge(
        Layer.effect(
          HttpClient.HttpClient,
          Effect.map(HttpClient.HttpClient, (client) =>
            HttpClient.mapRequest(client, (request) => {
              httpLoads += 1;
              return request;
            }),
          ),
        ).pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, snapshotClient))),
      ),
    ),
    scope,
  );
  const state = yield* makeEnvironmentThreadState(operationThreadId).pipe(
    Effect.provide(clientContext),
    Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
  );

  const open = Effect.fnUntraced(function* (expected: OperationControlResponse, bounded: boolean) {
    yield* SubscriptionRef.set(supervisorState, {
      ...AVAILABLE_CONNECTION_STATE,
      desired: true,
      phase: "connecting" as const,
    });
    const connectionScope = yield* Scope.fork(scope);
    const connectionStartedAt = wallClockMs();
    const synchronized = yield* Deferred.make<void>();
    const config = yield* Deferred.make<ServerConfig>();
    const context = yield* makeRpcProtocol(transport, tls, operationRpcSerialization).pipe(
      Scope.provide(connectionScope),
    );
    const protocol = Context.get(context, RpcClient.Protocol);
    const threadRequests = new Set<string | number>();
    const measuredProtocol = RpcClient.Protocol.of({
      ...protocol,
      send: (clientId, message, transferables) => {
        if (
          message._tag === "Request" &&
          message.tag === ORCHESTRATION_V2_WS_METHODS.subscribeThread
        ) {
          threadRequests.add(message.id);
          lastAfterSequence =
            Predicate.hasProperty(message.payload, "afterSequence") &&
            typeof message.payload.afterSequence === "number"
              ? message.payload.afterSequence
              : undefined;
          deliveredSequence = lastAfterSequence;
        }
        return protocol.send(clientId, message, transferables);
      },
      run: (clientId, receive) =>
        protocol.run(clientId, (message) =>
          Effect.gen(function* () {
            if (message._tag === "Chunk" && threadRequests.has(message.requestId)) {
              for (const value of message.values) {
                streamItems += 1;
                if (!Predicate.hasProperty(value, "kind")) continue;
                if (value.kind === "snapshot") {
                  snapshots += 1;
                  snapshotSequence =
                    Predicate.hasProperty(value, "snapshotSequence") &&
                    typeof value.snapshotSequence === "number"
                      ? value.snapshotSequence
                      : undefined;
                }
                if (value.kind === "event") {
                  events += 1;
                  if (
                    !Predicate.hasProperty(value, "sequence") ||
                    typeof value.sequence !== "number" ||
                    value.sequence !== (deliveredSequence ?? 0) + 1
                  )
                    sequenceError = true;
                  if (
                    Predicate.hasProperty(value, "sequence") &&
                    typeof value.sequence === "number"
                  )
                    deliveredSequence = value.sequence;
                }
                if (value.kind === "synchronized") yield* Deferred.succeed(synchronized, undefined);
              }
            }
            yield* receive(message);
          }),
        ),
    });
    const client = yield* RpcClient.make(WsRpcGroup).pipe(
      Effect.provideService(RpcClient.Protocol, measuredProtocol),
      Scope.provide(connectionScope),
    );
    yield* client.subscribeServerConfig({}).pipe(
      Stream.runForEach((event) =>
        event.type === "snapshot" ? Deferred.succeed(config, event.config) : Effect.void,
      ),
      Effect.orDie,
      Effect.tapCause((cause) => Deferred.failCause(config, cause)),
      Effect.forkIn(connectionScope),
    );
    const session: RpcSession = {
      client,
      initialConfig: Deferred.await(config),
      subscribeServerConfig: client.subscribeServerConfig,
      ready: Deferred.await(config).pipe(Effect.asVoid),
      probe: Effect.void,
      closed: Effect.never,
    };
    if (cold) {
      yield* Deferred.await(config);
      record.set("connectionReadyMs", wallClockMs() - connectionStartedAt);
      coldCounters = readLinkCounters();
      coldStartedAt = wallClockMs();
      record.set("phase", "operation");
    }
    yield* SubscriptionRef.set(sessions, Option.some(session));
    yield* Deferred.await(synchronized);
    yield* waitForOperationState(state, expected, bounded);
    return connectionScope;
  });

  let connection: Scope.Closeable;
  let expected = expectedInitial;
  let bounded = cold;
  if (cold || name === "sync-five") {
    connection = yield* open(expectedInitial, cold);
    if (!cold)
      record.check(
        operationStateError(yield* SubscriptionRef.get(state), expectedInitial, false) ===
          undefined,
        "Invalid initial 100-message client fixture",
      );
  } else {
    // Reconnect starts from the same offline cache/cursor. A warm-up socket adds
    // delayed teardown and stale subscriptions to the measured traffic and loss draws.
    yield* SubscriptionRef.set(supervisorState, AVAILABLE_CONNECTION_STATE);
    const cached = yield* waitForOperationState(state, expectedInitial, false, "cached");
    record.check(
      operationStateError(cached, expectedInitial, false, "cached") === undefined,
      "Invalid initial 100-message client fixture",
    );
  }
  if (!cold) {
    snapshots = 0;
    events = 0;
    streamItems = 0;
    record.check(httpLoads === 0, "Loaded state unexpectedly needed an HTTP snapshot");
  }
  const counters = coldCounters ?? readLinkCounters();
  if (name === "sync-five") {
    record.set("phase", "operation");
    const before = readLinkCounters();
    const applied = yield* Effect.forkChild(
      waitForOperationState(
        state,
        {
          ...expectedInitial,
          messages: expectedInitial.messages + operationSettings.addedMessages,
        },
        false,
      ).pipe(Effect.map((state) => ({ state, completedAt: wallClockMs() }))),
    );
    snapshots = 0;
    events = 0;
    streamItems = 0;
    expected = yield* control(operationSettings.addedMessages);
    const completed = yield* Fiber.join(applied);
    record.set("operationMs", completed.completedAt - expected.startedAt);
    record.check(
      operationStateError(completed.state, expected, false) === undefined,
      "Invalid incremental client state",
    );
    Object.assign(counters, before);
  } else if (!cold) {
    const count =
      name === "reconnect-replay"
        ? operationSettings.replayMessages
        : name === "reconnect-snapshot"
          ? operationSettings.snapshotMessages
          : 0;
    expected = yield* control(count);
    Object.assign(counters, readLinkCounters());
    const startedAt = wallClockMs();
    record.set("phase", "operation");
    bounded = name === "reconnect-snapshot";
    connection = yield* open(expected, bounded);
    record.set("operationMs", wallClockMs() - startedAt);
  } else {
    record.set("operationMs", wallClockMs() - coldStartedAt);
  }
  const after = readLinkCounters();
  const final = yield* SubscriptionRef.get(state);
  const error = operationStateError(final, expected, bounded);
  record.check(error === undefined, error ?? "Invalid client state");
  record.check(
    lastAfterSequence === expectedInitial.sequence,
    "Subscription did not use the loaded cursor",
  );
  const expectedSnapshots = name === "reconnect-snapshot" ? 1 : 0;
  record.check(snapshots === expectedSnapshots, "Wrong replay/snapshot path");
  record.check(!sequenceError, "Missing, duplicated, or reordered event sequence");
  if (snapshots > 0)
    record.check(
      snapshotSequence === expected.sequence,
      "Snapshot did not reach the server cursor",
    );
  const expectedEvents = name === "sync-five" ? 13 : name === "reconnect-replay" ? 125 : 0;
  record.check(events === expectedEvents, "Wrong delivered event count");
  record.set("linkTxBytes", after.bytes - counters.bytes);
  record.set("linkTxPackets", after.packets - counters.packets);
  record.set("streamItems", streamItems);
  record.set("events", events);
  record.set("socketSnapshots", snapshots);
  record.set("httpSnapshots", cold ? httpLoads : 0);
  record.check(httpLoads === (cold ? 1 : 0), "Unexpected HTTP refetch or fallback during sync");
  record.set("syncMode", cold ? "http+replay" : bounded ? "snapshot" : "replay");
  record.set("loadedMessages", Option.getOrThrow(final.data).messages.length);
  record.set("totalMessages", expected.messages);
  record.set("phase", "complete");
  yield* SubscriptionRef.set(sessions, Option.none());
  yield* Scope.close(connection!, Exit.void);
});
