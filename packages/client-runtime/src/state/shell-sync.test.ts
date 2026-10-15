import {
  EnvironmentId,
  ORCHESTRATION_V2_WS_METHODS,
  WS_METHODS,
  type OrchestrationV2ShellSnapshot,
  type OrchestrationV2ShellStreamItem,
  type ServerConfig,
  type ServerLifecycleStreamEvent,
  ThreadId,
  type ThreadPullRequestLink,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as TestClock from "effect/testing/TestClock";
import * as HttpClient from "effect/http/HttpClient";
import * as RpcClientError from "effect/rpc/RpcClientError";
import * as Socket from "effect/socket/Socket";

import {
  AVAILABLE_CONNECTION_STATE,
  ConnectionTransientError,
  PrimaryConnectionTarget,
  SshConnectionTarget,
  type ConnectionAttemptError,
  type PreparedConnection,
  type SupervisorConnectionState,
} from "../connection/model.ts";
import type { ConnectionCatalogEntry } from "../connection/catalog.ts";
import * as Connectivity from "../connection/connectivity.ts";
import * as ConnectionDriver from "../connection/driver.ts";
import * as ConnectionResolver from "../connection/resolver.ts";
import { environmentMismatchError } from "../connection/errors.ts";
import * as EnvironmentSupervisor from "../connection/supervisor.ts";
import * as ConnectionWakeups from "../connection/wakeups.ts";
import * as Persistence from "../platform/persistence.ts";
import * as RpcSession from "../rpc/session.ts";
import type { WsRpcProtocolClient } from "../rpc/protocol.ts";
import { makeEnvironmentShellState, type EnvironmentShellState } from "./shell.ts";
import * as ShellSnapshotLoader from "./shellSnapshotHttp.ts";
import type { DeferredShellSnapshot } from "./shellPullRequests.ts";
import { v2Project, v2ShellSnapshot, v2ThreadShell } from "./orchestrationV2TestFixtures.ts";

const TARGET = new PrimaryConnectionTarget({
  environmentId: EnvironmentId.make("environment-1"),
  label: "Test environment",
  httpBaseUrl: "https://environment.example.test",
  wsBaseUrl: "wss://environment.example.test",
});

const PREPARED: PreparedConnection = {
  environmentId: TARGET.environmentId,
  label: TARGET.label,
  httpBaseUrl: TARGET.httpBaseUrl,
  socketUrl: TARGET.wsBaseUrl,
  httpAuthorization: null,
  target: TARGET,
};

const LIVE_SHELL_SNAPSHOT: OrchestrationV2ShellSnapshot = {
  ...v2ShellSnapshot,
  snapshotSequence: 1,
};

const INITIAL_CONFIG = {
  shellResumeCompletionMarker: true,
  environment: {
    environmentId: TARGET.environmentId,
    serverVersion: "1.0.0",
    orchestrationProtocolVersion: 2,
  },
} as ServerConfig;
const SERVER_STARTED_AT = "2026-06-20T00:00:00.000Z";

function session(client: WsRpcProtocolClient, config = INITIAL_CONFIG): RpcSession.RpcSession {
  return {
    client,
    initialConfig: Effect.succeed(config),
    subscribeServerConfig: (input) => client.subscribeServerConfig(input),
    ready: Effect.void,
    probe: Effect.void,
    closed: Effect.never,
  };
}

const makeWarmReconnectHarness = Effect.fnUntraced(function* (
  input: {
    readonly cached?: DeferredShellSnapshot;
    readonly load?: (call: number) => Effect.Effect<Option.Option<DeferredShellSnapshot>>;
    readonly config?: ServerConfig;
    readonly lifecycle?: Stream.Stream<ServerLifecycleStreamEvent>;
  } = {},
) {
  const requests = yield* Queue.unbounded<{
    readonly input: { readonly afterSequence?: number; readonly requestCompletionMarker?: boolean };
    readonly events: Queue.Queue<OrchestrationV2ShellStreamItem>;
    readonly stopped: Deferred.Deferred<void>;
    readonly failure: Deferred.Deferred<never, RpcClientError.RpcClientError>;
  }>();
  const wakeups = yield* Queue.unbounded<ConnectionWakeups.ConnectionWakeup>();
  const lifecycleRequests = yield* Queue.unbounded<string>();
  const client = {
    [ORCHESTRATION_V2_WS_METHODS.subscribeShell]: (subscribeInput: {
      readonly afterSequence?: number;
      readonly requestCompletionMarker?: boolean;
    }) =>
      Stream.unwrap(
        Effect.gen(function* () {
          const events = yield* Queue.unbounded<OrchestrationV2ShellStreamItem>();
          const stopped = yield* Deferred.make<void>();
          const failure = yield* Deferred.make<never, RpcClientError.RpcClientError>();
          yield* Queue.offer(requests, { input: subscribeInput, events, stopped, failure });
          return Stream.fromQueue(events).pipe(
            Stream.merge(Stream.fromEffect(Deferred.await(failure)), { haltStrategy: "either" }),
            Stream.ensuring(Deferred.succeed(stopped, undefined)),
          );
        }),
      ),
  };
  const makeSession = (config = INITIAL_CONFIG, startedAt = SERVER_STARTED_AT) =>
    session(
      {
        ...client,
        [WS_METHODS.subscribeServerLifecycle]: () =>
          Stream.unwrap(
            Queue.offer(lifecycleRequests, startedAt).pipe(
              Effect.as(
                input.lifecycle ??
                  Stream.succeed<ServerLifecycleStreamEvent>({
                    version: 1 as const,
                    sequence: 1,
                    type: "ready" as const,
                    payload: { at: startedAt, environment: config.environment },
                  }),
              ),
            ),
          ),
      } as unknown as WsRpcProtocolClient,
      config,
    );
  const activeSession = yield* SubscriptionRef.make(Option.some(makeSession(input.config)));
  const supervisorState = yield* SubscriptionRef.make<SupervisorConnectionState>({
    ...AVAILABLE_CONNECTION_STATE,
    desired: true,
    network: "online" as const,
    phase: "connected" as const,
  });
  const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
    target: TARGET,
    state: supervisorState,
    session: activeSession,
    prepared: yield* SubscriptionRef.make(Option.some(PREPARED)),
    connect: Effect.void,
    disconnect: Effect.void,
    retryNow: Effect.void,
  });
  const cache = Persistence.EnvironmentCacheStore.of({
    loadShell: () => Effect.succeed(Option.fromUndefinedOr(input.cached)),
    saveShell: () => Effect.void,
    loadThread: () => Effect.succeedNone,
    saveThread: () => Effect.void,
    removeThread: () => Effect.void,
    loadServerConfig: () => Effect.succeedNone,
    saveServerConfig: () => Effect.void,
    loadVcsRefs: () => Effect.succeedNone,
    saveVcsRefs: () => Effect.void,
    removeVcsRefs: () => Effect.void,
    clearVcsRefs: () => Effect.void,
    clear: () => Effect.void,
  });
  const loaderCalls = yield* Ref.make(0);
  const cancelledPrefetches = yield* Ref.make(0);
  const loader = yield* ShellSnapshotLoader.make.pipe(
    Effect.provideService(
      HttpClient.HttpClient,
      HttpClient.make(() => Effect.die("Unexpected HTTP")),
    ),
  );
  const scope = yield* Scope.make();
  yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
  const state = yield* makeEnvironmentShellState().pipe(
    Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
    Effect.provideService(Persistence.EnvironmentCacheStore, cache),
    Effect.provideService(ShellSnapshotLoader.ShellSnapshotLoader, {
      ...loader,
      cancelPrefetch: () => Ref.update(cancelledPrefetches, (count) => count + 1),
      load: () =>
        Ref.updateAndGet(loaderCalls, (count) => count + 1).pipe(
          Effect.flatMap((call) => input.load?.(call) ?? Effect.succeedSome(LIVE_SHELL_SNAPSHOT)),
        ),
    }),
    Effect.provideService(ConnectionWakeups.ConnectionWakeups, {
      changes: Stream.fromQueue(wakeups),
    }),
    Scope.provide(scope),
  );
  const waitForState = (predicate: (value: EnvironmentShellState) => boolean) =>
    SubscriptionRef.changes(state).pipe(
      Stream.filter(predicate),
      Stream.runHead,
      Effect.map(Option.getOrThrow),
    );
  return {
    state,
    requests,
    lifecycleRequests,
    loaderCalls,
    cancelledPrefetches,
    scope,
    shouldPrefetch: loader.shouldPrefetch!,
    waitForState,
    complete: (request: { readonly events: Queue.Queue<OrchestrationV2ShellStreamItem> }) =>
      Queue.offer(request.events, { kind: "synchronized" }).pipe(
        Effect.andThen(waitForState((value) => value.status === "live")),
      ),
    disconnect: (request: { readonly stopped: Deferred.Deferred<void> }) =>
      SubscriptionRef.set(activeSession, Option.none()).pipe(
        Effect.andThen(Deferred.await(request.stopped)),
        Effect.andThen(
          SubscriptionRef.set(supervisorState, {
            ...AVAILABLE_CONNECTION_STATE,
            network: "offline" as const,
          }),
        ),
        Effect.andThen(waitForState((value) => value.status === "cached")),
      ),
    reconnect: (config = INITIAL_CONFIG, startedAt = SERVER_STARTED_AT) =>
      SubscriptionRef.set(supervisorState, {
        ...AVAILABLE_CONNECTION_STATE,
        desired: true,
        network: "online" as const,
        phase: "connected" as const,
      }).pipe(
        Effect.andThen(
          SubscriptionRef.set(activeSession, Option.some(makeSession(config, startedAt))),
        ),
      ),
    resubscribe: () => Queue.offer(wakeups, "application-active"),
  };
});

const makeReconnectAttempt = Effect.fnUntraced(function* (
  prepared: PreparedConnection = { ...PREPARED },
) {
  return {
    prepared,
    ready: yield* Deferred.make<void, ConnectionAttemptError>(),
    httpStarted: yield* Deferred.make<PreparedConnection>(),
    httpResult: yield* Deferred.make<Option.Option<DeferredShellSnapshot>>(),
    httpCompleted: yield* Deferred.make<void>(),
    httpInterrupted: yield* Deferred.make<void>(),
    socketReleased: yield* Deferred.make<void>(),
  };
});

const makeReconnectHarness = Effect.fnUntraced(function* (
  entry: ConnectionCatalogEntry = { target: TARGET, profile: Option.none(), enabled: true },
) {
  const attempts =
    yield* Queue.unbounded<Effect.Success<ReturnType<typeof makeReconnectAttempt>>>();
  const preparedAttempts = new Map<
    PreparedConnection,
    Effect.Success<ReturnType<typeof makeReconnectAttempt>>
  >();
  const subscriptions = yield* Queue.unbounded<{
    readonly afterSequence?: number;
    readonly requestCompletionMarker?: true;
  }>();
  const events = yield* Queue.unbounded<OrchestrationV2ShellStreamItem>();
  const wakeups = yield* Queue.unbounded<ConnectionWakeups.ConnectionWakeup>();
  const loaderCalls = yield* Ref.make(0);
  const driver = yield* ConnectionDriver.make.pipe(
    Effect.provideService(
      ConnectionResolver.ConnectionResolver,
      ConnectionResolver.ConnectionResolver.of({
        prepare: () =>
          Queue.take(attempts).pipe(
            Effect.map((attempt) => {
              preparedAttempts.set(attempt.prepared, attempt);
              return attempt.prepared;
            }),
          ),
        prepareForUpdate: () => Effect.die("Update preparation is not used here."),
      }),
    ),
    Effect.provideService(RpcSession.RpcSessionFactory, {
      connect: (prepared) => {
        const attempt = preparedAttempts.get(prepared)!;
        const client = {
          [ORCHESTRATION_V2_WS_METHODS.subscribeShell]: (input: {
            readonly afterSequence?: number;
            readonly requestCompletionMarker?: true;
          }) =>
            Stream.fromEffect(Queue.offer(subscriptions, input)).pipe(
              Stream.flatMap(() => Stream.fromQueue(events)),
            ),
        } as unknown as WsRpcProtocolClient;
        return Effect.acquireRelease(
          Effect.succeed({ ...session(client), ready: Deferred.await(attempt.ready) }),
          () => Deferred.succeed(attempt.socketReleased, undefined),
        );
      },
    }),
    Effect.provideService(
      HttpClient.HttpClient,
      HttpClient.make(() => Effect.die("These routes do not need an HTTP reachability check.")),
    ),
  );
  const supervisor = yield* EnvironmentSupervisor.make(entry).pipe(
    Effect.provideService(ConnectionDriver.ConnectionDriver, driver),
    Effect.provideService(Connectivity.Connectivity, {
      status: Effect.succeed("online" as const),
      changes: Stream.never,
    }),
    Effect.provideService(ConnectionWakeups.ConnectionWakeups, {
      changes: Stream.fromQueue(wakeups),
    }),
  );
  const shell = yield* makeEnvironmentShellState().pipe(
    Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
    Effect.provideService(Persistence.EnvironmentCacheStore, {
      loadShell: () => Effect.succeedSome(LIVE_SHELL_SNAPSHOT),
      saveShell: () => Effect.void,
      loadThread: () => Effect.succeedNone,
      saveThread: () => Effect.void,
      removeThread: () => Effect.void,
      loadServerConfig: () => Effect.succeedNone,
      saveServerConfig: () => Effect.void,
      loadVcsRefs: () => Effect.succeedNone,
      saveVcsRefs: () => Effect.void,
      removeVcsRefs: () => Effect.void,
      clearVcsRefs: () => Effect.void,
      clear: () => Effect.void,
    }),
    Effect.provideService(ShellSnapshotLoader.ShellSnapshotLoader, {
      load: (prepared) => {
        const attempt = preparedAttempts.get(prepared)!;
        return Ref.update(loaderCalls, (count) => count + 1).pipe(
          Effect.andThen(Deferred.succeed(attempt.httpStarted, prepared)),
          Effect.andThen(Deferred.await(attempt.httpResult)),
          Effect.tap(() => Deferred.succeed(attempt.httpCompleted, undefined)),
          Effect.onInterrupt(() => Deferred.succeed(attempt.httpInterrupted, undefined)),
        );
      },
    }),
  );
  return { supervisor, shell, attempts, subscriptions, events, wakeups, loaderCalls };
});

describe("environment shell synchronization", () => {
  it.effect("prefetches the new session's authoritative shell before reconnect readiness", () =>
    Effect.gen(function* () {
      const harness = yield* makeReconnectHarness();
      const initial = yield* makeReconnectAttempt();
      const initialSnapshot = { ...LIVE_SHELL_SNAPSHOT, snapshotSequence: 40 };
      yield* Queue.offer(harness.attempts, initial);
      yield* harness.supervisor.connect;
      yield* Deferred.await(initial.httpStarted);
      yield* Deferred.succeed(initial.httpResult, Option.some(initialSnapshot));
      yield* Deferred.succeed(initial.ready, undefined);
      expect((yield* Queue.take(harness.subscriptions)).afterSequence).toBe(40);
      yield* Queue.offer(harness.events, { kind: "synchronized" });
      yield* SubscriptionRef.changes(harness.shell).pipe(
        Stream.filter((state) => state.status === "live"),
        Stream.runHead,
      );

      const reconnect = yield* makeReconnectAttempt({
        ...PREPARED,
        httpBaseUrl: "https://new-route.example.test",
        socketUrl: "wss://new-route.example.test/ws",
        httpAuthorization: { _tag: "Bearer", token: "new-attempt-credential" },
      });
      yield* Queue.offer(harness.attempts, reconnect);
      yield* Queue.offer(harness.wakeups, "application-active-reconnect");
      expect(yield* Deferred.await(reconnect.httpStarted)).toBe(reconnect.prepared);
      expect(yield* Deferred.isDone(initial.socketReleased)).toBe(true);
      expect(yield* Deferred.isDone(reconnect.ready)).toBe(false);
      expect(Option.isNone(yield* SubscriptionRef.get(harness.supervisor.session))).toBe(true);

      // A valid high cursor must not keep incomplete old rows. The new attempt
      // replaces them authoritatively, even when the server sequence is lower.
      const snapshot = { ...LIVE_SHELL_SNAPSHOT, snapshotSequence: 9, threads: [] };
      yield* Deferred.succeed(reconnect.httpResult, Option.some(snapshot));
      yield* Deferred.await(reconnect.httpCompleted);
      expect(Option.getOrThrow((yield* SubscriptionRef.get(harness.shell)).snapshot)).toEqual(
        initialSnapshot,
      );
      yield* Deferred.succeed(reconnect.ready, undefined);
      expect(yield* Queue.take(harness.subscriptions)).toEqual({
        afterSequence: 9,
        requestCompletionMarker: true,
      });
      expect((yield* SubscriptionRef.get(harness.shell)).status).toBe("synchronizing");
      expect(Option.getOrThrow((yield* SubscriptionRef.get(harness.shell)).snapshot)).toEqual(
        snapshot,
      );

      // Events committed between HTTP and subscribe replay after its cursor.
      // Old/duplicate deltas cannot overwrite the newer row or advance twice.
      yield* Queue.offerAll(harness.events, [
        { kind: "thread.updated", sequence: 9, location: "active", thread: v2ThreadShell },
        {
          kind: "thread.updated",
          sequence: 10,
          location: "active",
          thread: { ...v2ThreadShell, title: "After snapshot" },
        },
        { kind: "thread.updated", sequence: 10, location: "active", thread: v2ThreadShell },
        { kind: "synchronized" },
      ]);
      const live = Option.getOrThrow(
        yield* SubscriptionRef.changes(harness.shell).pipe(
          Stream.filter((state) => state.status === "live"),
          Stream.runHead,
        ),
      );
      expect(Option.getOrThrow(live.snapshot).snapshotSequence).toBe(10);
      expect(Option.getOrThrow(live.snapshot).threads.map((thread) => thread.title)).toEqual([
        "After snapshot",
      ]);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(2);
    }),
  );

  it.effect.each([
    { failure: "transport", completed: false },
    { failure: "transport", completed: true },
    { failure: "identity", completed: false },
    { failure: "identity", completed: true },
    { failure: "config", completed: false },
    { failure: "config", completed: true },
  ] as const)(
    "discards a shell prefetch after a $failure failure with completed=$completed",
    ({ failure, completed }) =>
      Effect.gen(function* () {
        const harness = yield* makeReconnectHarness();
        const failed = yield* makeReconnectAttempt();
        yield* Queue.offer(harness.attempts, failed);
        yield* harness.supervisor.connect;
        yield* Deferred.await(failed.httpStarted);
        if (completed) {
          yield* Deferred.succeed(
            failed.httpResult,
            Option.some({ ...LIVE_SHELL_SNAPSHOT, snapshotSequence: 99, threads: [] }),
          );
          yield* Deferred.await(failed.httpCompleted);
        }
        const error =
          failure === "identity"
            ? environmentMismatchError({
                expected: TARGET.environmentId,
                actual: EnvironmentId.make("wrong-environment"),
              })
            : new ConnectionTransientError({
                reason: failure === "config" ? "remote-unavailable" : "transport",
                detail: failure === "config" ? "Config subscription failed." : "Socket failed.",
              });
        yield* Deferred.fail(failed.ready, error);
        yield* SubscriptionRef.changes(harness.supervisor.state).pipe(
          Stream.filter((state) => state.phase === "blocked" || state.phase === "backoff"),
          Stream.runHead,
        );
        if (!completed) yield* Deferred.await(failed.httpInterrupted);
        expect(Option.getOrThrow((yield* SubscriptionRef.get(harness.shell)).snapshot)).toEqual(
          LIVE_SHELL_SNAPSHOT,
        );
        expect(yield* Queue.size(harness.subscriptions)).toBe(0);
        // Even identical route/credential objects belong to a fresh attempt.
        const retry = yield* makeReconnectAttempt(failed.prepared);
        yield* Queue.offer(harness.attempts, retry);
        yield* harness.supervisor.retryNow;
        yield* Deferred.await(retry.httpStarted);
        yield* Deferred.succeed(
          retry.httpResult,
          Option.some({ ...LIVE_SHELL_SNAPSHOT, snapshotSequence: 7 }),
        );
        yield* Deferred.succeed(retry.ready, undefined);
        expect((yield* Queue.take(harness.subscriptions)).afterSequence).toBe(7);
        expect(yield* Ref.get(harness.loaderCalls)).toBe(2);
      }),
  );

  it.effect.each([false, true])(
    "discards a route's shell prefetch before the fallback route with completed=%s",
    (completed) =>
      Effect.gen(function* () {
        const firstTarget = new SshConnectionTarget({
          environmentId: TARGET.environmentId,
          label: TARGET.label,
          connectionId: "ssh:first",
        });
        const secondTarget = new SshConnectionTarget({
          ...firstTarget,
          connectionId: "ssh:second",
        });
        const harness = yield* makeReconnectHarness({
          target: firstTarget,
          profile: Option.none(),
          alternateRoutes: [{ target: secondTarget, profile: Option.none() }],
          enabled: true,
        });
        const first = yield* makeReconnectAttempt({ ...PREPARED, target: firstTarget });
        const second = yield* makeReconnectAttempt({
          ...PREPARED,
          target: secondTarget,
          httpBaseUrl: "https://fallback.example.test",
          httpAuthorization: { _tag: "Bearer", token: "fallback-credential" },
        });
        yield* Queue.offerAll(harness.attempts, [first, second]);
        yield* harness.supervisor.connect;
        yield* Deferred.await(first.httpStarted);
        if (completed) {
          yield* Deferred.succeed(
            first.httpResult,
            Option.some({ ...LIVE_SHELL_SNAPSHOT, snapshotSequence: 99 }),
          );
          yield* Deferred.await(first.httpCompleted);
        }
        yield* Deferred.fail(
          first.ready,
          new ConnectionTransientError({ reason: "transport", detail: "First route failed." }),
        );
        expect(yield* Deferred.await(second.httpStarted)).toBe(second.prepared);
        if (!completed) yield* Deferred.await(first.httpInterrupted);
        // A late result from the abandoned request must never reach shell state.
        yield* Deferred.succeed(
          first.httpResult,
          Option.some({ ...LIVE_SHELL_SNAPSHOT, snapshotSequence: 99 }),
        );
        yield* Deferred.succeed(second.ready, undefined);
        expect(yield* Queue.size(harness.subscriptions)).toBe(0);
        yield* Deferred.succeed(
          second.httpResult,
          Option.some({ ...LIVE_SHELL_SNAPSHOT, snapshotSequence: 8 }),
        );
        expect((yield* Queue.take(harness.subscriptions)).afterSequence).toBe(8);
        expect(
          Option.getOrThrow((yield* SubscriptionRef.get(harness.shell)).snapshot).snapshotSequence,
        ).toBe(8);
        expect(yield* Ref.get(harness.loaderCalls)).toBe(2);
      }),
  );

  it.effect("publishes live state before persistence and preserves it when ready", () =>
    Effect.gen(function* () {
      const events = yield* Queue.unbounded<OrchestrationV2ShellStreamItem>();
      const client = {
        [ORCHESTRATION_V2_WS_METHODS.subscribeShell]: () => Stream.fromQueue(events),
      } as unknown as WsRpcProtocolClient;
      const supervisorState = yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE);
      const activeSession = yield* SubscriptionRef.make<Option.Option<RpcSession.RpcSession>>(
        Option.some(session(client)),
      );
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: TARGET,
        state: supervisorState,
        session: activeSession,
        prepared: yield* SubscriptionRef.make(Option.some(PREPARED)),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
      const cache = Persistence.EnvironmentCacheStore.of({
        loadShell: () => Effect.succeed(Option.none()),
        saveShell: () => Effect.void,
        loadThread: () => Effect.succeed(Option.none()),
        saveThread: () => Effect.void,
        removeThread: () => Effect.void,
        loadServerConfig: () => Effect.succeedNone,
        saveServerConfig: () => Effect.void,
        loadVcsRefs: () => Effect.succeedNone,
        saveVcsRefs: () => Effect.void,
        removeVcsRefs: () => Effect.void,
        clearVcsRefs: () => Effect.void,
        clear: () => Effect.void,
      });
      // Cold cache with no HTTP snapshot available → falls back to the
      // socket-embedded snapshot.
      const snapshotLoader = ShellSnapshotLoader.ShellSnapshotLoader.of({
        load: () => Effect.succeedNone,
      });
      const shellState = yield* makeEnvironmentShellState().pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.provideService(Persistence.EnvironmentCacheStore, cache),
        Effect.provideService(ShellSnapshotLoader.ShellSnapshotLoader, snapshotLoader),
      );

      yield* SubscriptionRef.set(supervisorState, {
        desired: true,
        network: "online",
        phase: "connecting",
        stage: "synchronizing",
        attempt: 1,
        generation: 0,
        lastFailure: null,
        retryAt: null,
      });
      yield* Queue.offer(events, {
        kind: "snapshot",
        snapshot: LIVE_SHELL_SNAPSHOT,
      });
      const synchronizing = yield* SubscriptionRef.changes(shellState).pipe(
        Stream.filter((state) => state.status === "synchronizing" && Option.isSome(state.snapshot)),
        Stream.runHead,
      );
      expect(Option.getOrThrow(Option.getOrThrow(synchronizing).snapshot)).toEqual(
        LIVE_SHELL_SNAPSHOT,
      );

      yield* Queue.offer(events, { kind: "synchronized" });
      yield* SubscriptionRef.changes(shellState).pipe(
        Stream.filter((state) => state.status === "live"),
        Stream.runHead,
      );

      yield* SubscriptionRef.set(supervisorState, {
        desired: true,
        network: "online",
        phase: "connected",
        stage: null,
        attempt: 1,
        generation: 1,
        lastFailure: null,
        retryAt: null,
      });
      for (let index = 0; index < 10; index += 1) {
        yield* Effect.yieldNow;
      }

      const state = yield* SubscriptionRef.get(shellState);
      expect(state.status).toBe("live");
      expect(Option.getOrThrow(state.snapshot)).toEqual(LIVE_SHELL_SNAPSHOT);
    }),
  );

  it.live.each([
    { bufferSize: Infinity, expectedSequences: [51] },
    // RpcClient defaults to a 16-event buffer, which splits larger server chunks.
    { bufferSize: 16, expectedSequences: [17, 33, 49, 51] },
  ])("batches live events with a $bufferSize event buffer", ({ bufferSize, expectedSequences }) =>
    Effect.gen(function* () {
      const events = yield* Queue.bounded<OrchestrationV2ShellStreamItem>(bufferSize);
      const batchSnapshot = { ...LIVE_SHELL_SNAPSHOT, threads: [] };
      const client = {
        [ORCHESTRATION_V2_WS_METHODS.subscribeShell]: () => Stream.fromQueue(events),
      } as unknown as WsRpcProtocolClient;
      const supervisorState = yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE);
      const activeSession = yield* SubscriptionRef.make<Option.Option<RpcSession.RpcSession>>(
        Option.some(session(client)),
      );
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: TARGET,
        state: supervisorState,
        session: activeSession,
        prepared: yield* SubscriptionRef.make(Option.some(PREPARED)),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
      const cache = Persistence.EnvironmentCacheStore.of({
        loadShell: () => Effect.succeed(Option.none()),
        saveShell: () => Effect.void,
        loadThread: () => Effect.succeed(Option.none()),
        saveThread: () => Effect.void,
        removeThread: () => Effect.void,
        loadServerConfig: () => Effect.succeed(Option.none()),
        saveServerConfig: () => Effect.void,
        loadVcsRefs: () => Effect.succeed(Option.none()),
        saveVcsRefs: () => Effect.void,
        removeVcsRefs: () => Effect.void,
        clearVcsRefs: () => Effect.void,
        clear: () => Effect.void,
      });
      const shellState = yield* makeEnvironmentShellState().pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.provideService(Persistence.EnvironmentCacheStore, cache),
        Effect.provideService(
          ShellSnapshotLoader.ShellSnapshotLoader,
          ShellSnapshotLoader.ShellSnapshotLoader.of({ load: () => Effect.succeed(Option.none()) }),
        ),
      );
      yield* SubscriptionRef.set(supervisorState, {
        desired: true,
        network: "online",
        phase: "connected",
        stage: null,
        attempt: 1,
        generation: 1,
        lastFailure: null,
        retryAt: null,
      });
      yield* Queue.offer(events, { kind: "snapshot", snapshot: batchSnapshot });
      yield* Queue.offer(events, { kind: "synchronized" });
      yield* SubscriptionRef.changes(shellState).pipe(
        Stream.filter((state) => state.status === "live"),
        Stream.runHead,
      );

      // Observe before publishing so no batch can arrive before the subscription.
      const observed = yield* SubscriptionRef.changes(shellState).pipe(
        Stream.drop(1),
        Stream.takeUntil(
          (state) => Option.isSome(state.snapshot) && state.snapshot.value.threads.length === 50,
        ),
        Stream.runCollect,
        Effect.forkScoped({ startImmediately: true }),
      );
      yield* Queue.offerAll(
        events,
        Array.from({ length: 50 }, (_, index) => ({
          kind: "thread.updated" as const,
          sequence: 2 + index,
          location: "active" as const,
          thread: { ...v2ShellSnapshot.threads[0]!, id: `thread-${index}` } as never,
        })),
      );
      const states = yield* Fiber.join(observed);
      const snapshots = states.map((state) => Option.getOrThrow(state.snapshot));
      expect(snapshots.map((snapshot) => snapshot.snapshotSequence)).toEqual(expectedSequences);
      expect(snapshots.at(-1)!.threads.map((thread) => thread.id)).toEqual(
        Array.from({ length: 50 }, (_, index) => `thread-${index}`),
      );
    }).pipe(Effect.scoped),
  );

  it.effect("refreshes a disk-restored shell from HTTP before resuming", () =>
    Effect.gen(function* () {
      const cachedSnapshot: OrchestrationV2ShellSnapshot = {
        ...v2ShellSnapshot,
        snapshotSequence: 5,
      };
      const events = yield* Queue.unbounded<OrchestrationV2ShellStreamItem>();
      const capturedAfterSequence = yield* SubscriptionRef.make<number | undefined>(undefined);
      const capturedCompletionMarker = yield* Ref.make(false);
      const loaderCalls = yield* SubscriptionRef.make(0);
      const httpSnapshot: OrchestrationV2ShellSnapshot = {
        ...v2ShellSnapshot,
        snapshotSequence: 9,
      };
      const client = {
        [ORCHESTRATION_V2_WS_METHODS.subscribeShell]: (input: {
          readonly afterSequence?: number;
          readonly requestCompletionMarker?: true;
        }) =>
          Stream.unwrap(
            Effect.all([
              Ref.set(capturedCompletionMarker, input.requestCompletionMarker === true),
              SubscriptionRef.set(capturedAfterSequence, input.afterSequence),
            ]).pipe(Effect.as(Stream.fromQueue(events))),
          ),
      } as unknown as WsRpcProtocolClient;
      const supervisorState = yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE);
      const activeSession = yield* SubscriptionRef.make<Option.Option<RpcSession.RpcSession>>(
        Option.some(session(client)),
      );
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: TARGET,
        state: supervisorState,
        session: activeSession,
        prepared: yield* SubscriptionRef.make(Option.some(PREPARED)),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
      const cache = Persistence.EnvironmentCacheStore.of({
        loadShell: () => Effect.succeedSome(cachedSnapshot),
        saveShell: () => Effect.void,
        loadThread: () => Effect.succeedNone,
        saveThread: () => Effect.void,
        removeThread: () => Effect.void,
        loadServerConfig: () => Effect.succeedNone,
        saveServerConfig: () => Effect.void,
        loadVcsRefs: () => Effect.succeedNone,
        saveVcsRefs: () => Effect.void,
        removeVcsRefs: () => Effect.void,
        clearVcsRefs: () => Effect.void,
        clear: () => Effect.void,
      });
      const snapshotLoader = ShellSnapshotLoader.ShellSnapshotLoader.of({
        load: () =>
          SubscriptionRef.update(loaderCalls, (count) => count + 1).pipe(
            Effect.as(Option.some(httpSnapshot)),
          ),
      });
      const shellState = yield* makeEnvironmentShellState().pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.provideService(Persistence.EnvironmentCacheStore, cache),
        Effect.provideService(ShellSnapshotLoader.ShellSnapshotLoader, snapshotLoader),
      );

      // Wait until the subscription is established from the warm cache.
      yield* SubscriptionRef.changes(capturedAfterSequence).pipe(
        Stream.filter((value) => value !== undefined),
        Stream.runHead,
      );

      expect(yield* SubscriptionRef.get(capturedAfterSequence)).toBe(9);
      expect(yield* Ref.get(capturedCompletionMarker)).toBe(true);
      expect(yield* SubscriptionRef.get(loaderCalls)).toBe(1);
      const synchronizing = yield* SubscriptionRef.get(shellState);
      expect(synchronizing.status).toBe("synchronizing");
      expect(Option.getOrThrow(synchronizing.snapshot)).toEqual(httpSnapshot);

      yield* Queue.offer(events, { kind: "synchronized" });
      yield* SubscriptionRef.changes(shellState).pipe(
        Stream.filter((value) => value.status === "live"),
        Stream.runHead,
      );
    }),
  );

  it.effect(
    "resumes synchronized memory across foreground and replacement sessions without HTTP",
    () =>
      Effect.gen(function* () {
        const harness = yield* makeWarmReconnectHarness();
        const first = yield* Queue.take(harness.requests);
        expect(first.input).toEqual({ afterSequence: 1, requestCompletionMarker: true });
        expect(yield* harness.shouldPrefetch(TARGET.environmentId)).toBe(true);
        yield* harness.complete(first);
        expect(yield* harness.shouldPrefetch(TARGET.environmentId)).toBe(false);

        yield* harness.resubscribe();
        const foreground = yield* Queue.take(harness.requests);
        expect(foreground.input).toEqual(first.input);
        yield* harness.complete(foreground);
        yield* harness.disconnect(foreground);
        expect(yield* harness.shouldPrefetch(TARGET.environmentId)).toBe(false);
        yield* harness.reconnect();
        const replacement = yield* Queue.take(harness.requests);
        expect(replacement.input).toEqual(first.input);
        expect(yield* harness.shouldPrefetch(TARGET.environmentId)).toBe(true);
        const live = yield* harness.complete(replacement);
        expect(Option.getOrThrow(live.snapshot)).toEqual(LIVE_SHELL_SNAPSHOT);
        expect(yield* Ref.get(harness.loaderCalls)).toBe(1);
        expect(yield* Ref.get(harness.cancelledPrefetches)).toBe(2);
        // One startup read per socket, with no extra read for foregrounding.
        expect(yield* Queue.take(harness.lifecycleRequests)).toBe(SERVER_STARTED_AT);
        expect(yield* Queue.take(harness.lifecycleRequests)).toBe(SERVER_STARTED_AT);
        expect(yield* Queue.size(harness.lifecycleRequests)).toBe(0);

        yield* Scope.close(harness.scope, Exit.void);
        expect(yield* harness.shouldPrefetch(TARGET.environmentId)).toBe(true);
      }),
  );

  it.effect(
    "replays another client's creates, archives, deletes and renames after disconnect",
    () =>
      Effect.gen(function* () {
        const renamed = { ...v2ThreadShell, title: "Renamed remotely" };
        const archived = { ...v2ThreadShell, id: ThreadId.make("archived") };
        const deleted = { ...v2ThreadShell, id: ThreadId.make("deleted") };
        const created = {
          ...v2ThreadShell,
          id: ThreadId.make("created"),
          title: "Created remotely",
        };
        const base = { ...LIVE_SHELL_SNAPSHOT, threads: [v2ThreadShell, archived, deleted] };
        const harness = yield* makeWarmReconnectHarness({ load: () => Effect.succeedSome(base) });
        const first = yield* Queue.take(harness.requests);
        yield* harness.complete(first);
        yield* harness.disconnect(first);
        yield* harness.reconnect();
        const resumed = yield* Queue.take(harness.requests);
        expect(resumed.input.afterSequence).toBe(1);
        yield* Queue.offerAll(resumed.events, [
          { kind: "thread.updated", sequence: 2, location: "active", thread: created },
          { kind: "thread.removed", sequence: 3, location: "active", threadId: archived.id },
          { kind: "thread.removed", sequence: 4, location: "active", threadId: deleted.id },
          { kind: "thread.updated", sequence: 5, location: "active", thread: renamed },
        ]);
        const live = yield* harness.complete(resumed);
        expect(Option.getOrThrow(live.snapshot)).toEqual({
          ...base,
          snapshotSequence: 5,
          threads: [renamed, created],
        });
        expect(yield* Ref.get(harness.loaderCalls)).toBe(1);
        // The cursor applied by replay, rather than the old HTTP cursor, is reused next.
        yield* harness.disconnect(resumed);
        yield* harness.reconnect();
        expect((yield* Queue.take(harness.requests)).input.afterSequence).toBe(5);
      }),
  );

  it.effect.each([
    { reason: "cursor ahead of head", sequence: 0 },
    { reason: "gap beyond the 1,000-event replay limit", sequence: 1_002 },
    { reason: "raw replay payload beyond budget", sequence: 2 },
  ])(
    "replaces the warm base with the server's authoritative snapshot for $reason",
    ({ sequence }) =>
      Effect.gen(function* () {
        const harness = yield* makeWarmReconnectHarness();
        const first = yield* Queue.take(harness.requests);
        yield* harness.complete(first);
        yield* harness.disconnect(first);
        yield* harness.reconnect();
        const resumed = yield* Queue.take(harness.requests);
        expect(resumed.input.afterSequence).toBe(1);
        // ws.ts rejects these replay ranges with an unmarked full snapshot,
        // optionally enrichment, then the completion marker. It is not an HTTP retry.
        const fallback = {
          ...LIVE_SHELL_SNAPSHOT,
          snapshotSequence: sequence,
          threads: [{ ...v2ThreadShell, id: ThreadId.make("replacement"), title: "Server base" }],
        };
        yield* Queue.offer(resumed.events, { kind: "snapshot", snapshot: fallback });
        const synchronizing = yield* harness.waitForState(
          (value) =>
            value.status === "synchronizing" &&
            Option.isSome(value.snapshot) &&
            value.snapshot.value.snapshotSequence === sequence &&
            value.snapshot.value.threads[0]?.id === fallback.threads[0]!.id,
        );
        expect(Option.getOrThrow(synchronizing.snapshot)).toEqual(fallback);
        expect(yield* harness.shouldPrefetch(TARGET.environmentId)).toBe(true);
        yield* harness.complete(resumed);
        yield* harness.disconnect(resumed);
        yield* harness.reconnect();
        expect((yield* Queue.take(harness.requests)).input.afterSequence).toBe(sequence);
        expect(yield* Ref.get(harness.loaderCalls)).toBe(1);
      }),
  );

  it.effect.each([0, 1, 50])("refreshes HTTP after server restart with head %s", (sequence) =>
    Effect.gen(function* () {
      const fresh = { ...LIVE_SHELL_SNAPSHOT, snapshotSequence: sequence, threads: [] };
      const harness = yield* makeWarmReconnectHarness({
        load: (call) => Effect.succeedSome(call === 1 ? LIVE_SHELL_SNAPSHOT : fresh),
      });
      const first = yield* Queue.take(harness.requests);
      yield* harness.complete(first);
      yield* harness.disconnect(first);
      yield* harness.reconnect(INITIAL_CONFIG, "2026-06-20T01:00:00.000Z");
      const refreshed = yield* Queue.take(harness.requests);
      expect(refreshed.input.afterSequence).toBe(sequence);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(2);
      expect(Option.getOrThrow((yield* harness.complete(refreshed)).snapshot)).toEqual(fresh);
    }),
  );

  it.effect("refreshes when lifecycle proof is missing, including on a later session", () =>
    Effect.gen(function* () {
      const harness = yield* makeWarmReconnectHarness({ lifecycle: Stream.never });
      yield* Queue.take(harness.lifecycleRequests);
      yield* TestClock.adjust("3 seconds");
      const first = yield* Queue.take(harness.requests);
      yield* harness.complete(first);
      expect(yield* harness.shouldPrefetch(TARGET.environmentId)).toBe(true);
      yield* harness.disconnect(first);
      yield* harness.reconnect();
      yield* Queue.take(harness.lifecycleRequests);
      yield* TestClock.adjust("3 seconds");
      const refreshed = yield* Queue.take(harness.requests);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(2);
      yield* harness.complete(refreshed);
    }),
  );

  it.effect.each([
    {
      reason: "identity",
      environment: { ...INITIAL_CONFIG.environment, environmentId: EnvironmentId.make("changed") },
    },
    { reason: "version", environment: { ...INITIAL_CONFIG.environment, serverVersion: "2.0.0" } },
    {
      reason: "protocol",
      environment: { ...INITIAL_CONFIG.environment, orchestrationProtocolVersion: 3 },
    },
  ])("refreshes HTTP after the server $reason changes", ({ environment }) =>
    Effect.gen(function* () {
      const fresh = { ...LIVE_SHELL_SNAPSHOT, snapshotSequence: 0, threads: [] };
      const harness = yield* makeWarmReconnectHarness({
        load: (call) => Effect.succeedSome(call === 1 ? LIVE_SHELL_SNAPSHOT : fresh),
      });
      const first = yield* Queue.take(harness.requests);
      yield* harness.complete(first);
      yield* harness.disconnect(first);
      yield* harness.reconnect({ ...INITIAL_CONFIG, environment });
      const refreshed = yield* Queue.take(harness.requests);
      expect(refreshed.input.afterSequence).toBe(0);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(2);
      const live = yield* harness.complete(refreshed);
      expect(Option.getOrThrow(live.snapshot)).toEqual(fresh);
    }),
  );

  it.effect.each(["initial load", "replay"] as const)(
    "refreshes HTTP after disconnect during $0",
    (stage) =>
      Effect.gen(function* () {
        const fresh = { ...LIVE_SHELL_SNAPSHOT, snapshotSequence: 10, threads: [] };
        const harness = yield* makeWarmReconnectHarness({
          load: (call) => Effect.succeedSome(call === 1 ? LIVE_SHELL_SNAPSHOT : fresh),
        });
        let request = yield* Queue.take(harness.requests);
        if (stage === "replay") {
          yield* harness.complete(request);
          yield* harness.disconnect(request);
          yield* harness.reconnect();
          request = yield* Queue.take(harness.requests);
          yield* Queue.offer(request.events, {
            kind: "thread.updated",
            sequence: 2,
            location: "active",
            thread: { ...v2ThreadShell, title: "Partial replay" },
          });
          yield* harness.waitForState(
            (value) => Option.isSome(value.snapshot) && value.snapshot.value.snapshotSequence === 2,
          );
        }
        yield* harness.disconnect(request);
        expect(yield* harness.shouldPrefetch(TARGET.environmentId)).toBe(true);
        yield* harness.reconnect();
        const refreshed = yield* Queue.take(harness.requests);
        expect(refreshed.input.afterSequence).toBe(10);
        expect(yield* Ref.get(harness.loaderCalls)).toBe(2);
        expect(Option.getOrThrow((yield* harness.complete(refreshed)).snapshot)).toEqual(fresh);
      }),
  );

  it.effect("does not reuse a partially loaded HTTP snapshot after its load is interrupted", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const interrupted = yield* Deferred.make<void>();
      const harness = yield* makeWarmReconnectHarness({
        cached: LIVE_SHELL_SNAPSHOT,
        load: (call) =>
          call === 1
            ? Deferred.succeed(started, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.ensuring(Deferred.succeed(interrupted, undefined)),
              )
            : Effect.succeedSome({ ...LIVE_SHELL_SNAPSHOT, snapshotSequence: 10 }),
      });
      yield* Deferred.await(started);
      // Replacing the session cancels its initial HTTP load before the next starts.
      yield* harness.reconnect();
      yield* Deferred.await(interrupted);
      const refreshed = yield* Queue.take(harness.requests);
      expect(refreshed.input.afterSequence).toBe(10);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(2);
      yield* harness.complete(refreshed);
    }),
  );

  it.effect("does not certify an incomplete disk base with enrichment and a plausible cursor", () =>
    Effect.gen(function* () {
      const incomplete = { ...LIVE_SHELL_SNAPSHOT, threads: [] };
      const harness = yield* makeWarmReconnectHarness({
        cached: incomplete,
        load: (call) => (call === 1 ? Effect.succeedNone : Effect.succeedSome(LIVE_SHELL_SNAPSHOT)),
      });
      const first = yield* Queue.take(harness.requests);
      expect(first.input).toEqual({ requestCompletionMarker: true });
      yield* Queue.offer(first.events, {
        kind: "snapshot",
        snapshot: { ...incomplete, snapshotSequence: 99 },
        resolvedRepositoryIdentityRoots: [v2Project.workspaceRoot],
      });
      yield* harness.complete(first);
      expect(yield* harness.shouldPrefetch(TARGET.environmentId)).toBe(true);
      yield* harness.disconnect(first);
      yield* harness.reconnect();
      const refreshed = yield* Queue.take(harness.requests);
      expect(refreshed.input.afterSequence).toBe(1);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(2);
      expect(Option.getOrThrow((yield* harness.complete(refreshed)).snapshot).threads).toEqual([
        v2ThreadShell,
      ]);
    }),
  );

  it.effect("can certify a full socket fallback but not metadata-only enrichment", () =>
    Effect.gen(function* () {
      const harness = yield* makeWarmReconnectHarness({ load: () => Effect.succeedNone });
      const first = yield* Queue.take(harness.requests);
      expect(first.input.afterSequence).toBeUndefined();
      yield* Queue.offer(first.events, { kind: "snapshot", snapshot: LIVE_SHELL_SNAPSHOT });
      yield* harness.complete(first);
      yield* harness.disconnect(first);
      yield* harness.reconnect();
      const resumed = yield* Queue.take(harness.requests);
      expect(resumed.input.afterSequence).toBe(1);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(1);
      yield* harness.complete(resumed);
    }),
  );

  it.effect.each([false, true])(
    "preserves a completed base across socket failure but refreshes an unfinished replay: $0",
    (unfinishedReplay) =>
      Effect.gen(function* () {
        const harness = yield* makeWarmReconnectHarness();
        let request = yield* Queue.take(harness.requests);
        yield* harness.complete(request);
        if (unfinishedReplay) {
          yield* harness.disconnect(request);
          yield* harness.reconnect();
          request = yield* Queue.take(harness.requests);
        }
        yield* Deferred.fail(
          request.failure,
          new RpcClientError.RpcClientError({
            reason: new Socket.SocketCloseError({ code: 1006 }),
          }),
        );
        yield* Deferred.await(request.stopped);
        yield* harness.reconnect();
        const resumed = yield* Queue.take(harness.requests);
        expect(resumed.input.afterSequence).toBe(1);
        expect(yield* Ref.get(harness.loaderCalls)).toBe(unfinishedReplay ? 2 : 1);
        yield* harness.complete(resumed);
      }),
  );

  it.effect("refreshes a completed base after a protocol defect", () =>
    Effect.gen(function* () {
      const harness = yield* makeWarmReconnectHarness();
      const first = yield* Queue.take(harness.requests);
      yield* harness.complete(first);
      yield* Deferred.fail(
        first.failure,
        new RpcClientError.RpcClientError({
          reason: new RpcClientError.RpcClientDefect({ message: "Malformed frame", cause: null }),
        }),
      );
      yield* Deferred.await(first.stopped);
      yield* harness.reconnect();
      const resumed = yield* Queue.take(harness.requests);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(2);
      yield* harness.complete(resumed);
    }),
  );

  it.effect("refreshes legacy servers on foreground resubscriptions and replacement sessions", () =>
    Effect.gen(function* () {
      const legacy = { ...INITIAL_CONFIG, shellResumeCompletionMarker: false };
      const harness = yield* makeWarmReconnectHarness({ config: legacy });
      const first = yield* Queue.take(harness.requests);
      expect(first.input).toEqual({ afterSequence: 1 });
      yield* harness.resubscribe();
      const foreground = yield* Queue.take(harness.requests);
      expect(foreground.input).toEqual(first.input);
      yield* harness.disconnect(foreground);
      yield* harness.reconnect(legacy);
      const replacement = yield* Queue.take(harness.requests);
      expect(replacement.input).toEqual(first.input);
      expect(yield* Ref.get(harness.loaderCalls)).toBe(3);
      expect(yield* harness.shouldPrefetch(TARGET.environmentId)).toBe(true);
    }),
  );

  it.effect("throttles shell writes and flushes the latest snapshot on disconnect", () =>
    Effect.gen(function* () {
      const events = yield* Queue.unbounded<OrchestrationV2ShellStreamItem>();
      const saved = yield* Ref.make<ReadonlyArray<OrchestrationV2ShellSnapshot>>([]);
      const client = {
        [ORCHESTRATION_V2_WS_METHODS.subscribeShell]: () => Stream.fromQueue(events),
      } as unknown as WsRpcProtocolClient;
      const supervisorState = yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE);
      const activeSession = yield* SubscriptionRef.make<Option.Option<RpcSession.RpcSession>>(
        Option.some(session(client)),
      );
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: TARGET,
        state: supervisorState,
        session: activeSession,
        prepared: yield* SubscriptionRef.make(Option.some(PREPARED)),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
      const cache = Persistence.EnvironmentCacheStore.of({
        loadShell: () => Effect.succeed(Option.none()),
        saveShell: (_environmentId, snapshot) =>
          Ref.update(saved, (values) => [...values, snapshot]),
        loadThread: () => Effect.succeed(Option.none()),
        saveThread: () => Effect.void,
        removeThread: () => Effect.void,
        loadServerConfig: () => Effect.succeed(Option.none()),
        saveServerConfig: () => Effect.void,
        loadVcsRefs: () => Effect.succeed(Option.none()),
        saveVcsRefs: () => Effect.void,
        removeVcsRefs: () => Effect.void,
        clearVcsRefs: () => Effect.void,
        clear: () => Effect.void,
      });
      const snapshotLoader = ShellSnapshotLoader.ShellSnapshotLoader.of({
        load: () => Effect.succeed(Option.none()),
      });

      const shellState = yield* makeEnvironmentShellState().pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.provideService(Persistence.EnvironmentCacheStore, cache),
        Effect.provideService(ShellSnapshotLoader.ShellSnapshotLoader, snapshotLoader),
      );

      const sendSnapshot = Effect.fn(function* (sequence: number) {
        yield* Queue.offer(events, {
          kind: "snapshot",
          snapshot: { ...LIVE_SHELL_SNAPSHOT, snapshotSequence: sequence },
        });
        yield* SubscriptionRef.changes(shellState).pipe(
          Stream.filter(
            (state) =>
              Option.isSome(state.snapshot) && state.snapshot.value.snapshotSequence === sequence,
          ),
          Stream.runHead,
        );
      });
      yield* sendSnapshot(1);
      yield* TestClock.adjust("500 millis");
      for (let sequence = 2; sequence <= 10; sequence++) {
        yield* sendSnapshot(sequence);
        yield* TestClock.adjust("1 second");
      }
      expect((yield* Ref.get(saved)).map((snapshot) => snapshot.snapshotSequence)).toEqual([1]);
      yield* TestClock.adjust("1 second");
      expect((yield* Ref.get(saved)).map((snapshot) => snapshot.snapshotSequence)).toEqual([1, 10]);
      yield* sendSnapshot(11);
      yield* SubscriptionRef.set(supervisorState, {
        ...AVAILABLE_CONNECTION_STATE,
        desired: false,
      });
      yield* SubscriptionRef.changes(shellState).pipe(
        Stream.filter((state) => state.status === "cached"),
        Stream.runHead,
      );
      // The disconnect flush runs independently from the connection-state update.
      yield* TestClock.adjust("0 millis");
      expect((yield* Ref.get(saved)).map((snapshot) => snapshot.snapshotSequence)).toEqual([
        1, 10, 11,
      ]);
    }),
  );

  it.effect("flushes the latest live snapshot without blocking connection state updates", () =>
    Effect.gen(function* () {
      const events = yield* Queue.unbounded<OrchestrationV2ShellStreamItem>();
      const saved = yield* Ref.make<ReadonlyArray<OrchestrationV2ShellSnapshot>>([]);
      const saveStarted = yield* Deferred.make<void>();
      const releaseSave = yield* Deferred.make<void>();
      const client = {
        [ORCHESTRATION_V2_WS_METHODS.subscribeShell]: () => Stream.fromQueue(events),
      } as unknown as WsRpcProtocolClient;
      const supervisorState = yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE);
      const activeSession = yield* SubscriptionRef.make<Option.Option<RpcSession.RpcSession>>(
        Option.some(session(client)),
      );
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: TARGET,
        state: supervisorState,
        session: activeSession,
        prepared: yield* SubscriptionRef.make(Option.some(PREPARED)),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
      const cache = Persistence.EnvironmentCacheStore.of({
        loadShell: () => Effect.succeed(Option.none()),
        saveShell: (_environmentId, snapshot) =>
          Deferred.succeed(saveStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseSave)),
            Effect.andThen(Ref.update(saved, (values) => [...values, snapshot])),
          ),
        loadThread: () => Effect.succeed(Option.none()),
        saveThread: () => Effect.void,
        removeThread: () => Effect.void,
        loadServerConfig: () => Effect.succeed(Option.none()),
        saveServerConfig: () => Effect.void,
        loadVcsRefs: () => Effect.succeed(Option.none()),
        saveVcsRefs: () => Effect.void,
        removeVcsRefs: () => Effect.void,
        clearVcsRefs: () => Effect.void,
        clear: () => Effect.void,
      });
      const snapshotLoader = ShellSnapshotLoader.ShellSnapshotLoader.of({
        load: () => Effect.succeed(Option.none()),
      });

      const shellState = yield* makeEnvironmentShellState().pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.provideService(Persistence.EnvironmentCacheStore, cache),
        Effect.provideService(ShellSnapshotLoader.ShellSnapshotLoader, snapshotLoader),
      );

      yield* SubscriptionRef.set(supervisorState, {
        desired: true,
        network: "online",
        phase: "connecting",
        stage: "synchronizing",
        attempt: 1,
        generation: 0,
        lastFailure: null,
        retryAt: null,
      });
      yield* Queue.offer(events, {
        kind: "snapshot",
        snapshot: LIVE_SHELL_SNAPSHOT,
      });
      yield* Queue.offer(events, { kind: "synchronized" });
      yield* SubscriptionRef.changes(shellState).pipe(
        Stream.filter((state) => state.status === "live"),
        Stream.runHead,
      );

      yield* SubscriptionRef.set(supervisorState, {
        desired: true,
        network: "online",
        phase: "connecting",
        stage: "synchronizing",
        attempt: 2,
        generation: 1,
        lastFailure: null,
        retryAt: null,
      });
      yield* SubscriptionRef.set(supervisorState, {
        desired: false,
        network: "online",
        phase: "available",
        stage: null,
        attempt: 2,
        generation: 2,
        lastFailure: null,
        retryAt: null,
      });
      yield* Deferred.await(saveStarted);

      yield* SubscriptionRef.set(supervisorState, {
        desired: true,
        network: "online",
        phase: "connecting",
        stage: "synchronizing",
        attempt: 3,
        generation: 3,
        lastFailure: null,
        retryAt: null,
      });
      for (let index = 0; index < 100; index += 1) {
        if ((yield* SubscriptionRef.get(shellState)).status === "synchronizing") break;
        yield* Effect.yieldNow;
      }

      expect((yield* SubscriptionRef.get(shellState)).status).toBe("synchronizing");
      expect(yield* Ref.get(saved)).toEqual([]);

      yield* Deferred.succeed(releaseSave, undefined);
      for (let index = 0; index < 100; index += 1) {
        if ((yield* Ref.get(saved)).length > 0) break;
        yield* Effect.yieldNow;
      }

      expect(yield* Ref.get(saved)).toEqual([LIVE_SHELL_SNAPSHOT]);
    }),
  );

  it.effect("re-persists same-sequence enrichment when an older in-flight save completes", () =>
    Effect.gen(function* () {
      const repositoryIdentity = {
        canonicalKey: "github.com/example/repo",
        locator: {
          source: "git-remote" as const,
          remoteName: "origin",
          remoteUrl: "https://github.com/example/repo.git",
        },
      };
      const unenrichedSnapshot: OrchestrationV2ShellSnapshot = {
        ...LIVE_SHELL_SNAPSHOT,
        snapshotSequence: 1,
        projects: [{ ...v2Project, repositoryIdentity: null }],
      };
      const enrichedSnapshot: OrchestrationV2ShellSnapshot = {
        ...LIVE_SHELL_SNAPSHOT,
        snapshotSequence: 1,
        projects: [{ ...v2Project, repositoryIdentity }],
      };

      const events = yield* Queue.unbounded<OrchestrationV2ShellStreamItem>();
      const saved = yield* Ref.make<ReadonlyArray<OrchestrationV2ShellSnapshot>>([]);
      const unenrichedSaveStarted = yield* Deferred.make<void>();
      const releaseUnenrichedSave = yield* Deferred.make<void>();
      const client = {
        [ORCHESTRATION_V2_WS_METHODS.subscribeShell]: () => Stream.fromQueue(events),
      } as unknown as WsRpcProtocolClient;
      const supervisorState = yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE);
      const activeSession = yield* SubscriptionRef.make<Option.Option<RpcSession.RpcSession>>(
        Option.some(session(client)),
      );
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: TARGET,
        state: supervisorState,
        session: activeSession,
        prepared: yield* SubscriptionRef.make(Option.some(PREPARED)),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
      const cache = Persistence.EnvironmentCacheStore.of({
        loadShell: () => Effect.succeed(Option.none()),
        saveShell: (_environmentId, snapshot) =>
          Effect.gen(function* () {
            if (snapshot.projects[0]?.repositoryIdentity == null) {
              yield* Deferred.succeed(unenrichedSaveStarted, undefined);
              yield* Deferred.await(releaseUnenrichedSave);
            }
            yield* Ref.update(saved, (values) => [...values, snapshot]);
          }),
        loadThread: () => Effect.succeed(Option.none()),
        saveThread: () => Effect.void,
        removeThread: () => Effect.void,
        loadServerConfig: () => Effect.succeed(Option.none()),
        saveServerConfig: () => Effect.void,
        loadVcsRefs: () => Effect.succeed(Option.none()),
        saveVcsRefs: () => Effect.void,
        removeVcsRefs: () => Effect.void,
        clearVcsRefs: () => Effect.void,
        clear: () => Effect.void,
      });
      const snapshotLoader = ShellSnapshotLoader.ShellSnapshotLoader.of({
        load: () => Effect.succeed(Option.none()),
      });

      const shellState = yield* makeEnvironmentShellState().pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.provideService(Persistence.EnvironmentCacheStore, cache),
        Effect.provideService(ShellSnapshotLoader.ShellSnapshotLoader, snapshotLoader),
      );

      yield* SubscriptionRef.set(supervisorState, {
        desired: true,
        network: "online",
        phase: "connecting",
        stage: "synchronizing",
        attempt: 1,
        generation: 0,
        lastFailure: null,
        retryAt: null,
      });
      yield* Queue.offer(events, {
        kind: "snapshot",
        snapshot: unenrichedSnapshot,
      });
      yield* Queue.offer(events, { kind: "synchronized" });
      yield* SubscriptionRef.changes(shellState).pipe(
        Stream.filter((state) => state.status === "live"),
        Stream.runHead,
      );

      // Disconnect flush starts an immediate persist of the unenriched snapshot
      // (debounced path needs TestClock advancement; flush does not).
      yield* SubscriptionRef.set(supervisorState, {
        desired: false,
        network: "online",
        phase: "available",
        stage: null,
        attempt: 1,
        generation: 1,
        lastFailure: null,
        retryAt: null,
      });
      yield* Deferred.await(unenrichedSaveStarted);

      // Same sequence, different content. Session stays open so the stream can
      // still apply enrichment while the older save is in flight.
      yield* Queue.offer(events, {
        kind: "snapshot",
        snapshot: enrichedSnapshot,
      });
      yield* SubscriptionRef.changes(shellState).pipe(
        Stream.filter(
          (state) =>
            Option.isSome(state.snapshot) &&
            state.snapshot.value.projects[0]?.repositoryIdentity != null,
        ),
        Stream.runHead,
      );

      // Completing the older same-sequence save must re-persist the latest object.
      yield* Deferred.succeed(releaseUnenrichedSave, undefined);
      for (let index = 0; index < 100; index += 1) {
        const values = yield* Ref.get(saved);
        if (values.length > 0 && values.at(-1)?.projects[0]?.repositoryIdentity != null) {
          break;
        }
        yield* Effect.yieldNow;
      }

      const finalSaved = (yield* Ref.get(saved)).at(-1);
      expect(finalSaved?.snapshotSequence).toBe(1);
      expect(finalSaved?.projects[0]?.repositoryIdentity).toEqual(repositoryIdentity);
    }),
  );

  it.effect("scope teardown flush stays stable while a save is blocked", () =>
    Effect.gen(function* () {
      const lateSnapshot: OrchestrationV2ShellSnapshot = {
        ...LIVE_SHELL_SNAPSHOT,
        snapshotSequence: 2,
        projects: [{ ...v2Project, title: "Late stream event" }],
      };
      const events = yield* Queue.unbounded<OrchestrationV2ShellStreamItem>();
      const saved = yield* Ref.make<ReadonlyArray<OrchestrationV2ShellSnapshot>>([]);
      const saveStarted = yield* Deferred.make<void>();
      const releaseSave = yield* Deferred.make<void>();
      const client = {
        [ORCHESTRATION_V2_WS_METHODS.subscribeShell]: () => Stream.fromQueue(events),
      } as unknown as WsRpcProtocolClient;
      const supervisorState = yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE);
      const activeSession = yield* SubscriptionRef.make<Option.Option<RpcSession.RpcSession>>(
        Option.some(session(client)),
      );
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: TARGET,
        state: supervisorState,
        session: activeSession,
        prepared: yield* SubscriptionRef.make(Option.some(PREPARED)),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
      const cache = Persistence.EnvironmentCacheStore.of({
        loadShell: () => Effect.succeed(Option.none()),
        saveShell: (_environmentId, snapshot) =>
          Deferred.succeed(saveStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseSave)),
            Effect.andThen(Ref.update(saved, (values) => [...values, snapshot])),
          ),
        loadThread: () => Effect.succeed(Option.none()),
        saveThread: () => Effect.void,
        removeThread: () => Effect.void,
        loadServerConfig: () => Effect.succeed(Option.none()),
        saveServerConfig: () => Effect.void,
        loadVcsRefs: () => Effect.succeed(Option.none()),
        saveVcsRefs: () => Effect.void,
        removeVcsRefs: () => Effect.void,
        clearVcsRefs: () => Effect.void,
        clear: () => Effect.void,
      });
      const snapshotLoader = ShellSnapshotLoader.ShellSnapshotLoader.of({
        load: () => Effect.succeed(Option.none()),
      });

      const scope = yield* Scope.make();
      const shellState = yield* makeEnvironmentShellState().pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.provideService(Persistence.EnvironmentCacheStore, cache),
        Effect.provideService(ShellSnapshotLoader.ShellSnapshotLoader, snapshotLoader),
        Scope.provide(scope),
      );

      yield* SubscriptionRef.set(supervisorState, {
        desired: true,
        network: "online",
        phase: "connecting",
        stage: "synchronizing",
        attempt: 1,
        generation: 0,
        lastFailure: null,
        retryAt: null,
      });
      yield* Queue.offer(events, {
        kind: "snapshot",
        snapshot: LIVE_SHELL_SNAPSHOT,
      });
      yield* Queue.offer(events, { kind: "synchronized" });
      yield* SubscriptionRef.changes(shellState).pipe(
        Stream.filter((state) => state.status === "live"),
        Stream.runHead,
      );

      // Close scope so the finalizer flush blocks on save. Workers must be
      // interrupted before that flush; late stream events must not extend it.
      const closeFiber = yield* Effect.forkChild(Scope.close(scope, Exit.void));
      yield* Deferred.await(saveStarted);
      yield* Queue.offer(events, {
        kind: "snapshot",
        snapshot: lateSnapshot,
      });
      for (let index = 0; index < 50; index += 1) {
        yield* Effect.yieldNow;
      }
      yield* Deferred.succeed(releaseSave, undefined);
      yield* Fiber.join(closeFiber);

      const values = yield* Ref.get(saved);
      expect(values.length).toBeGreaterThan(0);
      expect(values.every((snapshot) => snapshot.snapshotSequence === 1)).toBe(true);
      expect(values.every((snapshot) => snapshot.projects[0]?.title !== "Late stream event")).toBe(
        true,
      );
    }),
  );

  it.effect("applies authoritative lower-sequence HTTP resets over client-ahead cache", () =>
    Effect.gen(function* () {
      const cachedSnapshot: OrchestrationV2ShellSnapshot = {
        ...v2ShellSnapshot,
        snapshotSequence: 20,
        projects: [{ ...v2Project, title: "Client ahead" }],
      };
      const httpSnapshot: OrchestrationV2ShellSnapshot = {
        ...v2ShellSnapshot,
        snapshotSequence: 4,
        projects: [{ ...v2Project, title: "Server reset" }],
      };
      const events = yield* Queue.unbounded<OrchestrationV2ShellStreamItem>();
      const client = {
        [ORCHESTRATION_V2_WS_METHODS.subscribeShell]: () => Stream.fromQueue(events),
      } as unknown as WsRpcProtocolClient;
      const supervisorState = yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE);
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: TARGET,
        state: supervisorState,
        session: yield* SubscriptionRef.make(Option.some(session(client))),
        prepared: yield* SubscriptionRef.make(Option.some(PREPARED)),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
      const cache = Persistence.EnvironmentCacheStore.of({
        loadShell: () => Effect.succeed(Option.some(cachedSnapshot)),
        saveShell: () => Effect.void,
        loadThread: () => Effect.succeed(Option.none()),
        saveThread: () => Effect.void,
        removeThread: () => Effect.void,
        loadServerConfig: () => Effect.succeed(Option.none()),
        saveServerConfig: () => Effect.void,
        loadVcsRefs: () => Effect.succeed(Option.none()),
        saveVcsRefs: () => Effect.void,
        removeVcsRefs: () => Effect.void,
        clearVcsRefs: () => Effect.void,
        clear: () => Effect.void,
      });
      const snapshotLoader = ShellSnapshotLoader.ShellSnapshotLoader.of({
        load: () => Effect.succeed(Option.some(httpSnapshot)),
      });
      const shellState = yield* makeEnvironmentShellState().pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.provideService(Persistence.EnvironmentCacheStore, cache),
        Effect.provideService(ShellSnapshotLoader.ShellSnapshotLoader, snapshotLoader),
      );

      yield* SubscriptionRef.changes(shellState).pipe(
        Stream.filter(
          (state) => Option.isSome(state.snapshot) && state.snapshot.value.snapshotSequence === 4,
        ),
        Stream.runHead,
      );

      const state = yield* SubscriptionRef.get(shellState);
      expect(Option.getOrThrow(state.snapshot).snapshotSequence).toBe(4);
      expect(Option.getOrThrow(state.snapshot).projects[0]?.title).toBe("Server reset");
    }),
  );

  it.effect("fills deferred cached pull request links without overwriting newer rows", () =>
    Effect.gen(function* () {
      const link = (number: number): ThreadPullRequestLink => ({
        host: "github.com",
        repository: "pingdotgg/t3code",
        number,
        url: `https://github.com/pingdotgg/t3code/pull/${number}`,
        source: "agent",
        linkedAt: "2026-06-20T00:00:00.000Z",
        snapshot: null,
        stack: null,
      });
      const otherThreadId = ThreadId.make("thread-other");
      const cachedSnapshot: OrchestrationV2ShellSnapshot = {
        ...v2ShellSnapshot,
        snapshotSequence: 5,
        threads: [v2ThreadShell, { ...v2ThreadShell, id: otherThreadId }],
      };
      const linksReady = yield* Deferred.make<void>();
      const events = yield* Queue.unbounded<OrchestrationV2ShellStreamItem>();
      const client = {
        [ORCHESTRATION_V2_WS_METHODS.subscribeShell]: () => Stream.fromQueue(events),
      } as unknown as WsRpcProtocolClient;
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: TARGET,
        state: yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE),
        session: yield* SubscriptionRef.make(Option.some(session(client))),
        prepared: yield* SubscriptionRef.make(Option.some(PREPARED)),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
      const cache = Persistence.EnvironmentCacheStore.of({
        loadShell: () =>
          Effect.succeedSome({
            ...cachedSnapshot,
            loadPullRequests: Deferred.await(linksReady).pipe(
              Effect.as(
                new Map([
                  [v2ThreadShell.id, [link(1)]],
                  [otherThreadId, [link(2)]],
                ]),
              ),
            ),
          }),
        saveShell: () => Effect.void,
        loadThread: () => Effect.succeedNone,
        saveThread: () => Effect.void,
        removeThread: () => Effect.void,
        loadServerConfig: () => Effect.succeedNone,
        saveServerConfig: () => Effect.void,
        loadVcsRefs: () => Effect.succeedNone,
        saveVcsRefs: () => Effect.void,
        removeVcsRefs: () => Effect.void,
        clearVcsRefs: () => Effect.void,
        clear: () => Effect.void,
      });
      // No HTTP snapshot, so the cached rows stay until the socket sends deltas.
      const snapshotLoader = ShellSnapshotLoader.ShellSnapshotLoader.of({
        load: () => Effect.succeedNone,
      });
      const shellState = yield* makeEnvironmentShellState().pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.provideService(Persistence.EnvironmentCacheStore, cache),
        Effect.provideService(ShellSnapshotLoader.ShellSnapshotLoader, snapshotLoader),
      );

      const threadsOf = (state: {
        readonly snapshot: Option.Option<OrchestrationV2ShellSnapshot>;
      }) => Option.getOrThrow(state.snapshot).threads;
      expect(threadsOf(yield* SubscriptionRef.get(shellState))).toEqual(cachedSnapshot.threads);

      // The server updates one row before the cached links finish decoding.
      const serverRow = { ...v2ThreadShell, title: "From server", pullRequests: [link(9)] };
      yield* Queue.offer(events, {
        kind: "thread.updated",
        sequence: 6,
        location: "active",
        thread: serverRow,
      });
      yield* SubscriptionRef.changes(shellState).pipe(
        Stream.filter((state) => threadsOf(state)[0]?.title === "From server"),
        Stream.runHead,
      );

      yield* Deferred.succeed(linksReady, undefined);
      const filled = yield* SubscriptionRef.changes(shellState).pipe(
        Stream.map(threadsOf),
        Stream.filter((threads) => threads[1]?.pullRequests !== undefined),
        Stream.runHead,
        Effect.map(Option.getOrThrow),
      );
      expect(filled[0]).toBe(serverRow);
      expect(filled[1]?.pullRequests).toEqual([link(2)]);
    }),
  );

  it.effect("shows HTTP rows before their pull request links and saves them once filled", () =>
    Effect.gen(function* () {
      const link: ThreadPullRequestLink = {
        host: "github.com",
        repository: "pingdotgg/t3code",
        number: 7,
        url: "https://github.com/pingdotgg/t3code/pull/7",
        source: "agent",
        linkedAt: "2026-06-20T00:00:00.000Z",
        snapshot: null,
        stack: null,
      };
      const httpSnapshot: OrchestrationV2ShellSnapshot = {
        ...v2ShellSnapshot,
        snapshotSequence: 3,
      };
      const linksReady = yield* Deferred.make<void>();
      const saved = yield* Queue.unbounded<OrchestrationV2ShellSnapshot>();
      const client = {
        [ORCHESTRATION_V2_WS_METHODS.subscribeShell]: () => Stream.never,
      } as unknown as WsRpcProtocolClient;
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: TARGET,
        state: yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE),
        session: yield* SubscriptionRef.make(Option.some(session(client))),
        prepared: yield* SubscriptionRef.make(Option.some(PREPARED)),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
      const cache = Persistence.EnvironmentCacheStore.of({
        loadShell: () => Effect.succeedNone,
        saveShell: (_environmentId, snapshot) => Queue.offer(saved, snapshot),
        loadThread: () => Effect.succeedNone,
        saveThread: () => Effect.void,
        removeThread: () => Effect.void,
        loadServerConfig: () => Effect.succeedNone,
        saveServerConfig: () => Effect.void,
        loadVcsRefs: () => Effect.succeedNone,
        saveVcsRefs: () => Effect.void,
        removeVcsRefs: () => Effect.void,
        clearVcsRefs: () => Effect.void,
        clear: () => Effect.void,
      });
      const snapshotLoader = ShellSnapshotLoader.ShellSnapshotLoader.of({
        load: () =>
          Effect.succeedSome({
            ...httpSnapshot,
            loadPullRequests: Deferred.await(linksReady).pipe(
              Effect.as(new Map([[v2ThreadShell.id, [link]]])),
            ),
          }),
      });
      const shellState = yield* makeEnvironmentShellState().pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.provideService(Persistence.EnvironmentCacheStore, cache),
        Effect.provideService(ShellSnapshotLoader.ShellSnapshotLoader, snapshotLoader),
      );

      const rows = yield* SubscriptionRef.changes(shellState).pipe(
        Stream.filter((state) => Option.isSome(state.snapshot)),
        Stream.runHead,
        Effect.map((state) => Option.getOrThrow(Option.getOrThrow(state).snapshot)),
      );
      expect(rows.threads[0]?.pullRequests).toBeUndefined();
      expect(rows).not.toHaveProperty("loadPullRequests");

      yield* Deferred.succeed(linksReady, undefined);
      const filled = yield* SubscriptionRef.changes(shellState).pipe(
        Stream.map((state) => Option.getOrThrow(state.snapshot).threads[0]),
        Stream.filter((thread) => thread?.pullRequests !== undefined),
        Stream.runHead,
        Effect.map(Option.getOrThrow),
      );
      expect(filled?.pullRequests).toEqual([link]);

      // The cache must end up with the links: the last save carries them.
      yield* TestClock.adjust("30 seconds");
      const saves = yield* Queue.takeAll(saved);
      expect(saves.at(-1)?.threads[0]?.pullRequests).toEqual([link]);
    }),
  );

  it.effect("does not save rows whose deferred links are still pending", () =>
    Effect.gen(function* () {
      const link: ThreadPullRequestLink = {
        host: "github.com",
        repository: "pingdotgg/t3code",
        number: 8,
        url: "https://github.com/pingdotgg/t3code/pull/8",
        source: "agent",
        linkedAt: "2026-06-20T00:00:00.000Z",
        snapshot: null,
        stack: null,
      };
      const saved = yield* Ref.make<ReadonlyArray<OrchestrationV2ShellSnapshot>>([]);
      const client = {
        [ORCHESTRATION_V2_WS_METHODS.subscribeShell]: () => Stream.never,
      } as unknown as WsRpcProtocolClient;
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: TARGET,
        state: yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE),
        session: yield* SubscriptionRef.make(Option.some(session(client))),
        prepared: yield* SubscriptionRef.make(Option.some(PREPARED)),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
      const cache = Persistence.EnvironmentCacheStore.of({
        loadShell: () => Effect.succeedNone,
        saveShell: (_environmentId, snapshot) => Ref.update(saved, (all) => [...all, snapshot]),
        loadThread: () => Effect.succeedNone,
        saveThread: () => Effect.void,
        removeThread: () => Effect.void,
        loadServerConfig: () => Effect.succeedNone,
        saveServerConfig: () => Effect.void,
        loadVcsRefs: () => Effect.succeedNone,
        saveVcsRefs: () => Effect.void,
        removeVcsRefs: () => Effect.void,
        clearVcsRefs: () => Effect.void,
        clear: () => Effect.void,
      });
      const snapshotLoader = ShellSnapshotLoader.ShellSnapshotLoader.of({
        load: () =>
          Effect.succeedSome({
            ...v2ShellSnapshot,
            snapshotSequence: 3,
            // Links never finish decoding before the shell closes.
            loadPullRequests: Effect.never.pipe(Effect.as(new Map([[v2ThreadShell.id, [link]]]))),
          }),
      });
      const scope = yield* Scope.make();
      const shellState = yield* makeEnvironmentShellState().pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.provideService(Persistence.EnvironmentCacheStore, cache),
        Effect.provideService(ShellSnapshotLoader.ShellSnapshotLoader, snapshotLoader),
        Scope.provide(scope),
      );
      yield* SubscriptionRef.changes(shellState).pipe(
        Stream.filter((state) => Option.isSome(state.snapshot)),
        Stream.runHead,
      );

      // Neither the throttled worker nor the closing flush may save the rows without links.
      yield* TestClock.adjust("30 seconds");
      yield* Scope.close(scope, Exit.void);
      expect(yield* Ref.get(saved)).toEqual([]);
    }),
  );

  it.effect("drops an older snapshot's links once a newer snapshot reuses its rows", () =>
    Effect.gen(function* () {
      const link = (number: number): ThreadPullRequestLink => ({
        host: "github.com",
        repository: "pingdotgg/t3code",
        number,
        url: `https://github.com/pingdotgg/t3code/pull/${number}`,
        source: "agent",
        linkedAt: "2026-06-20T00:00:00.000Z",
        snapshot: null,
        stack: null,
      });
      const firstLinks = yield* Deferred.make<void>();
      const secondLinks = yield* Deferred.make<void>();
      // Set once each snapshot's links start decoding, i.e. once its rows are applied.
      const firstFillStarted = yield* Deferred.make<void>();
      const loads = yield* Ref.make(0);
      const subscriptions = yield* Ref.make(0);
      const client = {
        [ORCHESTRATION_V2_WS_METHODS.subscribeShell]: () =>
          Stream.unwrap(Ref.update(subscriptions, (n) => n + 1).pipe(Effect.as(Stream.never))),
      } as unknown as WsRpcProtocolClient;
      const activeSession = yield* SubscriptionRef.make<Option.Option<RpcSession.RpcSession>>(
        Option.some(session(client)),
      );
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: TARGET,
        state: yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE),
        session: activeSession,
        prepared: yield* SubscriptionRef.make(Option.some(PREPARED)),
        connect: Effect.void,
        disconnect: Effect.void,
        retryNow: Effect.void,
      } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
      const cache = Persistence.EnvironmentCacheStore.of({
        loadShell: () => Effect.succeedNone,
        saveShell: () => Effect.void,
        loadThread: () => Effect.succeedNone,
        saveThread: () => Effect.void,
        removeThread: () => Effect.void,
        loadServerConfig: () => Effect.succeedNone,
        saveServerConfig: () => Effect.void,
        loadVcsRefs: () => Effect.succeedNone,
        saveVcsRefs: () => Effect.void,
        removeVcsRefs: () => Effect.void,
        clearVcsRefs: () => Effect.void,
        clear: () => Effect.void,
      });
      // Both snapshots carry the same row; only its pull request links differ.
      const snapshotLoader = ShellSnapshotLoader.ShellSnapshotLoader.of({
        load: () =>
          Ref.updateAndGet(loads, (n) => n + 1).pipe(
            Effect.map((n) =>
              Option.some({
                ...v2ShellSnapshot,
                snapshotSequence: n,
                loadPullRequests: (n === 1
                  ? Deferred.succeed(firstFillStarted, undefined).pipe(
                      Effect.andThen(Deferred.await(firstLinks)),
                    )
                  : Deferred.await(secondLinks)
                ).pipe(Effect.as(new Map([[v2ThreadShell.id, [link(n)]]]))),
              }),
            ),
          ),
      });
      const shellState = yield* makeEnvironmentShellState().pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
        Effect.provideService(Persistence.EnvironmentCacheStore, cache),
        Effect.provideService(ShellSnapshotLoader.ShellSnapshotLoader, snapshotLoader),
      );
      yield* Deferred.await(firstFillStarted);

      // A new session loads a second snapshot that reuses the bare row.
      yield* SubscriptionRef.set(activeSession, Option.some(session(client)));
      yield* SubscriptionRef.changes(shellState).pipe(
        Stream.filter(
          (state) => Option.isSome(state.snapshot) && state.snapshot.value.snapshotSequence === 2,
        ),
        Stream.runHead,
      );

      // The superseded first fill finishes first, then the second.
      yield* Deferred.succeed(firstLinks, undefined);
      yield* Deferred.succeed(secondLinks, undefined);
      const filled = yield* SubscriptionRef.changes(shellState).pipe(
        Stream.map((state) => Option.getOrThrow(state.snapshot).threads[0]?.pullRequests),
        Stream.filter((links) => links !== undefined),
        Stream.runHead,
        Effect.map(Option.getOrThrow),
      );
      expect(filled).toEqual([link(2)]);
    }),
  );
});
