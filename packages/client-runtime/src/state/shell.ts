import {
  ORCHESTRATION_V2_WS_METHODS,
  type EnvironmentId,
  type OrchestrationV2ShellSnapshot,
  type OrchestrationV2ShellStreamItem,
  type OrchestrationV2ThreadShell,
  type ServerConfig,
  type ThreadId,
  type ThreadPullRequestLink,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { AsyncResult, Atom } from "effect/reactivity";

import * as EnvironmentRegistry from "../connection/registry.ts";
import { connectionProjectionPhase, type PreparedConnection } from "../connection/model.ts";
import * as EnvironmentSupervisor from "../connection/supervisor.ts";
import * as ConnectionWakeups from "../connection/wakeups.ts";
import { safeErrorLogAttributes } from "../errors/safeLog.ts";
import * as Persistence from "../platform/persistence.ts";
import { runCachePersistence } from "./cachePersistence.ts";
import {
  getSessionServerStartedAt,
  isRpcClientError,
  subscribeDynamicMapped,
} from "../rpc/client.ts";
import type { RpcSession } from "../rpc/session.ts";
import * as ShellSnapshotLoader from "./shellSnapshotHttp.ts";
import type { DeferredShellSnapshot } from "./shellPullRequests.ts";
import {
  applyShellStreamEvent,
  mergeShellSnapshotProjects,
  reuseUnchangedThreadShells,
  sameThreadPullRequests,
} from "./shellReducer.ts";
import { type EnvironmentCatalogState, enabledEnvironmentIds } from "./connections.ts";
import { followStreamInEnvironment } from "./runtime.ts";

export type EnvironmentShellStatus = "empty" | "cached" | "synchronizing" | "live";

export interface EnvironmentShellState {
  readonly snapshot: Option.Option<OrchestrationV2ShellSnapshot>;
  readonly status: EnvironmentShellStatus;
  readonly error: Option.Option<string>;
}

const EMPTY_SHELL_STATE: EnvironmentShellState = {
  snapshot: Option.none(),
  status: "empty",
  error: Option.none(),
};

function shellStatusForSnapshot(
  snapshot: Option.Option<OrchestrationV2ShellSnapshot>,
): EnvironmentShellStatus {
  return Option.isSome(snapshot) ? "cached" : "empty";
}

function withoutDeferredPullRequests(value: DeferredShellSnapshot): OrchestrationV2ShellSnapshot {
  if (value.loadPullRequests === undefined) return value;
  const { loadPullRequests: _deferred, ...snapshot } = value;
  return snapshot;
}

const SHELL_SYNCHRONIZATION_ERROR_MESSAGE = "Could not synchronize environment data.";

interface ShellSubscription {
  readonly session: RpcSession;
  environment: ServerConfig["environment"] | undefined;
  serverStartedAt: string | undefined;
  supportsCompletionMarker: boolean;
  awaitingCompletion: boolean;
  authoritative: boolean;
  synchronized: boolean;
  failed: boolean;
}

export const makeEnvironmentShellState = Effect.fn("EnvironmentShellState.make")(function* () {
  const supervisor = yield* EnvironmentSupervisor.EnvironmentSupervisor;
  const cache = yield* Persistence.EnvironmentCacheStore;
  const snapshotLoader = yield* ShellSnapshotLoader.ShellSnapshotLoader;
  const wakeups = yield* Effect.serviceOption(ConnectionWakeups.ConnectionWakeups);
  const environmentId = supervisor.target.environmentId;
  const cached = yield* cache.loadShell(environmentId).pipe(
    Effect.catch((error) =>
      Effect.logWarning("Could not load cached environment shell.").pipe(
        Effect.annotateLogs({
          environmentId,
          ...safeErrorLogAttributes(error),
        }),
        Effect.as(Option.none<DeferredShellSnapshot>()),
      ),
    ),
  );
  const cachedSnapshot = Option.map(cached, withoutDeferredPullRequests);
  const state = yield* SubscriptionRef.make<EnvironmentShellState>({
    snapshot: cachedSnapshot,
    status: shellStatusForSnapshot(cachedSnapshot),
    error: Option.none(),
  });
  let activeSubscription: ShellSubscription | undefined;
  if (snapshotLoader.registerWarmBase !== undefined) {
    yield* snapshotLoader.registerWarmBase(
      environmentId,
      Effect.sync(() => activeSubscription?.synchronized === true),
    );
  }
  const latestLiveSnapshot = yield* Ref.make<Option.Option<OrchestrationV2ShellSnapshot>>(
    Option.none(),
  );
  const persistence = yield* Queue.sliding<OrchestrationV2ShellSnapshot>(1);

  const persistenceLock = yield* Semaphore.make(1);
  let lastPersisted: OrchestrationV2ShellSnapshot | undefined;
  // Rows still waiting for their deferred pull request links, which must not be saved:
  // the cache would keep them without links until the next server refresh.
  let pendingLinkRows: ReadonlySet<OrchestrationV2ThreadShell> = new Set();
  const persistLatest = Effect.fn("EnvironmentShellState.persistLatest")(function* (
    flush: boolean,
  ) {
    while (true) {
      const latest = yield* Ref.get(latestLiveSnapshot);
      if (Option.isNone(latest) || latest.value === lastPersisted) return;
      if (latest.value.threads.some((thread) => pendingLinkRows.has(thread))) return;
      const snapshot = latest.value;
      const saved = yield* cache.saveShell(environmentId, snapshot).pipe(
        Effect.as(true),
        Effect.catch((error) =>
          Effect.logWarning("Could not persist environment shell cache.").pipe(
            Effect.annotateLogs({ environmentId, ...safeErrorLogAttributes(error) }),
            Effect.as(false),
          ),
        ),
      );
      if (!saved) return;
      lastPersisted = snapshot;
      // Only lifecycle flushes chase updates that arrived during an in-flight save.
      // The regular worker leaves those updates for the next write window.
      if (!flush) return;
    }
  }, persistenceLock.withPermit);
  const persist = () => persistLatest(false);
  const flushLiveShellSnapshot = persistLatest(true);

  // Register before scoped worker fibers so reverse finalizer order interrupts
  // those fibers first and this flush sees a stable latestLiveSnapshot.
  yield* Effect.addFinalizer(() => flushLiveShellSnapshot);

  yield* runCachePersistence(persistence, persist).pipe(Effect.forkScoped);

  const setDisconnected = SubscriptionRef.update(state, (current) => ({
    ...current,
    status: shellStatusForSnapshot(current.snapshot),
  })).pipe(Effect.andThen(flushLiveShellSnapshot.pipe(Effect.forkScoped)), Effect.asVoid);
  const setSynchronizing = SubscriptionRef.update(state, (current) => ({
    ...current,
    status: "synchronizing" as const,
    error: Option.none(),
  }));
  const setReady = SubscriptionRef.update(state, (current) =>
    current.status === "live"
      ? current
      : {
          ...current,
          status: "synchronizing" as const,
          error: Option.none(),
        },
  );
  const setStreamError = (error: unknown) =>
    Effect.sync(() => {
      if (activeSubscription !== undefined) {
        activeSubscription.synchronized = false;
        activeSubscription.failed = true;
      }
    }).pipe(
      Effect.andThen(Effect.logWarning("Could not synchronize the environment shell.")),
      Effect.annotateLogs({
        environmentId,
        ...safeErrorLogAttributes(error),
      }),
      Effect.andThen(
        SubscriptionRef.update(state, (current) => ({
          ...current,
          status: shellStatusForSnapshot(current.snapshot),
          error: Option.some(SHELL_SYNCHRONIZATION_ERROR_MESSAGE),
        })),
      ),
    );

  // Server items and deferred link fills both read the snapshot and write it back; one at a
  // time, so neither can overwrite what the other wrote in between.
  const snapshotWriteLock = yield* Semaphore.make(1);

  // Apply each received batch with one state write. The RPC client's bounded
  // buffer can split a server chunk, so a bulk action can still need several
  // writes, but each write includes every event in that batch.
  const applyItems = Effect.fn("EnvironmentShellState.applyItems")(function* (
    items: ReadonlyArray<OrchestrationV2ShellStreamItem>,
    // Ids of rows whose links will fill later; they are held out of saves before one queues.
    pendingLinkIds?: ReadonlySet<ThreadId>,
    subscription = activeSubscription,
  ) {
    // An old stream's buffered items must not mutate or certify its replacement,
    // including a foreground resubscription on the very same RpcSession.
    if (
      subscription === undefined ||
      subscription.failed ||
      subscription !== activeSubscription ||
      Option.getOrNull(yield* SubscriptionRef.get(supervisor.session)) !== subscription.session
    )
      return;
    const initial = yield* SubscriptionRef.get(state);
    let waiting = subscription.awaitingCompletion;
    let next = initial;
    for (const item of items) {
      if (item.kind === "synchronized") {
        waiting = false;
        if (Option.isSome(next.snapshot)) {
          subscription.synchronized =
            subscription.supportsCompletionMarker &&
            subscription.authoritative &&
            subscription.serverStartedAt !== undefined;
          next = { ...next, status: "live", error: Option.none() };
        }
        continue;
      }
      const nextSnapshot =
        item.kind === "snapshot"
          ? mergeShellSnapshotProjects(
              Option.getOrNull(next.snapshot),
              item.snapshot,
              item.resolvedRepositoryIdentityRoots === undefined
                ? undefined
                : {
                    resolvedRepositoryIdentityRoots: item.resolvedRepositoryIdentityRoots,
                  },
            )
          : Option.match(next.snapshot, {
              onNone: () => null,
              onSome: (snapshot) =>
                item.sequence > snapshot.snapshotSequence
                  ? applyShellStreamEvent(snapshot, item)
                  : snapshot,
            });
      if (nextSnapshot === null) continue;
      if (item.kind === "snapshot" && item.resolvedRepositoryIdentityRoots === undefined) {
        subscription.authoritative = true;
        subscription.synchronized = false;
      }
      next = {
        snapshot: Option.some(nextSnapshot),
        status: waiting ? "synchronizing" : "live",
        error: Option.none(),
      };
    }
    subscription.awaitingCompletion = waiting;
    if (next === initial) return;
    if (pendingLinkIds !== undefined && Option.isSome(next.snapshot)) {
      pendingLinkRows = new Set(
        next.snapshot.value.threads.filter((thread) => pendingLinkIds.has(thread.id)),
      );
    }
    if (Option.isSome(next.snapshot)) {
      yield* Ref.set(latestLiveSnapshot, next.snapshot);
    }
    yield* SubscriptionRef.set(state, next);
    if (next.snapshot !== initial.snapshot && Option.isSome(next.snapshot)) {
      yield* Queue.offer(persistence, next.snapshot.value);
    }
  }, snapshotWriteLock.withPermit);

  // Rows can arrive before their pull request links (see DeferredShellSnapshot), so the list
  // paints first. The links then fill into the rows that still hold the object they arrived
  // with; a row the server has replaced since carries its own links.
  // Captured so fills started from the subscription callback still end with this state.
  const stateScope = yield* Effect.scope;
  // Each full snapshot supersedes the previous one's fill; links from an older snapshot must
  // not land on rows a newer snapshot reused. A cancelled fill's rows stay pending until the
  // next snapshot replaces the pending set.
  let activeFill: Fiber.Fiber<void> | undefined;
  const cancelActiveFill = Effect.suspend(() => {
    const fill = activeFill;
    activeFill = undefined;
    return fill === undefined ? Effect.void : Fiber.interrupt(fill);
  });
  const fillDeferredPullRequests = Effect.fnUntraced(function* (
    source: DeferredShellSnapshot,
    applied: OrchestrationV2ShellSnapshot,
  ) {
    yield* cancelActiveFill;
    if (source.loadPullRequests === undefined) return;
    const arrivedRows = new Set(applied.threads);
    const arrivedIds = new Set(source.threads.map((thread) => thread.id));
    pendingLinkRows = new Set(applied.threads.filter((thread) => arrivedIds.has(thread.id)));
    activeFill = yield* source.loadPullRequests.pipe(
      Effect.flatMap((linksByThreadId) =>
        writeFilledLinks(linksByThreadId, arrivedRows, arrivedIds),
      ),
      Effect.forkIn(stateScope),
    );
  });
  const writeFilledLinks = Effect.fnUntraced(
    function* (
      linksByThreadId: ReadonlyMap<ThreadId, ReadonlyArray<ThreadPullRequestLink>>,
      arrivedRows: ReadonlySet<OrchestrationV2ThreadShell>,
      arrivedIds: ReadonlySet<ThreadId>,
    ) {
      const filled = yield* SubscriptionRef.modify(
        state,
        (current): [Option.Option<OrchestrationV2ShellSnapshot>, EnvironmentShellState] => {
          if (Option.isNone(current.snapshot)) return [Option.none(), current];
          const snapshot = current.snapshot.value;
          let changed = false;
          const threads = snapshot.threads.map((thread) => {
            if (!arrivedRows.has(thread) || !arrivedIds.has(thread.id)) return thread;
            const links = linksByThreadId.get(thread.id);
            // A reused row may already hold these links from an earlier fill.
            if (sameThreadPullRequests(thread.pullRequests, links)) return thread;
            changed = true;
            if (links === undefined) {
              const { pullRequests: _previous, ...row } = thread;
              return row;
            }
            return { ...thread, pullRequests: links };
          });
          if (!changed) return [Option.none(), current];
          const next = { ...snapshot, threads };
          return [Option.some(next), { ...current, snapshot: Option.some(next) }];
        },
      );
      // Saves skipped while links were pending are queued again. Cached rows are never live:
      // until a server snapshot arrives there is nothing new to save.
      pendingLinkRows = new Set();
      const live = yield* Ref.get(latestLiveSnapshot);
      if (Option.isNone(live)) return;
      const latest = Option.isSome(filled) ? filled : live;
      yield* Ref.set(latestLiveSnapshot, latest);
      yield* Queue.offer(persistence, latest.value);
    },
    (effect) => snapshotWriteLock.withPermit(effect),
  );
  if (Option.isSome(cached) && Option.isSome(cachedSnapshot)) {
    yield* fillDeferredPullRequests(cached.value, cachedSnapshot.value);
  }

  const foregroundResubscriptions = Option.match(wakeups, {
    onNone: () => Stream.never,
    onSome: (service) =>
      service.changes.pipe(Stream.filter(ConnectionWakeups.shouldResubscribeAfterWakeup)),
  });

  const prefetchedSnapshot = yield* SubscriptionRef.make<
    Option.Option<{
      readonly prepared: PreparedConnection;
      readonly take: Effect.Effect<Option.Option<DeferredShellSnapshot>>;
      readonly cancel: Effect.Effect<void>;
    }>
  >(Option.none());
  // Prepared routes arrive before socket/config readiness. Each route owns one
  // speculative load; replacing or clearing it interrupts the load and drops
  // its result. Only a published, validated session can consume that result.
  yield* SubscriptionRef.changes(supervisor.prepared).pipe(
    Stream.changesWith((left, right) => Option.getOrNull(left) === Option.getOrNull(right)),
    Stream.switchMap((prepared) =>
      Stream.fromEffect(
        Effect.gen(function* () {
          if (Option.isNone(prepared)) {
            yield* SubscriptionRef.set(prefetchedSnapshot, Option.none());
            return;
          }
          const shouldPrefetch = yield* (
            snapshotLoader.shouldPrefetch?.(environmentId) ?? Effect.succeed(true)
          );
          if (!shouldPrefetch) {
            yield* SubscriptionRef.set(
              prefetchedSnapshot,
              Option.some({
                prepared: prepared.value,
                take: snapshotLoader.load(prepared.value),
                cancel: Effect.void,
              }),
            );
            return;
          }
          const result = yield* Deferred.make<Option.Option<DeferredShellSnapshot>>();
          const load = yield* snapshotLoader.load(prepared.value).pipe(
            Effect.flatMap((snapshot) => Deferred.succeed(result, snapshot)),
            Effect.forkChild,
          );
          const pending = yield* Ref.make(Option.some(result));
          yield* SubscriptionRef.set(
            prefetchedSnapshot,
            Option.some({
              prepared: prepared.value,
              cancel: Fiber.interrupt(load).pipe(Effect.asVoid),
              // Consume once and release the stored result. Session replacements
              // and same-session HTTP failure retries must refresh again.
              take: Ref.getAndSet(pending, Option.none()).pipe(
                Effect.flatMap(
                  Option.match({
                    onNone: () => snapshotLoader.load(prepared.value),
                    onSome: Deferred.await,
                  }),
                ),
              ),
            }),
          );
          yield* Fiber.join(load);
        }),
      ),
    ),
    Stream.runDrain,
    Effect.forkScoped,
  );

  const cancelPrefetch = Effect.gen(function* () {
    const prefetch = yield* SubscriptionRef.get(prefetchedSnapshot);
    if (Option.isSome(prefetch)) yield* prefetch.value.cancel;
    yield* snapshotLoader.cancelPrefetch?.(environmentId) ?? Effect.void;
  });

  yield* setSynchronizing;
  yield* Effect.forkScoped(
    subscribeDynamicMapped(
      ORCHESTRATION_V2_WS_METHODS.subscribeShell,
      Effect.fn("EnvironmentShellState.makeSubscribeInput")(function* (session) {
        // Consume eligibility before any async work. If this initial load or
        // replay is interrupted, the next attempt must refresh authoritatively.
        const previous = activeSubscription;
        const subscription: ShellSubscription = {
          session,
          environment: undefined,
          serverStartedAt: undefined,
          supportsCompletionMarker: false,
          awaitingCompletion: false,
          authoritative: false,
          synchronized: false,
          failed: false,
        };
        activeSubscription = subscription;
        const config = yield* session.initialConfig.pipe(Effect.option);
        const supportsCompletionMarker =
          Option.isSome(config) && config.value.shellResumeCompletionMarker === true;
        subscription.environment = Option.getOrUndefined(config)?.environment;
        subscription.supportsCompletionMarker = supportsCompletionMarker;
        subscription.awaitingCompletion = supportsCompletionMarker;
        yield* setSynchronizing;

        // Config has no process identity. The lifecycle stream replays the
        // ready timestamp published once at startup, even on a fresh socket.
        // Checking it also catches a reset whose new head passed our cursor.
        // If an older/broken server cannot provide it, keep the HTTP path.
        if (supportsCompletionMarker) {
          subscription.serverStartedAt =
            previous?.session === session && previous.serverStartedAt !== undefined
              ? previous.serverStartedAt
              : yield* getSessionServerStartedAt(session);
        }

        // A cursor is safe only above a complete base loaded in this runtime
        // and caught up to a completion marker. Disk caches and metadata-only
        // enrichment cannot establish that provenance. Identity/version changes
        // and legacy servers take the HTTP path even on the same session.
        let canResume =
          supportsCompletionMarker &&
          previous?.synchronized === true &&
          subscription.serverStartedAt !== undefined &&
          previous.serverStartedAt === subscription.serverStartedAt &&
          subscription.environment !== undefined &&
          subscription.environment.environmentId === environmentId &&
          previous.environment?.environmentId === subscription.environment.environmentId &&
          previous.environment.serverVersion === subscription.environment.serverVersion &&
          previous.environment.orchestrationProtocolVersion ===
            subscription.environment.orchestrationProtocolVersion;
        subscription.authoritative = canResume;
        let current = yield* SubscriptionRef.get(state);
        if (canResume && Option.isSome(current.snapshot)) {
          yield* cancelPrefetch;
        }
        if (!canResume || Option.isNone(current.snapshot)) {
          subscription.authoritative = false;
          const prepared = yield* SubscriptionRef.get(supervisor.prepared).pipe(
            Effect.flatMap(
              Option.match({
                onSome: Effect.succeed,
                onNone: () =>
                  SubscriptionRef.changes(supervisor.prepared).pipe(
                    Stream.filter(Option.isSome),
                    Stream.map((value) => value.value),
                    Stream.runHead,
                    Effect.map(Option.getOrThrow),
                  ),
              }),
            ),
          );
          const prefetch = yield* SubscriptionRef.changes(prefetchedSnapshot).pipe(
            Stream.filter(Option.isSome),
            Stream.map((value) => value.value),
            Stream.filter((value) => value.prepared === prepared),
            Stream.runHead,
            Effect.map(Option.getOrThrow),
          );
          const httpSnapshot = yield* prefetch.take;
          const activeSession = yield* SubscriptionRef.get(supervisor.session);
          const activePrepared = yield* SubscriptionRef.get(supervisor.prepared);
          if (
            Option.getOrNull(activeSession) !== session ||
            Option.getOrNull(activePrepared) !== prepared
          ) {
            return yield* Effect.never;
          }
          if (Option.isSome(httpSnapshot)) {
            yield* cancelActiveFill;
            const previous = yield* SubscriptionRef.get(state);
            const snapshot = reuseUnchangedThreadShells(
              Option.getOrNull(previous.snapshot),
              withoutDeferredPullRequests(httpSnapshot.value),
            );
            const pendingLinkIds = new Set(
              httpSnapshot.value.loadPullRequests === undefined
                ? []
                : httpSnapshot.value.threads.map((thread) => thread.id),
            );
            yield* applyItems([{ kind: "snapshot", snapshot }], pendingLinkIds, subscription);
            canResume = true;
            current = yield* SubscriptionRef.get(state);
            if (Option.isSome(current.snapshot)) {
              yield* fillDeferredPullRequests(httpSnapshot.value, current.snapshot.value);
            }
          }
        }

        // If the authoritative refresh failed, omit the cached cursor so the
        // socket fallback sends a complete snapshot for this new session.
        if (!canResume || Option.isNone(current.snapshot)) {
          return supportsCompletionMarker ? { requestCompletionMarker: true as const } : {};
        }
        if (!supportsCompletionMarker) {
          // Without a completion marker there is no synchronized signal for a
          // resumed subscription, so report live immediately, like threads.
          yield* SubscriptionRef.update(state, (value) => ({
            ...value,
            status: "live" as const,
            error: Option.none(),
          }));
        }
        return {
          afterSequence: current.snapshot.value.snapshotSequence,
          ...(supportsCompletionMarker ? { requestCompletionMarker: true as const } : {}),
        };
      }),
      (_session, stream) => {
        const subscription = activeSubscription;
        return stream.pipe(
          Stream.map((item) => ({ subscription, item })),
          Stream.tapCause((cause) => {
            // Closing a socket or cancelling a subscription after completion
            // leaves its applied base intact. Protocol/application failures do
            // not carry that guarantee. An unfinished replay is never warm.
            if (
              Cause.hasInterruptsOnly(cause) ||
              cause.reasons.every(
                (reason) =>
                  reason._tag === "Fail" &&
                  isRpcClientError(reason.error) &&
                  reason.error.reason._tag !== "RpcClientDefect",
              )
            )
              return Effect.void;
            return Effect.sync(() => {
              if (subscription !== undefined) {
                subscription.synchronized = false;
                subscription.failed = true;
              }
            });
          }),
        );
      },
      {
        onExpectedFailure: (cause) => setStreamError(Cause.squash(cause)),
        onDefect: (cause) => setStreamError(Cause.squash(cause)),
        retryExpectedFailureAfter: "250 millis",
        resubscribe: foregroundResubscriptions,
      },
    ).pipe(
      Stream.runForEachArray((items) => {
        const subscription = activeSubscription;
        return applyItems(
          items.filter((value) => value.subscription === subscription).map((value) => value.item),
          undefined,
          subscription,
        );
      }),
    ),
  );
  yield* SubscriptionRef.changes(supervisor.state).pipe(
    Stream.runForEach((connectionState) => {
      switch (connectionProjectionPhase(connectionState)) {
        case "synchronizing":
          return setSynchronizing;
        case "disconnected":
          return setDisconnected;
        case "ready":
          return setReady;
      }
    }),
    Effect.forkScoped,
  );

  return state;
});

function shellStateChanges(environmentId: EnvironmentId) {
  return followStreamInEnvironment(
    environmentId,
    Stream.unwrap(makeEnvironmentShellState().pipe(Effect.map(SubscriptionRef.changes))),
  );
}

export interface EnvironmentShellSummary {
  readonly hasSnapshot: boolean;
  readonly hasSynchronizingShell: boolean;
  readonly hasCachedShell: boolean;
  readonly hasLiveShell: boolean;
  readonly firstError: string | null;
}

const EMPTY_ENVIRONMENT_SHELL_SUMMARY: EnvironmentShellSummary = Object.freeze({
  hasSnapshot: false,
  hasSynchronizingShell: false,
  hasCachedShell: false,
  hasLiveShell: false,
  firstError: null,
});

const EMPTY_SERVER_CONFIGS: ReadonlyMap<EnvironmentId, ServerConfig> = new Map();

function shellSummariesEqual(
  left: EnvironmentShellSummary,
  right: EnvironmentShellSummary,
): boolean {
  return (
    left.hasSnapshot === right.hasSnapshot &&
    left.hasSynchronizingShell === right.hasSynchronizingShell &&
    left.hasCachedShell === right.hasCachedShell &&
    left.hasLiveShell === right.hasLiveShell &&
    left.firstError === right.firstError
  );
}

function mapsEqual<K, V>(left: ReadonlyMap<K, V>, right: ReadonlyMap<K, V>): boolean {
  if (left.size !== right.size) {
    return false;
  }
  for (const [key, value] of left) {
    if (right.get(key) !== value) {
      return false;
    }
  }
  return true;
}

export function createEnvironmentShellSummaryAtom(input: {
  readonly catalogValueAtom: Atom.Atom<EnvironmentCatalogState>;
  readonly shellStateValueAtom: (environmentId: EnvironmentId) => Atom.Atom<EnvironmentShellState>;
}) {
  let previousSummary = EMPTY_ENVIRONMENT_SHELL_SUMMARY;
  return Atom.make((get) => {
    let hasSnapshot = false;
    let hasSynchronizingShell = false;
    let hasCachedShell = false;
    let hasLiveShell = false;
    let firstError: string | null = null;

    for (const environmentId of enabledEnvironmentIds(get(input.catalogValueAtom))) {
      const state = get(input.shellStateValueAtom(environmentId));
      hasSynchronizingShell ||= state.status === "synchronizing";
      hasCachedShell ||= state.status === "cached";
      hasLiveShell ||= state.status === "live";
      if (firstError === null) {
        firstError = Option.getOrNull(state.error);
      }
      if (Option.isNone(state.snapshot)) {
        continue;
      }
      hasSnapshot = true;
    }

    const next: EnvironmentShellSummary = {
      hasSnapshot,
      hasSynchronizingShell,
      hasCachedShell,
      hasLiveShell,
      firstError,
    };
    if (shellSummariesEqual(previousSummary, next)) {
      return previousSummary;
    }
    previousSummary = next;
    return previousSummary;
  }).pipe(Atom.withLabel("environment-shell-summary"));
}

export function createEnvironmentServerConfigsAtom(input: {
  readonly catalogValueAtom: Atom.Atom<EnvironmentCatalogState>;
  readonly serverConfigValueAtom: (environmentId: EnvironmentId) => Atom.Atom<ServerConfig | null>;
}) {
  let previousServerConfigs = EMPTY_SERVER_CONFIGS;
  return Atom.make((get) => {
    const next = new Map<EnvironmentId, ServerConfig>();
    for (const environmentId of enabledEnvironmentIds(get(input.catalogValueAtom))) {
      const config = get(input.serverConfigValueAtom(environmentId));
      if (config !== null) {
        next.set(environmentId, config);
      }
    }
    if (mapsEqual(previousServerConfigs, next)) {
      return previousServerConfigs;
    }
    previousServerConfigs = next;
    return previousServerConfigs;
  }).pipe(Atom.withLabel("environment-server-configs"));
}

export function createEnvironmentShellAtoms<R, E>(
  runtime: Atom.AtomRuntime<
    | EnvironmentRegistry.EnvironmentRegistry
    | Persistence.EnvironmentCacheStore
    | ShellSnapshotLoader.ShellSnapshotLoader
    | R,
    E
  >,
) {
  const stateAtom = Atom.family((environmentId: EnvironmentId) =>
    runtime.atom(shellStateChanges(environmentId), {
      initialValue: EMPTY_SHELL_STATE,
    }),
  );

  const stateValueAtom = Atom.family((environmentId: EnvironmentId) =>
    Atom.make((get) =>
      Option.getOrElse(AsyncResult.value(get(stateAtom(environmentId))), () => EMPTY_SHELL_STATE),
    ).pipe(Atom.withLabel(`environment-shell-state-value:${environmentId}`)),
  );

  return {
    stateAtom,
    stateValueAtom,
  };
}

export * from "./models.ts";
export * from "./shellCommands.ts";
export * from "./shellReducer.ts";
export * from "./shellPullRequests.ts";
export * as ShellSnapshotLoader from "./shellSnapshotHttp.ts";
export * from "./snapshots.ts";
