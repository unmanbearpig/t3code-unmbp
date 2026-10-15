// @effect-diagnostics nodeBuiltinImport:off globalTimers:off deterministicKeys:off - Bench clocks and socket observations must not add RPC round trips.
import * as NodeFS from "node:fs";
import type { OrchestrationV2ShellSnapshot } from "@t3tools/contracts";
import { Agent } from "undici";
import * as Clock from "effect/Clock";
import { observedScope, observedStream } from "./reconnectLifecycle.ts";
import { isFullShellSnapshot } from "./reconnectObservation.ts";
import { backgroundClock } from "./reconnectClock.ts";
import { workloadPlan } from "./reconnectWorkload.ts";
import * as NodeTls from "node:tls";
import NodeWebSocket from "ws";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { FetchHttpClient, HttpClient } from "effect/http";
import * as Socket from "effect/socket/Socket";
import * as RemoteAuthorization from "../../../../packages/client-runtime/src/authorization/service.ts";
import * as TokenStore from "../../../../packages/client-runtime/src/authorization/tokenStore.ts";
import * as Catalog from "../../../../packages/client-runtime/src/connection/catalog.ts";
import * as CredentialStore from "../../../../packages/client-runtime/src/connection/credentialStore.ts";
import * as ProfileStore from "../../../../packages/client-runtime/src/connection/profileStore.ts";
import * as Resolver from "../../../../packages/client-runtime/src/connection/resolver.ts";
import * as Driver from "../../../../packages/client-runtime/src/connection/driver.ts";
import * as Supervisor from "../../../../packages/client-runtime/src/connection/supervisor.ts";
import * as Connectivity from "../../../../packages/client-runtime/src/connection/connectivity.ts";
import * as Wakeups from "../../../../packages/client-runtime/src/connection/wakeups.ts";
import {
  BearerConnectionTarget,
  type NetworkStatus,
} from "../../../../packages/client-runtime/src/connection/model.ts";
import * as Capabilities from "../../../../packages/client-runtime/src/platform/capabilities.ts";
import * as Persistence from "../../../../packages/client-runtime/src/platform/persistence.ts";
import * as ManagedRelay from "../../../../packages/client-runtime/src/relay/managedRelay.ts";
import * as Session from "../../../../packages/client-runtime/src/rpc/session.ts";
import * as BoundedSnapshot from "../../../../packages/client-runtime/src/state/boundedThreadSnapshotHttp.ts";
import * as History from "../../../../packages/client-runtime/src/state/threadHistoryController.ts";
import * as ShellSnapshot from "../../../../packages/client-runtime/src/state/shellSnapshotHttp.ts";
import { makeEnvironmentShellState } from "../../../../packages/client-runtime/src/state/shell.ts";
import { blackholeSocket, slowSocket, blackholePreferredRoute } from "./reconnectNetwork.ts";
import type { VariantOptions } from "./reconnectOptions.ts";
import { makeEnvironmentThreadState } from "../../../../packages/client-runtime/src/state/threads.ts";
import { operationThreadId, type OperationControlResponse } from "./reconnectFixture.ts";
import { observeHttpClient } from "./reconnectHttp.ts";
import { operationStateError, waitForOperationState } from "./reconnectState.ts";
import type { ReconnectCase } from "./reconnectManifest.ts";

export interface Stage {
  readonly name: string;
  readonly attempt?: number;
  readonly atMs: number;
  readonly endMs?: number;
  readonly bytes?: number;
  readonly outcome?: "succeeded" | "failed";
}
export function linkCounters() {
  const row = NodeFS.readFileSync("/proc/net/dev", "utf8")
    .split("\n")
    .find((line) => line.trimStart().startsWith("lo:"));
  if (!row) throw new Error("Missing loopback IP counters");
  const fields = row.split(":")[1]!.trim().split(/\s+/).map(Number);
  return { bytes: fields[8]!, packets: fields[9]! };
}
class HeadersConstructor extends Context.Service<
  HeadersConstructor,
  (url: string, headers: Readonly<Record<string, string>>) => Socket.WebSocketLike
>()("@t3tools/client-runtime/platform/capabilities/WebSocketConstructorWithHeaders") {}
class ExperimentalHello extends Context.Service<ExperimentalHello, { readonly enabled: boolean }>()(
  "@t3tools/client-runtime/platform/capabilities/ExperimentalReconnectHello",
) {}
class ExperimentalOptions extends Context.Service<
  ExperimentalOptions,
  { readonly skipDescriptorOnHttpsReconnect: boolean }
>()("@t3tools/client-runtime/platform/capabilities/ExperimentalReconnectOptions") {}
const decodeFrame = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

/** Every measured service below is production code; only platform persistence and unused capabilities are fixed. */
export const runReconnect = Effect.fnUntraced(function* (input: {
  readonly entry: ReconnectCase;
  readonly environmentId: import("@t3tools/contracts").EnvironmentId;
  readonly registerResource: (kind: string, resource: object) => string;
  readonly registerSocket: (socket: NodeWebSocket) => void;
  readonly queuePackets: number;
  readonly origin: string;
  readonly bearer: string;
  readonly cert: string;
  readonly initial: OperationControlResponse;
  readonly advance: () => Effect.Effect<OperationControlResponse, Error>;
  readonly options: VariantOptions;
  readonly startController: () => void;
}) {
  const plan = workloadPlan(input.entry);
  const resources: string[] = [];
  const track = (kind: string, resource: object) => {
    resources.push(input.registerResource(kind, resource));
  };
  const memoMap = yield* Layer.makeMemoMap;
  const caseScope = yield* Effect.scope;
  track("client-runtime", memoMap);
  let wallOffsetMs = 0;
  const baseClock = yield* Clock.Clock;
  const clock = backgroundClock(baseClock, () => wallOffsetMs);
  let start = 0;
  let measurementStartMs = 0;
  let observationStartMs = 0;
  const stages: Stage[] = [];
  const requests: Record<string, number> = {};
  const rpcRequests: Record<string, number> = {};
  let attempt = 0;
  let sockets = 0;
  let events = 0;
  let snapshots = 0;
  let shellSnapshots = 0;
  let usesHelloHandshake = false;
  let sequence = input.initial.sequence;
  let sequenceError = false;
  const synchronized = yield* Deferred.make<void>();
  const preparationStarted = yield* Deferred.make<void>();
  let activeSocket: NodeWebSocket | undefined;
  let oldSocket: NodeWebSocket | undefined;
  const liveSockets = new Set<NodeWebSocket>();
  let peakSockets = 0;
  let activeSocketPort: number | undefined;

  const switches = {
    helloHandshake: input.options.helloHandshake && "ExperimentalReconnectHello" in Capabilities,
    helloSupportCached: input.options.helloHandshake && input.options.helloSupportCached,
    websocketHeaders:
      input.options.websocketHeaders && "WebSocketConstructorWithHeaders" in Capabilities,
    skipDescriptorOnHttpsReconnect:
      input.options.skipDescriptorOnHttpsReconnect &&
      "ExperimentalReconnectOptions" in Capabilities,
  };
  if (input.options.skipDescriptorOnHttpsReconnect && !switches.skipDescriptorOnHttpsReconnect)
    return yield* Effect.die(new Error("Requested descriptor switch unavailable"));
  if (input.options.helloHandshake && !switches.helloHandshake)
    return yield* Effect.die(new Error("Requested hello switch unavailable"));
  const helloKeys = new Set<string>();
  track("hello-support-cache", helloKeys);
  const helloKey = (id: string, http: string, ws: string) =>
    `${id.length}:${id}${JSON.stringify([http, ws])}`;
  if (switches.helloSupportCached)
    for (const index of input.entry.routes === 2 ? [0, 1] : [0]) {
      const http = index === 0 ? input.origin : input.origin.replace("127.0.0.1", "127.0.0.2");
      helloKeys.add(helloKey(input.environmentId, http, http.replace(/^http/, "ws")));
    }
  const helloSupport = {
    load: (id: string, http: string, ws: string) =>
      Effect.sync(() => helloKeys.has(helloKey(id, http, ws))),
    save: (id: string, http: string, ws: string) =>
      Effect.sync(() => {
        helloKeys.add(helloKey(id, http, ws));
      }),
    remove: (id: string, http: string, ws: string) =>
      Effect.sync(() => {
        helloKeys.delete(helloKey(id, http, ws));
      }),
  };
  const stamp = (name: string) => {
    if (start) stages.push({ name, atMs: performance.now() - start, attempt });
  };
  const observe = (text: string, outgoing: boolean, requestTags: Map<string, unknown>) => {
    const decoded = decodeFrame(text);
    for (const message of Array.isArray(decoded) ? decoded : [decoded]) {
      if (!Predicate.hasProperty(message, "_tag")) continue;
      if (!outgoing && message._tag === "Hello") stamp("hello-received");
      if (outgoing && message._tag === "BearerAuth") stamp("auth-sent");
      if (!outgoing && message._tag === "Authenticated") stamp("authenticated");
      if (outgoing && message._tag === "Request" && Predicate.hasProperty(message, "tag")) {
        if (typeof message.tag === "string")
          rpcRequests[message.tag] = (rpcRequests[message.tag] ?? 0) + 1;
        if (Predicate.hasProperty(message, "id")) requestTags.set(String(message.id), message.tag);
        if (message.tag === "orchestration.subscribeShell") stamp("shell-subscribe");
        if (message.tag === "server.probe") stamp("probe-request");
        if (message.tag === "subscribeServerConfig") stamp("config-request");
        if (message.tag === "orchestration.subscribeThread") stamp("subscribe");
      }
      if (
        outgoing ||
        message._tag !== "Chunk" ||
        !Predicate.hasProperty(message, "values") ||
        !Array.isArray(message.values)
      )
        continue;
      const tag = Predicate.hasProperty(message, "requestId")
        ? requestTags.get(String(message.requestId))
        : undefined;
      for (const value of message.values) {
        if (tag === "orchestration.subscribeShell") {
          if (isFullShellSnapshot(value)) shellSnapshots++;
          if (Predicate.hasProperty(value, "kind") && value.kind === "synchronized")
            stamp("shell-synchronized");
          continue;
        }
        if (Predicate.hasProperty(value, "config")) stamp("config");
        if (!start || !Predicate.hasProperty(value, "kind")) continue;
        if (value.kind === "snapshot") snapshots++;
        if (value.kind === "event") {
          events++;
          if (!Predicate.hasProperty(value, "sequence") || value.sequence !== sequence + 1)
            sequenceError = true;
          if (Predicate.hasProperty(value, "sequence") && typeof value.sequence === "number")
            sequence = value.sequence;
        }
        if (value.kind === "synchronized") {
          stamp("synchronized");
          Deferred.doneUnsafe(synchronized, Effect.void);
        }
      }
    }
  };
  const socketConstructor: Socket.WebSocketConstructor["Service"] = (url, options) => {
    sockets++;
    const path = new URL(url).pathname;
    requests[path] = (requests[path] ?? 0) + 1;
    stamp("socket-start");
    const wsOptions = typeof options === "object" && !Array.isArray(options) ? options : {};
    const protocols = typeof options === "string" || Array.isArray(options) ? options : [];
    const socket = new NodeWebSocket(url, protocols, {
      ...wsOptions,
      ca: input.cert,
      minVersion: "TLSv1.3",
      maxVersion: "TLSv1.3",
      perMessageDeflate: true,
    });
    liveSockets.add(socket);
    peakSockets = Math.max(peakSockets, liveSockets.size);
    socket.once("close", () => liveSockets.delete(socket));
    const requestTags = new Map<string, unknown>();
    usesHelloHandshake ||= new URL(url).searchParams.get("handshake") === "hello-v1";
    input.registerSocket(socket);
    socket.on("error", () => stamp("socket-error"));
    if (start) Deferred.doneUnsafe(preparationStarted, Effect.void);
    const old = activeSocket === undefined;
    socket.once("close", () => stamp(old && plan.establishOld ? "old-close" : "socket-close"));
    activeSocket = socket;
    socket.once("upgrade", (response) => {
      activeSocketPort = response.socket.localPort;
      if (
        input.entry.scheme === "https" &&
        (!(response.socket instanceof NodeTls.TLSSocket) ||
          response.socket.getProtocol() !== "TLSv1.3")
      )
        throw new Error("Wrong TLS version");
    });
    socket.once("open", () => {
      stamp("socket-open");
      if (!socket.extensions.includes("permessage-deflate")) throw new Error("Deflate missing");
    });
    socket.on("message", (data) => {
      if (socket === activeSocket) observe(data.toString(), false, requestTags);
    });
    const send = socket.send.bind(socket);
    socket.send = (
      data,
      options?: Parameters<NodeWebSocket["send"]>[1] | ((error?: Error) => void),
      callback?: (error?: Error) => void,
    ) => {
      observe(String(data), true, requestTags);
      if (typeof options === "function") send(data, options);
      else send(data, options ?? {}, callback);
    };
    return socket;
  };
  const makePool = () => {
    const agent = new Agent({
      connect: { ca: input.cert, minVersion: "TLSv1.3", maxVersion: "TLSv1.3" },
    });
    track("http-pool", agent);
    return agent;
  };
  let pool = yield* Effect.sync(makePool);
  yield* Effect.addFinalizer(() => Effect.promise(() => pool.destroy()));
  // Keep Node's bundled fetch and its timing. Only its dispatcher is case-local.
  const unpooled = yield* HttpClient.HttpClient.pipe(Effect.provide(FetchHttpClient.layer));
  const requestInit = { keepalive: true, dispatcher: pool };
  const httpBase = HttpClient.transform(unpooled, (effect) =>
    effect.pipe(Effect.provideService(FetchHttpClient.RequestInit, requestInit)),
  );
  const http = observeHttpClient(
    httpBase,
    stages,
    requests,
    () => performance.now() - start,
    () => {
      if (start) Deferred.doneUnsafe(preparationStarted, Effect.void);
    },
  );
  const unused = Effect.die(new Error("Unrelated platform service used by reconnect"));
  const helloPersistence = { loadShell: () => Effect.succeedNone, helloSupport };
  const platform = Layer.mergeAll(
    Layer.succeed(Clock.Clock, clock),
    Layer.mock(Persistence.EnvironmentCacheStore)(helloPersistence),
    ...(switches.helloHandshake ? [Layer.succeed(ExperimentalHello, { enabled: true })] : []),
    Layer.succeed(HttpClient.HttpClient, http),
    Layer.succeed(Socket.WebSocketConstructor, socketConstructor),
    ...(switches.websocketHeaders
      ? [Layer.succeed(HeadersConstructor, (url, headers) => socketConstructor(url, { headers }))]
      : []),
    ...(switches.skipDescriptorOnHttpsReconnect
      ? [Layer.succeed(ExperimentalOptions, { skipDescriptorOnHttpsReconnect: true })]
      : []),
    Layer.succeed(Capabilities.ClientPresentation, { metadata: { surface: "mobile" } }),
    Layer.mock(Capabilities.CloudSession)({ identity: Effect.succeed(Option.none()) }),
    Layer.mock(Capabilities.RelayDeviceIdentity)({ deviceId: Effect.succeed(Option.none()) }),
    Layer.mock(Capabilities.PrimaryEnvironmentAuth)({ bearerToken: Effect.succeed(Option.none()) }),
    Layer.mock(Capabilities.SshEnvironmentGateway)({ prepare: () => unused }),
    Layer.mock(ManagedRelay.ManagedRelayDpopSigner)({}),
    Layer.mock(ManagedRelay.ManagedRelayClient)({ relayUrl: "https://unused.invalid" }),
    Layer.mock(TokenStore.RemoteDpopAccessTokenStore)({}),
    Layer.mock(CredentialStore.ConnectionCredentialStore)({
      get: () =>
        Effect.succeed(
          Option.some(new Catalog.BearerConnectionCredential({ token: input.bearer })),
        ),
    }),
    Layer.mock(ProfileStore.ConnectionProfileStore)({}),
  );
  const authorization = RemoteAuthorization.layer.pipe(Layer.provideMerge(platform));
  const sessionLayer = Layer.effect(
    Session.RpcSessionFactory,
    Effect.gen(function* () {
      const factory = yield* Session.RpcSessionFactory;
      return Session.RpcSessionFactory.of({
        connect: (prepared) =>
          factory.connect(prepared).pipe(
            Effect.map((session) => {
              const sessionAttempt = attempt;
              const stampSession = (name: string) => {
                if (start)
                  stages.push({ name, atMs: performance.now() - start, attempt: sessionAttempt });
              };
              return {
                ...session,
                client: new Proxy(session.client, {
                  get: (target, key, receiver) => {
                    const method: unknown = Reflect.get(target, key, receiver);
                    if (
                      typeof method !== "function" ||
                      typeof key !== "string" ||
                      !key.includes("subscribe")
                    )
                      return method;
                    return (...args: unknown[]) => {
                      const stream: unknown = Reflect.apply(method, target, args);
                      return Stream.isStream(stream)
                        ? observedStream(stream, stampSession, `stream-cancel:${key}`)
                        : stream;
                    };
                  },
                }),
              };
            }),
          ),
      });
    }),
  ).pipe(Layer.provide(Session.layer({})));
  const connectionLayers = Driver.layer.pipe(
    Layer.provide(Resolver.layer),
    Layer.provide(sessionLayer),
    Layer.provide(authorization),
  );
  const context = yield* Layer.buildWithMemoMap(connectionLayers, memoMap, caseScope);
  const driver = Context.get(context, Driver.ConnectionDriver);
  track("driver", driver);
  const measuredDriver = Driver.ConnectionDriver.of({
    ...driver,
    connect: (entry, report) => {
      attempt++;
      const generation = attempt;
      const stampAttempt = (name: string) => {
        if (start) stages.push({ name, atMs: performance.now() - start, attempt: generation });
      };
      stamp("attempt-start");
      return Effect.gen(function* () {
        if ((yield* Clock.Clock) !== clock)
          return yield* Effect.die(new Error("Reconnect clock was not applied to the attempt"));
        return yield* observedScope(
          driver.connect(entry, (progress) =>
            Effect.sync(() => stamp(`progress:${progress.stage}`)).pipe(
              Effect.andThen(report(progress)),
            ),
          ),
          stampAttempt,
          "release",
        );
      });
    },
  });
  const route = (index: number) => {
    const target = new BearerConnectionTarget({
      environmentId: input.environmentId,
      label: "Reconnect benchmark",
      connectionId: `${input.environmentId}-route-${index}`,
    });
    return {
      target,
      profile: Option.some(
        new Catalog.BearerConnectionProfile({
          connectionId: target.connectionId,
          environmentId: target.environmentId,
          label: target.label,
          httpBaseUrl: index === 0 ? input.origin : input.origin.replace("127.0.0.1", "127.0.0.2"),
          wsBaseUrl: (index === 0
            ? input.origin
            : input.origin.replace("127.0.0.1", "127.0.0.2")
          ).replace(/^http/, "ws"),
        }),
      ),
    };
  };
  const network = yield* SubscriptionRef.make<NetworkStatus>("offline");
  const wakeups = yield* PubSub.unbounded<Wakeups.ConnectionWakeup>();
  const connectivity = Connectivity.Connectivity.of({
    status: SubscriptionRef.get(network),
    changes: SubscriptionRef.changes(network),
  });
  const wakeupService = Wakeups.ConnectionWakeups.of({ changes: Stream.fromPubSub(wakeups) });
  const supervisor = yield* Supervisor.make(
    {
      ...route(0),
      enabled: true,
      ...(input.entry.routes === 2 ? { alternateRoutes: [route(1)] } : {}),
    },
    { initiallyDesired: true },
  ).pipe(
    Effect.provideService(Driver.ConnectionDriver, measuredDriver),
    Effect.provideService(Clock.Clock, clock),
    Effect.provideService(Connectivity.Connectivity, connectivity),
    Effect.provideService(Wakeups.ConnectionWakeups, wakeupService),
  );
  let cachedShell: OrchestrationV2ShellSnapshot | undefined;
  const cacheStore = {
    helloSupport,
    loadShell: () => Effect.sync(() => Option.fromUndefinedOr(cachedShell)),
    saveShell: (_id, snapshot) =>
      Effect.sync(() => {
        cachedShell = snapshot;
      }),
    loadThread: () => Effect.succeed(Option.fromUndefinedOr(input.initial.initialSnapshot)),
    saveThread: () => Effect.void,
    removeThread: () => Effect.void,
  } satisfies Partial<Persistence.EnvironmentCacheStore["Service"]> & {
    helloSupport: typeof helloSupport;
  };
  track("shell-thread-cache", cacheStore);
  const cache = Layer.mock(Persistence.EnvironmentCacheStore)(cacheStore);
  const threadContext = yield* Layer.buildWithMemoMap(
    Layer.mergeAll(cache, BoundedSnapshot.layer, History.layer, ShellSnapshot.layer).pipe(
      Layer.provideMerge(authorization),
    ),
    memoMap,
    caseScope,
  );
  const state = yield* makeEnvironmentThreadState(operationThreadId).pipe(
    Effect.provide(threadContext),
    Effect.provideService(Supervisor.EnvironmentSupervisor, supervisor),
    Effect.provideService(Wakeups.ConnectionWakeups, wakeupService),
  );
  const shell = yield* makeEnvironmentShellState().pipe(
    Effect.provide(threadContext),
    Effect.provideService(Supervisor.EnvironmentSupervisor, supervisor),
    Effect.provideService(Wakeups.ConnectionWakeups, wakeupService),
  );
  const waitShell = SubscriptionRef.changes(shell).pipe(
    Stream.filter((s) => s.status === "live"),
    Stream.runHead,
    Effect.map(Option.getOrThrow),
  );
  const cached = yield* waitForOperationState(state, input.initial, false, "cached");
  const cacheError = operationStateError(cached, input.initial, false, "cached");
  if (cacheError) return yield* Effect.die(new Error(cacheError));
  if (sockets || Object.values(requests).some((count) => count > 0))
    return yield* Effect.die(new Error("Offline cache setup performed network I/O"));
  if (plan.establishOld) {
    yield* SubscriptionRef.set(network, "online");
    yield* waitForOperationState(state, input.initial, false);
    yield* waitShell;
    if (!activeSocket || activeSocket.readyState !== NodeWebSocket.OPEN || !activeSocketPort)
      return yield* Effect.die(new Error("Missing live old socket"));
    oldSocket = activeSocket;
    if (plan.oldSocket === "half-open" || plan.oldSocket === "closed")
      blackholeSocket(activeSocketPort);
    if (input.entry.workload === "healthy-slow")
      slowSocket(activeSocketPort, input.entry.slowSocketDelayMs ?? 1200, input.queuePackets);
  }
  wallOffsetMs = plan.warm ? plan.simulatedBackgroundMs : 0;
  if (plan.warm) {
    // A simulated three-minute absence must not retain a millisecond-old HTTP pool.
    yield* Effect.promise(() => pool.destroy());
    pool = yield* Effect.sync(makePool);
    requestInit.dispatcher = pool;
  }
  if (input.entry.workload === "preferred-silent") blackholePreferredRoute();
  const helloSupportCachedAtStart = helloKeys.has(
    helloKey(input.environmentId, input.origin, input.origin.replace(/^http/, "ws")),
  );
  const expected = yield* input.advance();
  // Cold long-resume HTTP starts with empty pools. Short-resume retains its real warm pools.
  for (const key of Object.keys(requests)) delete requests[key];
  for (const key of Object.keys(rpcRequests)) delete rpcRequests[key];
  stages.length = 0;
  sockets = events = snapshots = 0;
  usesHelloHandshake = false;
  sequence = input.initial.sequence;
  sequenceError = false;
  const before = linkCounters();
  const shellDone = yield* Deferred.make<number>();
  const applied = yield* Effect.forkChild(
    Deferred.await(synchronized)
      .pipe(Effect.andThen(waitForOperationState(state, expected, false)))
      .pipe(Effect.tap(() => Effect.sync(() => stamp("applied")))),
  );
  observationStartMs = Number(process.hrtime.bigint()) / 1e6;
  start = performance.now();
  stamp(
    plan.warm
      ? "background-simulated"
      : input.entry.workload === "refocus"
        ? "inactive-without-background"
        : "setup-complete",
  );
  if (plan.oldSocket === "closed" && activeSocket)
    yield* Effect.promise(
      () =>
        new Promise<void>((resolve) => {
          activeSocket!.once("close", resolve);
          activeSocket!.terminate();
        }),
    );
  if (plan.preActive) {
    yield* SubscriptionRef.set(network, "online");
    yield* Deferred.await(preparationStarted);
    stamp("pre-active-preparation");
  }
  const oldSocketStateAtActive = oldSocket?.readyState ?? -1;
  if (
    plan.establishOld &&
    oldSocketStateAtActive !==
      (plan.oldSocket === "closed" ? NodeWebSocket.CLOSED : NodeWebSocket.OPEN)
  )
    return yield* Effect.die(new Error("Incorrect old socket state at active"));
  const active = performance.now();
  const shift = active - start;
  // Keep observation objects: in-flight HTTP completions still write into them.
  for (const stage of stages)
    Object.assign(stage, {
      atMs: stage.atMs - shift,
      ...(stage.endMs === undefined ? {} : { endMs: stage.endMs - shift }),
    });
  start = active;
  measurementStartMs = Number(process.hrtime.bigint()) / 1e6;
  input.startController();
  stamp("active");
  yield* PubSub.publish(wakeups, plan.wakeup);
  if (!plan.establishOld && !plan.preActive) yield* SubscriptionRef.set(network, "online");
  // The shell may still show the old live state during the probe. Require a new subscription.
  yield* SubscriptionRef.changes(shell).pipe(
    Stream.filter(
      (s) => s.status === "live" && stages.some((s) => s.name === "shell-synchronized"),
    ),
    Stream.take(1),
    Stream.runForEach(() =>
      Effect.sync(() => stamp("shell-applied")).pipe(
        Effect.andThen(Deferred.succeed(shellDone, performance.now() - start)),
      ),
    ),
    Effect.forkScoped,
  );
  yield* Deferred.await(synchronized);
  const final = yield* Fiber.join(applied);
  const totalMs = performance.now() - start;
  const shellLiveMs = yield* Deferred.await(shellDone);
  const finalShell = yield* SubscriptionRef.get(shell);
  if (
    Option.isNone(finalShell.snapshot) ||
    finalShell.snapshot.value.projects.length !== 8 ||
    finalShell.snapshot.value.threads.length !== 160 ||
    new Set(finalShell.snapshot.value.threads.map((t) => t.id)).size !== 160 ||
    finalShell.snapshot.value.threads.find((t) => t.id === operationThreadId)?.latestRunId !==
      `operation-run-${Math.floor((expected.messages - 1) / 2)}` ||
    finalShell.snapshot.value.threads.find((t) => t.id === operationThreadId)?.title !==
      "Network operation"
  )
    return yield* Effect.die(new Error("Incorrect shell fixture after synchronization"));
  if (shellSnapshots)
    return yield* Effect.die(new Error("Unexpected socket shell snapshot fallback"));
  const after = linkCounters();
  const error = operationStateError(final, expected, false);
  if (
    error ||
    sequenceError ||
    snapshots ||
    events !== (plan.addedMessages ? 50 : 0) ||
    sequence !== expected.sequence ||
    (plan.keep
      ? sockets > 1
      : sockets < 1 ||
        sockets > (plan.preActive || input.entry.workload.startsWith("preferred-") ? 2 : 1))
  )
    return yield* Effect.die(
      new Error(
        error ??
          `Invalid replay: sequenceError=${sequenceError} snapshots=${snapshots} events=${events} sequence=${sequence} sockets=${sockets}`,
      ),
    );
  const requiredStages =
    plan.keep && sockets === 0
      ? ["probe-request", "subscribe", "synchronized", "applied"]
      : [
          ...(usesHelloHandshake ? ["hello-received", "auth-sent", "authenticated"] : []),
          "socket-start",
          "socket-open",
          "config-request",
          "config",
          "subscribe",
          "synchronized",
          "applied",
        ];
  if (
    requiredStages.some((name) => !stages.some((stage) => stage.name === name)) ||
    stages.filter((s) => s.name.startsWith("descriptor-")).length !==
      (requests["/.well-known/t3/environment"] ?? 0) ||
    stages.filter((s) => s.name === "ticket").length !==
      (requests["/api/auth/websocket-ticket"] ?? 0)
  )
    return yield* Effect.die(new Error("Incomplete reconnect stage observations"));
  return {
    measurementStartMs,
    observationStartMs,
    totalMs,
    shellLiveMs,
    switches,
    usesHelloHandshake,
    helloSupportCachedAtStart,
    simulatedBackgroundMs: plan.simulatedBackgroundMs,
    backgroundHttpPoolExpired: plan.warm,
    oldSocketStateAtActive,
    clientPeakSockets: peakSockets,
    falseReplacements: plan.keep ? sockets : 0,
    extraRequests: plan.keep
      ? [...Object.values(requests), ...Object.values(rpcRequests)].reduce((a, b) => a + b, 0)
      : 0,
    isolation: {
      httpPoolFresh: true,
      shellCacheEmpty: true,
      helloSeeded: switches.helloSupportCached,
      socketsBefore: 0,
      resources,
    },
    stages,
    requests,
    rpcRequests,
    bytes: after.bytes - before.bytes,
    packets: after.packets - before.packets,
    events,
    shellSnapshots,
    sockets,
    correct: true,
  };
});
