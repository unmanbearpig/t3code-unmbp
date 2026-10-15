// @effect-diagnostics nodeBuiltinImport:off globalTimers:off deterministicKeys:off - Bench clocks and socket observations must not add RPC round trips.
import * as NodeFS from "node:fs";
import type { OrchestrationV2ShellSnapshot } from "@t3tools/contracts";
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
import type { ReliableConnector } from "@t3tools/shared/rpcWebTransport";
import { RpcWebTransportConnector } from "../../../../packages/client-runtime/src/platform/rpcWebTransport.ts";
import { observeRpcSessionFactory } from "./rpcObservation.ts";
import type { NativeConnectionEvidence } from "./productionConnector.ts";
import * as Session from "../../../../packages/client-runtime/src/rpc/session.ts";
import * as BoundedSnapshot from "../../../../packages/client-runtime/src/state/boundedThreadSnapshotHttp.ts";
import * as History from "../../../../packages/client-runtime/src/state/threadHistoryController.ts";
import * as ShellSnapshot from "../../../../packages/client-runtime/src/state/shellSnapshotHttp.ts";
import { makeEnvironmentShellState } from "../../../../packages/client-runtime/src/state/shell.ts";
import { blackholeSocket } from "./reconnectNetwork.ts";
import { makeEnvironmentThreadState } from "../../../../packages/client-runtime/src/state/threads.ts";
import {
  operationServerConfig,
  operationThreadId,
  type OperationControlResponse,
} from "./operationsFixture.ts";
import { observeHttpClient } from "./reconnectHttp.ts";
import { operationStateError, waitForOperationState } from "./operationState.ts";
import type { ReconnectCase } from "./reconnectManifest.ts";

export interface Stage {
  readonly name: string;
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
/** Every measured service below is production code; only platform persistence and unused capabilities are fixed. */
export const runReconnect = Effect.fnUntraced(function* (input: {
  readonly entry: ReconnectCase;
  readonly origin: string;
  readonly bearer: string;
  readonly cert: string;
  readonly initial: OperationControlResponse;
  readonly advance: () => Effect.Effect<OperationControlResponse, Error>;
  readonly startController: () => void;
  readonly connector?: ReliableConnector;
  readonly nativeEvidence?: NativeConnectionEvidence;
  readonly requestedTransport: "websocket" | "webtransport";
}) {
  let start = 0;
  let measurementStartMs = 0;
  const stages: Stage[] = [];
  const requests: Record<string, number> = {};
  let sockets = 0;
  let selectedTransport: Session.RpcSession["transport"];
  const selections: { transport: Session.RpcSession["transport"]; atMs: number }[] = [];
  let events = 0;
  let snapshots = 0;
  let sequence = input.initial.sequence;
  let sequenceError = false;
  const synchronized = yield* Deferred.make<void>();
  let activeSocket: NodeWebSocket | undefined;
  let activeSocketPort: number | undefined;
  const isShort =
    input.entry.workload === "short-probe" || input.entry.workload === "network-changed";
  const stamp = (name: string) => {
    if (start) stages.push({ name, atMs: performance.now() - start });
  };
  const observation = {
    session: (
      event: "start" | "selected" | "ready",
      transport?: Session.RpcSession["transport"],
    ) => {
      stamp(`session-${event}`);
      if (event === "selected") {
        selectedTransport = transport;
        if (start) selections.push({ transport, atMs: performance.now() - start });
      }
    },
    request: (tag: string) => {
      if (tag === "orchestration.subscribeShell") stamp("shell-subscribe");
      if (tag === "session-probe") stamp("probe-request");
      if (tag === "orchestration.subscribeThread") stamp("subscribe");
    },
    value: (tag: string, value: unknown) => {
      if (tag === "initialConfig" || tag === "subscribeServerConfig") stamp("config");
      if (!Predicate.hasProperty(value, "kind")) return;
      if (tag === "orchestration.subscribeShell") {
        if (value.kind === "synchronized") stamp("shell-synchronized");
        return;
      }
      if (!start || tag !== "orchestration.subscribeThread") return;
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
    },
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
    return socket;
  };
  const httpBase = yield* HttpClient.HttpClient.pipe(Effect.provide(FetchHttpClient.layer));
  const http = observeHttpClient(httpBase, stages, requests, () => performance.now() - start);
  const unused = Effect.die(new Error("Unrelated platform service used by reconnect"));
  const platform = Layer.mergeAll(
    Layer.succeed(HttpClient.HttpClient, http),
    ...(input.connector ? [Layer.succeed(RpcWebTransportConnector, input.connector)] : []),
    Layer.succeed(Socket.WebSocketConstructor, socketConstructor),
    Layer.succeed(Capabilities.ClientPresentation, { metadata: { surface: "mobile" } }),
    Layer.mock(Capabilities.CloudSession)({ identity: Effect.succeedNone }),
    Layer.mock(Capabilities.RelayDeviceIdentity)({ deviceId: Effect.succeedNone }),
    Layer.mock(Capabilities.PrimaryEnvironmentAuth)({ bearerToken: Effect.succeedNone }),
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
  const connectionLayers = Driver.layer.pipe(
    Layer.provide(Resolver.layer),
    Layer.provide(
      Layer.effect(
        Session.RpcSessionFactory,
        Effect.map(Session.make({}), (factory) => observeRpcSessionFactory(factory, observation)),
      ),
    ),
    Layer.provide(authorization),
  );
  const context = yield* Layer.build(connectionLayers);
  const driver = Context.get(context, Driver.ConnectionDriver);
  const measuredDriver = Driver.ConnectionDriver.of({
    ...driver,
    connect: (entry, report) =>
      driver.connect(entry, (progress) =>
        Effect.sync(() => stamp(`progress:${progress.stage}`)).pipe(
          Effect.andThen(report(progress)),
        ),
      ),
  });
  const route = (index: number) => {
    const target = new BearerConnectionTarget({
      environmentId: operationServerConfig.environment.environmentId,
      label: "Reconnect benchmark",
      connectionId: `bench-route-${index}`,
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
    Effect.provideService(Connectivity.Connectivity, connectivity),
    Effect.provideService(Wakeups.ConnectionWakeups, wakeupService),
  );
  let cachedShell: OrchestrationV2ShellSnapshot | undefined;
  const cache = Layer.mock(Persistence.EnvironmentCacheStore)({
    loadShell: () => Effect.sync(() => Option.fromUndefinedOr(cachedShell)),
    saveShell: (_id, snapshot) =>
      Effect.sync(() => {
        cachedShell = snapshot;
      }),
    loadThread: () => Effect.succeed(Option.fromUndefinedOr(input.initial.initialSnapshot)),
    saveThread: () => Effect.void,
    removeThread: () => Effect.void,
  });
  const threadContext = yield* Layer.build(
    Layer.mergeAll(cache, BoundedSnapshot.layer, History.layer, ShellSnapshot.layer).pipe(
      Layer.provideMerge(authorization),
    ),
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
  if (isShort) {
    yield* SubscriptionRef.set(network, "online");
    yield* waitForOperationState(state, input.initial, false);
    yield* waitShell;
    const old = activeSocket;
    if (!old || old.readyState !== NodeWebSocket.OPEN || !activeSocketPort)
      return yield* Effect.die(new Error("Missing live socket before short resume"));
    blackholeSocket(activeSocketPort!);
    if (old.readyState !== NodeWebSocket.OPEN)
      return yield* Effect.die(new Error("Socket closed before wakeup"));
  }
  const expected = yield* input.advance();
  // Cold long-resume HTTP starts with empty pools. Short-resume retains its real warm pools.
  for (const key of Object.keys(requests)) delete requests[key];
  stages.length = 0;
  sockets = events = snapshots = 0;
  sequence = input.initial.sequence;
  sequenceError = false;
  const nativeBefore = input.nativeEvidence ? { ...input.nativeEvidence } : undefined;
  const before = linkCounters();
  const shellDone = yield* Deferred.make<number>();
  const applied = yield* Effect.forkChild(
    waitForOperationState(state, expected, false).pipe(
      Effect.tap(() => Effect.sync(() => stamp("applied"))),
    ),
  );
  input.startController();
  measurementStartMs = Number(process.hrtime.bigint()) / 1e6;
  start = performance.now();
  if (isShort)
    yield* PubSub.publish(
      wakeups,
      input.entry.workload === "short-probe" ? "application-active-probe" : "network-changed",
    );
  else {
    yield* PubSub.publish(wakeups, "application-active-reconnect");
    yield* SubscriptionRef.set(network, "online");
  }
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
  const after = linkCounters();
  const error = operationStateError(final, expected, false);
  if (
    error ||
    sequenceError ||
    snapshots ||
    events !== (input.entry.workload === "idle" ? 0 : 50) ||
    sequence !== expected.sequence ||
    selections.length !== 1 ||
    selectedTransport !== input.requestedTransport ||
    sockets !== (selectedTransport === "websocket" ? 1 : 0)
  )
    return yield* Effect.die(
      new Error(
        error ??
          `Invalid replay: sequenceError=${sequenceError} snapshots=${snapshots} events=${events} sequence=${sequence} sockets=${sockets} selected=${selectedTransport} requested=${input.requestedTransport}`,
      ),
    );
  const requiredStages = [
    "session-start",
    "session-selected",
    "session-ready",
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
    totalMs,
    shellLiveMs,
    selectedTransport: selectedTransport!,
    selections,
    nativeEvidence:
      input.nativeEvidence && nativeBefore
        ? {
            ...input.nativeEvidence,
            constructed: input.nativeEvidence.constructed - nativeBefore.constructed,
            connectReady: input.nativeEvidence.connectReady - nativeBefore.connectReady,
            controlStreamsOpened:
              input.nativeEvidence.controlStreamsOpened - nativeBefore.controlStreamsOpened,
            requestStreamsOpened:
              input.nativeEvidence.requestStreamsOpened - nativeBefore.requestStreamsOpened,
          }
        : null,
    stages,
    requests,
    bytes: after.bytes - before.bytes,
    packets: after.packets - before.packets,
    events,
    sockets,
    correct: true,
  };
});
