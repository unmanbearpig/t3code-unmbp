import * as Redacted from "effect/Redacted";
import { ORCHESTRATION_PROTOCOL_VERSION } from "@t3tools/contracts";
import { rpcWebTransportDefaultLimits } from "@t3tools/shared/rpcWebTransport";
import { memorySessionPair } from "../../../shared/src/rpcWebTransport.test-support.ts";
import { transportTry } from "@t3tools/shared/rpcWebTransport";
import { makeRpcWebTransportFrameChannel } from "@t3tools/shared/rpcWebTransport";
import { createTransportByteBudget } from "@t3tools/shared/rpcWebTransport";
import { decodeTransportEnvelope } from "@t3tools/shared/rpcWebTransport";
import { ClientRequestEnvelopeSchema } from "@t3tools/shared/rpcWebTransport";
import { type ReliableConnector } from "@t3tools/shared/rpcWebTransport";
import * as Clock from "effect/Clock";
import * as RpcHttp from "./http.ts";
import * as ClientCapabilities from "../platform/capabilities.ts";
import { RpcWebTransportConnector } from "../platform/rpcWebTransport.ts";
import { issueRemoteWebSocketTicket } from "../authorization/remote.ts";
import { EnvironmentAuthInvalidError } from "@t3tools/contracts";
import * as ConnectionDriver from "../connection/driver.ts";
import * as Connectivity from "../connection/connectivity.ts";
import * as ConnectionWakeups from "../connection/wakeups.ts";
import { mapRemoteEnvironmentError } from "../connection/errors.ts";
import * as TestClock from "effect/testing/TestClock";
import * as Scope from "effect/Scope";
import { subscribeDynamicWithSession } from "./client.ts";
import { ORCHESTRATION_V2_WS_METHODS } from "@t3tools/contracts";
import { EnvironmentRpcSubscriptionObserver } from "./client.ts";
import { type ReliableSession } from "@t3tools/shared/rpcWebTransport";
import { AttemptReconnectArm } from "../connection/reconnectExperiment.ts";

import {
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  ServerConfig,
  type ServerConfig as ServerConfigType,
  ServerConfigStreamEvent,
  type ServerConfigStreamEvent as ServerConfigStreamEventType,
  WS_METHODS,
  UsageLimitSourceId,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as Socket from "effect/socket/Socket";

import {
  AVAILABLE_CONNECTION_STATE,
  ConnectionBlockedError,
  ConnectionTransientError,
  PrimaryConnectionTarget,
  type PreparedConnection,
} from "../connection/model.ts";
import * as EnvironmentSupervisor from "../connection/supervisor.ts";
import * as Persistence from "../platform/persistence.ts";
import * as RpcSession from "./session.ts";
import { makeEnvironmentServerConfigState } from "../state/server.ts";
import { applyServerConfigProjection } from "../state/serverConfigProjection.ts";

type SocketEventType = "open" | "message" | "close" | "error";
type SocketEvent = {
  readonly code?: number;
  readonly data?: unknown;
  readonly reason?: string;
  readonly type: SocketEventType;
};
type SocketListener = (event: SocketEvent) => void;

class TestWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  readyState = TestWebSocket.CONNECTING;
  readonly sent: string[] = [];
  readonly url: string;
  readonly sentChanges: Queue.Queue<void>;
  deferClose = false;
  private readonly listeners = new Map<SocketEventType, Set<SocketListener>>();

  constructor(url: string, sentChanges: Queue.Queue<void>) {
    this.url = url;
    this.sentChanges = sentChanges;
  }

  addEventListener(type: SocketEventType, listener: SocketListener) {
    const listeners = this.listeners.get(type) ?? new Set<SocketListener>();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type: SocketEventType, listener: SocketListener) {
    this.listeners.get(type)?.delete(listener);
  }

  send(data: string) {
    if (this.readyState !== TestWebSocket.OPEN) throw new Error("Socket is not open.");
    this.sent.push(data);
    Queue.offerUnsafe(this.sentChanges, undefined);
  }

  get listenerCount() {
    return [...this.listeners.values()].reduce((sum, listeners) => sum + listeners.size, 0);
  }

  close(code = 1000, reason = "") {
    if (this.readyState === TestWebSocket.CLOSED) {
      return;
    }
    if (this.deferClose) {
      this.readyState = TestWebSocket.CLOSING;
      return;
    }
    this.readyState = TestWebSocket.CLOSED;
    this.emit("close", { code, reason, type: "close" });
  }

  open() {
    this.readyState = TestWebSocket.OPEN;
    this.emit("open", { type: "open" });
  }

  fail() {
    this.emit("error", { type: "error" });
    this.close(1006, "WebSocket upgrade failed");
  }

  serverMessage(data: string) {
    this.emit("message", { data, type: "message" });
  }

  private emit(type: SocketEventType, event: SocketEvent) {
    for (const listener of this.listeners.get(type) ?? []) {
      listener(event);
    }
  }
}

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
  socketUrl: "wss://environment.example.test/ws?wsTicket=test",
  httpAuthorization: null,
  target: TARGET,
};

const SERVER_CONFIG: ServerConfigType = {
  environment: {
    environmentId: TARGET.environmentId,
    label: TARGET.label,
    platform: {
      os: "darwin",
      arch: "arm64",
    },
    serverVersion: "0.0.0-test",
    capabilities: {
      repositoryIdentity: true,
      connectionProbe: true,
    },
  },
  auth: {
    policy: "loopback-browser",
    bootstrapMethods: ["one-time-token"],
    sessionMethods: ["browser-session-cookie", "bearer-access-token"],
    sessionCookieName: "t3_session",
  },
  cwd: "/tmp/workspace",
  keybindingsConfigPath: "/tmp/workspace/keybindings.json",
  keybindings: [],
  issues: [],
  providers: [],
  availableEditors: [],
  observability: {
    logsDirectoryPath: "/tmp/logs",
    localTracingEnabled: false,
    otlpTracesEnabled: false,
    otlpMetricsEnabled: false,
    otlpLogsEnabled: false,
  },
  settings: DEFAULT_SERVER_SETTINGS,
};

const RpcRequest = Schema.TaggedStruct("Request", {
  id: Schema.Union([Schema.String, Schema.Number]),
  payload: Schema.Unknown,
  tag: Schema.String,
});
const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const isRpcRequest = Schema.is(RpcRequest);
const encodeJson = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const encodeServerConfig = Schema.encodeSync(ServerConfig);
const encodeServerConfigStreamEvent = Schema.encodeSync(ServerConfigStreamEvent);
const encodeDefect = Schema.encodeSync(Schema.Defect());
const ENCODED_SERVER_CONFIG = encodeServerConfig(SERVER_CONFIG);
const THEME_SERVER_CONFIG: ServerConfigType = {
  ...SERVER_CONFIG,
  environment: {
    ...SERVER_CONFIG.environment,
    capabilities: {
      ...SERVER_CONFIG.environment.capabilities,
      environmentThemes: true,
    },
  },
};
const ENCODED_THEME_SERVER_CONFIG = encodeServerConfig(THEME_SERVER_CONFIG);
const SOURCE_SERVER_CONFIG: ServerConfigType = {
  ...THEME_SERVER_CONFIG,
  environment: {
    ...THEME_SERVER_CONFIG.environment,
    capabilities: { ...THEME_SERVER_CONFIG.environment.capabilities, usageLimitSources: true },
  },
};
const SOURCE_EVENT: ServerConfigStreamEventType = {
  version: 1,
  type: "usageLimitSourcesUpdated",
  payload: {
    sources: [
      {
        id: UsageLimitSourceId.make("proxy"),
        kind: "cliproxy",
        label: "Proxy",
        checkedAt: "2026-09-04T00:00:00Z",
        accounts: [],
      },
    ],
  },
};

const makeFactory = Effect.fn("TestRpcSessionFactory.make")(function* (
  options: RpcSession.RpcSessionOptions = {},
  connectorOrFetch?: ReliableConnector | typeof fetch,
  native?: { readonly fetch: typeof fetch },
) {
  const sentChanges = yield* Queue.unbounded<void>();
  const sockets = Object.assign([] as TestWebSocket[], { changes: yield* Queue.unbounded<void>() });
  const connector = typeof connectorOrFetch === "function" ? undefined : connectorOrFetch;
  const fetchFn: typeof fetch =
    typeof connectorOrFetch === "function"
      ? connectorOrFetch
      : () => Promise.reject(new Error("Unexpected HTTP request."));
  const liveSocketCounts: number[] = [];
  const createdAt: number[] = [];
  const clock = yield* Clock.Clock;
  const layerConstructor = Layer.succeed(Socket.WebSocketConstructor, (url) => {
    const socket = new TestWebSocket(url, sentChanges);
    sockets.push(socket);
    Queue.offerUnsafe(sockets.changes, undefined);
    liveSocketCounts.push(
      sockets.filter((candidate) => candidate.readyState < TestWebSocket.CLOSING).length,
    );
    createdAt.push(clock.currentTimeMillisUnsafe());
    return socket as unknown as globalThis.WebSocket;
  });
  const layer = RpcSession.layer(options).pipe(
    Layer.provide(
      Layer.mergeAll(
        layerConstructor,
        RpcHttp.layerRemoteHttpClient(native?.fetch ?? fetchFn),
        native === undefined
          ? Layer.empty
          : Layer.succeed(ClientCapabilities.WebSocketConstructorWithHeaders, (url) => {
              const socket = new TestWebSocket(url, sentChanges);
              sockets.push(socket);

              Queue.offerUnsafe(sockets.changes, undefined);
              return socket as unknown as globalThis.WebSocket;
            }),
        connector === undefined ? Layer.empty : Layer.succeed(RpcWebTransportConnector, connector),
      ),
    ),
  );
  const factory = yield* RpcSession.RpcSessionFactory.pipe(Effect.provide(layer));
  return { factory, sockets, liveSocketCounts, createdAt };
});

const BEARER_PREPARED: PreparedConnection = {
  ...PREPARED,
  socketUrl: "wss://environment.example.test/ws?orchestrationProtocol=2",
  httpAuthorization: { _tag: "Bearer", token: "paired-mobile-bearer" },
};

const makeHeaderFactory = Effect.fn("TestRpcSessionFactory.makeHeader")(function* (
  fetchFn: typeof fetch,
) {
  const sentChanges = yield* Queue.unbounded<void>();
  const sockets = Object.assign([] as TestWebSocket[], { changes: yield* Queue.unbounded<void>() });
  const { factory } = yield* makeFactory({}, fetchFn).pipe(
    Effect.provideService(ClientCapabilities.WebSocketConstructorWithHeaders, (url) => {
      const socket = new TestWebSocket(url, sentChanges);
      sockets.push(socket);

      Queue.offerUnsafe(sockets.changes, undefined);
      return socket as unknown as globalThis.WebSocket;
    }),
  );
  return { factory, sockets };
});

const awaitSocket = Effect.fn("TestRpcSessionFactory.awaitSocket")(function* (
  sockets: ReadonlyArray<TestWebSocket> & { readonly changes: Queue.Queue<void> },
  index = 0,
) {
  for (;;) {
    const socket = sockets[index];
    if (socket) {
      return socket;
    }
    yield* Queue.take(sockets.changes);
  }
});

const awaitRequest = Effect.fn("TestRpcSessionFactory.awaitRequest")(function* (
  socket: TestWebSocket,
  index = 0,
) {
  for (;;) {
    const request = socket.sent.map((message) => decodeJson(message)).filter(isRpcRequest)[index];
    if (request) {
      return request;
    }
    yield* Queue.take(socket.sentChanges);
  }
});

const completeInitialConfig = Effect.fn("TestRpcSessionFactory.completeInitialConfig")(function* (
  socket: TestWebSocket,
  config: unknown = ENCODED_SERVER_CONFIG,
  payload: unknown = {},
) {
  const request = yield* awaitRequest(socket);
  expect(request).toMatchObject({
    _tag: "Request",
    tag: WS_METHODS.subscribeServerConfig,
    payload,
  });
  socket.serverMessage(
    encodeJson({
      _tag: "Chunk",
      requestId: request.id,
      values: [{ version: 1, type: "snapshot", config }],
    }),
  );
});

const publishConfigEvents = Effect.fn("TestRpcSessionFactory.publishConfigEvents")(function* (
  socket: TestWebSocket,
  events: ReadonlyArray<ServerConfigStreamEventType>,
) {
  const request = yield* awaitRequest(socket);
  socket.serverMessage(
    encodeJson({
      _tag: "Chunk",
      requestId: request.id,
      values: events.map((event) => encodeServerConfigStreamEvent(event)),
    }),
  );
});

const QUIC_PREPARED: PreparedConnection = {
  ...PREPARED,
  rpcTicket: {
    value: Redacted.make("issued-rpc-ticket"),
    expiresAtEpochMs: Number.MAX_SAFE_INTEGER,
  },
  rpcWebTransport: {
    url: "https://environment.example.test:4443/rpc/custom",
    wireVersion: 1,
    orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
    clientMetadata: { surface: "web", deviceType: "desktop" },
  },
};

const quicReady = {
  _tag: "Ready",
  wireVersion: 1,
  orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
  environmentId: TARGET.environmentId,
  encoding: "deflate",
  limits: rpcWebTransportDefaultLimits,
};

const acceptQuicControl = Effect.fnUntraced(function* (pair: ReturnType<typeof memorySessionPair>) {
  const stream = yield* transportTry((signal) => pair.server.acceptBidirectionalStream(signal));
  const channel = makeRpcWebTransportFrameChannel(
    stream,
    createTransportByteBudget(65536),
    createTransportByteBudget(65536),
    rpcWebTransportDefaultLimits,
  );
  expect(yield* channel.readPreface()).toBe("control");
  expect(yield* channel.read({ first: true, control: true })).toMatchObject({ _tag: "Hello" });
  return channel;
});

const acceptQuicRequest = Effect.fnUntraced(function* (pair: ReturnType<typeof memorySessionPair>) {
  const stream = yield* transportTry((signal) => pair.server.acceptBidirectionalStream(signal));
  const channel = makeRpcWebTransportFrameChannel(
    stream,
    createTransportByteBudget(1024 * 1024),
    createTransportByteBudget(1024 * 1024),
    rpcWebTransportDefaultLimits,
  );
  expect(yield* channel.readPreface()).toBe("request");
  const request = yield* channel
    .read({ first: true })
    .pipe(Effect.flatMap((value) => decodeTransportEnvelope(ClientRequestEnvelopeSchema, value)));
  if (request._tag !== "Request") return yield* Effect.die(new Error("Expected request"));
  return { channel, request };
});

const completeQuicConfig = Effect.fnUntraced(function* (
  pair: ReturnType<typeof memorySessionPair>,
) {
  const { channel, request } = yield* acceptQuicRequest(pair);
  expect(request.tag).toBe(WS_METHODS.subscribeServerConfig);
  yield* channel.write({
    _tag: "Chunk",
    requestId: request.id,
    values: [{ version: 1, type: "snapshot", config: ENCODED_SERVER_CONFIG }],
  });
});

describe("RpcSessionFactory", () => {
  it.effect("blocks a revoked bearer with the ticket path's error and stops retrying", () =>
    Effect.gen(function* () {
      const requests: Request[] = [];
      const fetchFn = ((input, init) => {
        requests.push(new Request(input, init));
        return Promise.resolve(
          Response.json(
            {
              _tag: "EnvironmentAuthInvalidError",
              code: "auth_invalid",
              reason: "invalid_credential",
              traceId: "trace-revoked-bearer",
            },
            { status: 401 },
          ),
        );
      }) satisfies typeof fetch;
      const ticketError = yield* issueRemoteWebSocketTicket({
        httpBaseUrl: PREPARED.httpBaseUrl,
        bearerToken: "paired-mobile-bearer",
      }).pipe(Effect.provide(RpcHttp.layerRemoteHttpClient(fetchFn)), Effect.flip);
      expect(ticketError).toBeInstanceOf(EnvironmentAuthInvalidError);
      requests.length = 0;
      const { factory, sockets } = yield* makeHeaderFactory(fetchFn);
      const driver = ConnectionDriver.ConnectionDriver.of({
        connect: (_entry, reportProgress) =>
          Effect.gen(function* () {
            yield* reportProgress({ stage: "opening", prepared: BEARER_PREPARED });
            const session = yield* factory.connect(BEARER_PREPARED);
            yield* session.ready;
            return { prepared: BEARER_PREPARED, session };
          }),
        checkHost: () => Effect.succeed("unchecked" as const),
        preflight: () => Effect.succeed(false),
      });
      const supervisor = yield* EnvironmentSupervisor.make(
        { target: TARGET, enabled: true, profile: Option.none() },
        { initiallyDesired: true },
      ).pipe(
        Effect.provideService(ConnectionDriver.ConnectionDriver, driver),
        Effect.provideService(Connectivity.Connectivity, {
          status: Effect.succeed("online" as const),
          changes: Stream.never,
        }),
        Effect.provideService(ConnectionWakeups.ConnectionWakeups, { changes: Stream.never }),
      );
      const socket = yield* awaitSocket(sockets);
      socket.fail();
      const state = yield* SubscriptionRef.changes(supervisor.state).pipe(
        Stream.filter((state) => state.phase === "blocked"),
        Stream.runHead,
        Effect.map(Option.getOrThrow),
      );
      expect(state.lastFailure).toEqual(mapRemoteEnvironmentError(ticketError));
      expect(state.lastFailure).toMatchObject({ reason: "authentication" });
      expect(state.retryAt).toBeNull();
      yield* TestClock.adjust("10 minutes");
      expect(sockets).toHaveLength(1);
      expect(requests).toHaveLength(1);
      expect(requests[0]?.url).toBe(`${PREPARED.httpBaseUrl}/api/auth/websocket-ticket`);
      expect(requests[0]?.method).toBe("POST");
      expect(requests[0]?.headers.get("authorization")).toBe("Bearer paired-mobile-bearer");
    }).pipe(Effect.provide(TestClock.layer())),
  );

  it.effect("shares one credential rejection diagnostic across concurrent session consumers", () =>
    Effect.gen(function* () {
      let requests = 0;
      const { factory, sockets } = yield* makeHeaderFactory(() => {
        requests += 1;
        return Promise.resolve(
          Response.json(
            {
              _tag: "EnvironmentAuthInvalidError",
              code: "auth_invalid",
              reason: "invalid_credential",
              traceId: "trace-shared-diagnostic",
            },
            { status: 401 },
          ),
        );
      });
      const session = yield* factory.connect(BEARER_PREPARED);
      const failures = yield* Effect.forEach(
        [session.ready, session.initialConfig, session.closed],
        Effect.flip,
        { concurrency: "unbounded" },
      ).pipe(Effect.forkChild);
      const socket = yield* awaitSocket(sockets);
      socket.fail();
      expect(yield* Fiber.join(failures)).toEqual([
        expect.objectContaining({ _tag: "ConnectionBlockedError", reason: "authentication" }),
        expect.objectContaining({ _tag: "ConnectionBlockedError", reason: "authentication" }),
        expect.objectContaining({ _tag: "ConnectionBlockedError", reason: "authentication" }),
      ]);
      yield* session.ready.pipe(Effect.flip);
      yield* session.closed.pipe(Effect.flip);
      expect(requests).toBe(1);
      expect(sockets).toHaveLength(1);
    }),
  );

  it.effect.each(["network", "authorized", "compatible"] as const)(
    "keeps a failed bearer upgrade transient when its diagnostic is %s",
    (diagnostic) =>
      Effect.gen(function* () {
        let requests = 0;
        const { factory, sockets } = yield* makeHeaderFactory(() => {
          requests += 1;
          return diagnostic === "network"
            ? Promise.reject(new Error("Network unavailable"))
            : Promise.resolve(
                Response.json({
                  ticket: "unused-ticket",
                  expiresAt: "2026-10-15T12:00:00Z",
                  ...(diagnostic === "compatible"
                    ? { orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION }
                    : {}),
                }),
              );
        });
        for (let attempt = 0; attempt < 2; attempt += 1) {
          const session = yield* factory.connect(BEARER_PREPARED);
          const failure = yield* session.ready.pipe(Effect.flip, Effect.forkChild);
          const socket = yield* awaitSocket(sockets, attempt);
          socket.fail();
          const error = yield* Fiber.join(failure);
          expect(error).toBeInstanceOf(ConnectionTransientError);
          expect(error.reason).toBe(diagnostic === "network" ? "network" : "transport");
          yield* session.ready.pipe(Effect.flip);
          yield* session.closed.pipe(Effect.flip);
          expect(requests).toBe(attempt + 1);
          expect(sockets).toHaveLength(attempt + 1);
        }
      }),
  );

  it.effect.each([ORCHESTRATION_PROTOCOL_VERSION - 1, ORCHESTRATION_PROTOCOL_VERSION + 1])(
    "blocks an upgrade when its diagnostic reports protocol %s",
    (orchestrationProtocolVersion) =>
      Effect.gen(function* () {
        let requests = 0;
        const { factory, sockets } = yield* makeHeaderFactory(() => {
          requests += 1;
          return Promise.resolve(
            Response.json({
              ticket: "unused-ticket",
              expiresAt: "2026-10-15T12:00:00Z",
              orchestrationProtocolVersion,
            }),
          );
        });
        const session = yield* factory.connect(BEARER_PREPARED);
        const ready = yield* session.ready.pipe(Effect.flip, Effect.forkChild);
        const socket = yield* awaitSocket(sockets);
        socket.fail();
        expect(yield* Fiber.join(ready)).toMatchObject({
          _tag: "ConnectionBlockedError",
          reason: "unsupported",
        });
        yield* session.closed.pipe(Effect.flip);
        expect(requests).toBe(1);
        expect(sockets).toHaveLength(1);
      }),
  );

  it.effect("retains the single diagnostic when a readiness waiter is canceled", () =>
    Effect.gen(function* () {
      const started = yield* Queue.unbounded<void>();
      let requests = 0;
      let respond: ((response: Response) => void) | undefined;
      const { factory, sockets } = yield* makeHeaderFactory(() => {
        requests += 1;
        Queue.offerUnsafe(started, undefined);
        return new Promise<Response>((resolve) => {
          respond = resolve;
        });
      });
      const session = yield* factory.connect(BEARER_PREPARED);
      const ready = yield* session.ready.pipe(Effect.forkChild);
      const socket = yield* awaitSocket(sockets);
      socket.fail();
      yield* Queue.take(started);
      yield* Fiber.interrupt(ready);
      const closed = yield* session.closed.pipe(Effect.flip, Effect.forkChild);
      if (respond === undefined) return yield* Effect.die("Expected a diagnostic request.");
      respond(Response.json({ ticket: "unused-ticket", expiresAt: "2026-10-15T12:00:00Z" }));
      expect(yield* Fiber.join(closed)).toBeInstanceOf(ConnectionTransientError);
      expect(requests).toBe(1);
    }),
  );

  it.effect("preserves permission errors after a bearer upgrade without diagnostics", () =>
    Effect.gen(function* () {
      let requests = 0;
      const { factory, sockets } = yield* makeHeaderFactory(() => {
        requests += 1;
        return Promise.reject(new Error("No diagnostic expected."));
      });
      const session = yield* factory.connect(BEARER_PREPARED);
      const ready = yield* session.ready.pipe(Effect.flip, Effect.forkChild);
      const socket = yield* awaitSocket(sockets);
      socket.open();
      const request = yield* awaitRequest(socket);
      socket.serverMessage(
        encodeJson({
          _tag: "Exit",
          requestId: request.id,
          exit: {
            _tag: "Failure",
            cause: [
              {
                _tag: "Fail",
                error: {
                  _tag: "EnvironmentAuthorizationError",
                  message: "config subscription rejected",
                  requiredScope: "orchestration:read",
                },
              },
            ],
          },
        }),
      );
      expect(yield* Fiber.join(ready)).toMatchObject({
        _tag: "ConnectionBlockedError",
        reason: "permission",
      });
      expect(requests).toBe(0);
    }),
  );

  it.effect("bounds a hung diagnostic to three seconds and preserves a transient failure", () =>
    Effect.gen(function* () {
      const started = yield* Queue.unbounded<void>();
      const aborted = yield* Queue.unbounded<void>();
      let requests = 0;
      const { factory, sockets } = yield* makeHeaderFactory((_input, init) => {
        requests += 1;
        Queue.offerUnsafe(started, undefined);
        init?.signal?.addEventListener("abort", () => Queue.offerUnsafe(aborted, undefined));
        return new Promise<Response>(() => undefined);
      });
      const session = yield* factory.connect(BEARER_PREPARED);
      const ready = yield* session.ready.pipe(Effect.flip, Effect.forkChild);
      const socket = yield* awaitSocket(sockets);
      socket.fail();
      yield* Queue.take(started);
      yield* TestClock.adjust("3 seconds");
      expect(yield* Fiber.join(ready)).toMatchObject({
        _tag: "ConnectionTransientError",
        reason: "timeout",
      });
      yield* Queue.take(aborted);
      yield* session.ready.pipe(Effect.flip);
      expect(requests).toBe(1);
    }).pipe(Effect.provide(TestClock.layer())),
  );

  it.effect.each([true, false])(
    "makes no diagnostic request after an upgrade opens, initial config ready: %s",
    (configReady) =>
      Effect.gen(function* () {
        let requests = 0;
        const { factory, sockets } = yield* makeHeaderFactory(() => {
          requests += 1;
          return Promise.reject(new Error("No diagnostic expected."));
        });
        const session = yield* factory.connect(BEARER_PREPARED);
        const ready = yield* session.ready.pipe(Effect.exit, Effect.forkChild);
        const socket = yield* awaitSocket(sockets);
        socket.open();
        if (configReady) {
          yield* completeInitialConfig(socket);
          expect(Exit.isSuccess(yield* Fiber.join(ready))).toBe(true);
        }
        expect(requests).toBe(0);
        socket.fail();
        expect(yield* session.closed.pipe(Effect.flip)).toBeInstanceOf(ConnectionTransientError);
        if (!configReady) expect(Exit.isFailure(yield* Fiber.join(ready))).toBe(true);
        expect(requests).toBe(0);
      }),
  );

  it.effect("sends bearer credentials through the platform's upgrade header constructor", () =>
    Effect.gen(function* () {
      const sentChanges = yield* Queue.unbounded<void>();
      const sockets = Object.assign([] as TestWebSocket[], {
        changes: yield* Queue.unbounded<void>(),
      });
      const upgradeHeaders: Readonly<Record<string, string>>[] = [];
      const { factory, sockets: defaultSockets } = yield* makeFactory().pipe(
        Effect.provideService(
          ClientCapabilities.WebSocketConstructorWithHeaders,
          (url, headers) => {
            const socket = new TestWebSocket(url, sentChanges);
            sockets.push(socket);

            Queue.offerUnsafe(sockets.changes, undefined);
            upgradeHeaders.push(headers);
            return socket as unknown as globalThis.WebSocket;
          },
        ),
      );
      const socketUrl = "wss://environment.example.test/ws?orchestrationProtocol=2";
      const session = yield* factory.connect({
        ...PREPARED,
        socketUrl,
        httpAuthorization: { _tag: "Bearer", token: "secret-bearer-token" },
      });
      const readyFiber = yield* Effect.forkChild(session.ready);
      const socket = yield* awaitSocket(sockets);
      expect(socket.url).toBe(socketUrl);
      expect(upgradeHeaders).toEqual([{ Authorization: "Bearer secret-bearer-token" }]);
      expect(defaultSockets).toHaveLength(0);

      socket.open();
      yield* completeInitialConfig(socket);
      yield* Fiber.join(readyFiber);
    }),
  );

  it.effect("keeps DPoP sockets on the ticket constructor when upgrade headers are available", () =>
    Effect.gen(function* () {
      const { factory, sockets } = yield* makeFactory().pipe(
        Effect.provideService(ClientCapabilities.WebSocketConstructorWithHeaders, () => {
          throw new Error("DPoP must use its ticket.");
        }),
      );
      const session = yield* factory.connect({
        ...PREPARED,
        httpAuthorization: { _tag: "Dpop", accessToken: "dpop-token", expiresAtEpochMs: 100_000 },
      });
      const readyFiber = yield* Effect.forkChild(session.ready);
      const socket = yield* awaitSocket(sockets);
      expect(socket.url).toBe(PREPARED.socketUrl);
      socket.open();
      yield* completeInitialConfig(socket);
      yield* Fiber.join(readyFiber);
    }),
  );

  it.effect("owns one scoped websocket attempt and exposes readiness and closure", () =>
    Effect.gen(function* () {
      const { factory, sockets } = yield* makeFactory();
      const session = yield* factory.connect(PREPARED);
      const readyFiber = yield* Effect.forkChild(session.ready);
      const socket = yield* awaitSocket(sockets);

      expect(socket.url).toBe(PREPARED.socketUrl);
      socket.open();
      yield* completeInitialConfig(socket);
      yield* Fiber.join(readyFiber);

      const config = yield* session.initialConfig;
      expect(config).toEqual(SERVER_CONFIG);
      expect(socket.sent.map((message) => decodeJson(message)).filter(isRpcRequest)).toHaveLength(
        1,
      );

      const probeFiber = yield* Effect.forkChild(session.probe);
      const probeRequest = yield* awaitRequest(socket, 1);
      expect(probeRequest).toMatchObject({
        _tag: "Request",
        tag: WS_METHODS.serverProbe,
        payload: {},
      });
      socket.serverMessage(
        encodeJson({
          _tag: "Exit",
          requestId: probeRequest.id,
          exit: {
            _tag: "Success",
            value: {},
          },
        }),
      );
      yield* Fiber.join(probeFiber);

      expect(
        socket.sent
          .map((message) => decodeJson(message))
          .filter(isRpcRequest)
          .map((request) => request.tag),
      ).toEqual([WS_METHODS.subscribeServerConfig, WS_METHODS.serverProbe]);

      socket.close(1012, "service restart");
      const error = yield* Effect.flip(session.closed);
      const configStreamError = yield* session
        .subscribeServerConfig({})
        .pipe(Stream.runDrain, Effect.flip);

      expect(error).toBeInstanceOf(ConnectionTransientError);
      expect(error).toMatchObject({
        reason: "transport",
        message: "Test environment disconnected.",
      });
      expect(configStreamError).toMatchObject({ _tag: "RpcClientError" });
      yield* Effect.yieldNow;
      expect(sockets).toHaveLength(1);
    }),
  );

  it.effect("counts every inbound frame, including ones that answer no request", () =>
    Effect.gen(function* () {
      const { factory, sockets } = yield* makeFactory();
      const session = yield* factory.connect(PREPARED);
      const readyFiber = yield* Effect.forkChild(session.ready);
      const socket = yield* awaitSocket(sockets);
      socket.open();
      yield* completeInitialConfig(socket);
      yield* Fiber.join(readyFiber);
      const before = yield* session.inboundFrames;

      socket.serverMessage(encodeJson({ _tag: "Pong" }));

      expect(yield* session.inboundFrames).toBe(before + 1);
    }),
  );

  it.effect("closes the websocket when the session scope is released", () =>
    Effect.gen(function* () {
      const { factory, sockets } = yield* makeFactory();

      yield* Effect.scoped(
        Effect.gen(function* () {
          const session = yield* factory.connect(PREPARED);
          const readyFiber = yield* Effect.forkChild(session.ready);
          const socket = yield* awaitSocket(sockets);
          socket.open();
          yield* completeInitialConfig(socket);
          yield* Fiber.join(readyFiber);
        }),
      );

      expect(sockets[0]?.readyState).toBe(TestWebSocket.CLOSED);
    }),
  );

  it.effect("finishes an interrupt already waiting to write when the socket closes", () =>
    Effect.gen(function* () {
      const { factory, sockets } = yield* makeFactory();
      const session = yield* factory.connect(PREPARED);
      const socket = yield* awaitSocket(sockets);
      const stream = yield* session.client[WS_METHODS.subscribeServerLifecycle]({}).pipe(
        Stream.runDrain,
        Effect.forkChild,
      );
      yield* TestClock.adjust(0);
      const canceling = yield* Effect.forkChild(Fiber.interrupt(stream));
      yield* TestClock.adjust(0);
      expect(canceling.pollUnsafe()).toBeUndefined();

      socket.close(1006, "network lost");
      yield* TestClock.adjust(0);
      const completedWithoutTimeout = canceling.pollUnsafe() !== undefined;
      // Let the unfixed client's 1 s interrupt timeout finish before asserting.
      yield* TestClock.adjust("1 second");
      yield* Fiber.join(canceling);
      expect(completedWithoutTimeout).toBe(true);
      expect(socket.sent).toEqual([]);
    }).pipe(Effect.provide(TestClock.layer())),
  );

  it.effect("releases an unopened session before the interrupt timeout", () =>
    Effect.gen(function* () {
      const { factory, sockets } = yield* makeFactory();
      const scope = yield* Scope.make();
      yield* factory.connect(PREPARED).pipe(Scope.provide(scope));
      const socket = yield* awaitSocket(sockets);
      yield* TestClock.adjust(0);
      const releasing = yield* Scope.close(scope, Exit.void).pipe(Effect.forkChild);
      yield* TestClock.adjust(0);
      const completedWithoutTimeout = releasing.pollUnsafe() !== undefined;
      yield* TestClock.adjust("1 second");
      yield* Fiber.join(releasing);
      expect(completedWithoutTimeout).toBe(true);
      expect(socket.readyState).toBe(TestWebSocket.CLOSED);
      expect(socket.sent).toEqual([]);
    }).pipe(Effect.provide(TestClock.layer())),
  );

  it.effect("cancels a live stream over its healthy socket", () =>
    Effect.gen(function* () {
      const { factory, sockets } = yield* makeFactory();
      const session = yield* factory.connect(PREPARED);
      const socket = yield* awaitSocket(sockets);
      socket.open();
      yield* completeInitialConfig(socket);
      yield* session.ready;
      const consumer = yield* session.client[WS_METHODS.subscribeServerLifecycle]({}).pipe(
        Stream.runDrain,
        Effect.forkChild,
      );
      const request = yield* awaitRequest(socket, 1);
      yield* Fiber.interrupt(consumer);
      expect(socket.sent.map((message) => decodeJson(message))).toContainEqual({
        _tag: "Interrupt",
        requestId: request.id,
      });
      expect(socket.readyState).toBe(TestWebSocket.OPEN);
    }),
  );

  it.effect("starts a replacement before a stale unopened session's interrupt timeout", () =>
    Effect.gen(function* () {
      const { factory, sockets, liveSocketCounts, createdAt } = yield* makeFactory();
      const wakeups = yield* Queue.unbounded<ConnectionWakeups.ConnectionWakeup>();
      const owner = yield* Scope.fork(yield* Scope.Scope);
      yield* EnvironmentSupervisor.make(
        { target: TARGET, profile: Option.none(), enabled: true },
        {
          initiallyDesired: true,
        },
      ).pipe(
        Effect.provideService(ConnectionDriver.ConnectionDriver, {
          connect: () =>
            Effect.gen(function* () {
              const session = yield* factory.connect(PREPARED);
              yield* session.ready;
              return { prepared: PREPARED, session };
            }),
          checkHost: () => Effect.succeed("unchecked" as const),
          preflight: () => Effect.succeed(false),
        }),
        Effect.provideService(Connectivity.Connectivity, {
          status: Effect.succeed("online" as const),
          changes: Stream.never,
        }),
        Effect.provideService(ConnectionWakeups.ConnectionWakeups, {
          changes: Stream.fromQueue(wakeups),
        }),
        Scope.provide(owner),
      );
      const first = yield* awaitSocket(sockets);
      yield* TestClock.adjust("2 seconds");
      yield* Queue.offer(wakeups, "application-active-long");
      yield* TestClock.adjust(0);
      const releasedWithoutTimeout = first.readyState === TestWebSocket.CLOSED;
      // On upstream, teardown is still waiting for an interrupt to the old
      // unopened transport. Advance its deadline only to allow test cleanup.
      yield* TestClock.adjust("1 second");
      const second = yield* awaitSocket(sockets, 1);
      second.open();
      yield* completeInitialConfig(second);
      yield* Scope.close(owner, Exit.void);
      expect(releasedWithoutTimeout).toBe(true);
      expect(createdAt).toEqual([0, 2000]);
      expect(liveSocketCounts).toEqual([1, 1]);
      expect(sockets.every((socket) => socket.listenerCount === 0)).toBe(true);
    }).pipe(Effect.provide(TestClock.layer())),
  );

  it.effect.each(["closed before active", "half-open"] as const)(
    "replaces a %s socket and migrates a durable stream exactly once",
    (oldTransport) =>
      Effect.gen(function* () {
        const { factory, sockets, liveSocketCounts, createdAt } = yield* makeFactory();
        const wakeups = yield* Queue.unbounded<ConnectionWakeups.ConnectionWakeup>();
        const owner = yield* Scope.fork(yield* Scope.Scope);
        const driver = ConnectionDriver.ConnectionDriver.of({
          connect: (_entry, report) =>
            Effect.gen(function* () {
              yield* report({ stage: "opening", prepared: PREPARED });
              const session = yield* factory.connect(PREPARED);
              yield* report({ stage: "synchronizing", prepared: PREPARED });
              yield* session.ready;
              return { prepared: PREPARED, session };
            }),
          checkHost: () => Effect.succeed("unchecked" as const),
          preflight: () => Effect.succeed(false),
        });
        const supervisor = yield* EnvironmentSupervisor.make(
          {
            target: TARGET,
            profile: Option.none(),
            enabled: true,
          },
          { initiallyDesired: true },
        ).pipe(
          Effect.provideService(ConnectionDriver.ConnectionDriver, driver),
          Effect.provideService(Connectivity.Connectivity, {
            status: Effect.succeed("online" as const),
            changes: Stream.never,
          }),
          Effect.provideService(ConnectionWakeups.ConnectionWakeups, {
            changes: Stream.fromQueue(wakeups),
          }),
          Scope.provide(owner),
        );
        const first = yield* awaitSocket(sockets);
        first.open();
        yield* completeInitialConfig(first);
        yield* TestClock.adjust(0);

        const received = yield* Queue.unbounded<number>();
        let cursor = 0;
        let subscriptions = 0;
        let cancellations = 0;
        let activeSubscriptions = 0;
        let maxSubscriptions = 0;
        const consumer = yield* subscribeDynamicWithSession(
          ORCHESTRATION_V2_WS_METHODS.subscribeShell,
          () => Effect.succeed({ afterSequence: cursor }),
        ).pipe(
          Stream.runForEach(([, event]) => {
            if (!("sequence" in event)) return Effect.void;
            cursor = event.sequence;
            return Queue.offer(received, event.sequence).pipe(Effect.asVoid);
          }),
          Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
          Effect.provideService(EnvironmentRpcSubscriptionObserver, {
            observe: () =>
              Effect.sync(() => {
                subscriptions += 1;
                activeSubscriptions += 1;
                maxSubscriptions = Math.max(maxSubscriptions, activeSubscriptions);
                return Effect.sync(() => {
                  cancellations += 1;
                  activeSubscriptions -= 1;
                });
              }),
          }),
          Effect.forkChild,
        );
        const oldRequest = yield* awaitRequest(first, 1);
        expect(oldRequest).toMatchObject({
          tag: ORCHESTRATION_V2_WS_METHODS.subscribeShell,
          payload: { afterSequence: 0 },
        });
        const publish = (socket: TestWebSocket, requestId: string | number, sequences: number[]) =>
          socket.serverMessage(
            encodeJson({
              _tag: "Chunk",
              requestId,
              values: sequences.map((sequence) => ({
                kind: "project.removed",
                sequence,
                projectId: "project-1",
              })),
            }),
          );
        publish(first, oldRequest.id, [1]);
        expect(yield* Queue.take(received)).toBe(1);

        if (oldTransport === "closed before active") {
          first.close(1006, "closed during suspension");
          yield* TestClock.adjust("400 millis");
        } else if (oldTransport === "half-open") {
          // No frames or close event arrive. Native close starts a handshake
          // that need not finish before the next socket is constructed.
          first.deferClose = true;
        }
        const activeAt = yield* Clock.currentTimeMillis;
        yield* Queue.offer(wakeups, "application-active-long");
        if (oldTransport === "half-open") yield* TestClock.adjust("3 seconds");
        const second = yield* awaitSocket(sockets, 1);
        expect(first.readyState).toBe(
          oldTransport === "half-open" ? TestWebSocket.CLOSING : TestWebSocket.CLOSED,
        );
        expect(first.listenerCount).toBe(0);
        second.open();
        yield* completeInitialConfig(second);
        const newRequest = yield* awaitRequest(second, 1);
        expect(newRequest).toMatchObject({
          tag: ORCHESTRATION_V2_WS_METHODS.subscribeShell,
          payload: { afterSequence: 1 },
        });
        // A late frame from the retired generation cannot reach the consumer.
        publish(first, oldRequest.id, [999]);
        publish(second, newRequest.id, [2, 3]);
        expect(yield* Queue.take(received)).toBe(2);
        expect(yield* Queue.take(received)).toBe(3);
        yield* TestClock.adjust(0);
        expect(yield* Queue.poll(received)).toEqual(Option.none());
        expect(subscriptions).toBe(2);
        expect(cancellations).toBe(1);
        expect(maxSubscriptions).toBe(1);
        expect(liveSocketCounts).toEqual([1, 1]);
        expect(createdAt[1]).toBe(activeAt + (oldTransport === "half-open" ? 3000 : 0));
        for (const socket of sockets) {
          expect(
            socket.sent.map((message) => decodeJson(message)).filter(isRpcRequest),
          ).toHaveLength(2);
        }
        yield* Fiber.interrupt(consumer);
        yield* Scope.close(owner, Exit.void);
        expect(activeSubscriptions).toBe(0);
        expect(cancellations).toBe(2);
        expect(sockets.every((socket) => socket.listenerCount === 0)).toBe(true);
        const sendsAfterRelease = sockets.map((socket) => socket.sent.length);
        yield* TestClock.adjust("1 minute");
        expect(sockets.map((socket) => socket.sent.length)).toEqual(sendsAfterRelease);
        expect(sockets).toHaveLength(2);
      }).pipe(Effect.provide(TestClock.layer())),
  );

  it.effect("replays current config and broadcasts updates to every subscriber", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { factory, sockets } = yield* makeFactory();
        const session = yield* factory.connect(PREPARED);
        const readyFiber = yield* Effect.forkChild(session.ready);
        const socket = yield* awaitSocket(sockets);
        socket.open();
        yield* completeInitialConfig(socket);
        yield* Fiber.join(readyFiber);

        const collectTwo = session
          .subscribeServerConfig({})
          .pipe(Stream.take(2), Stream.runCollect);
        const firstSubscriber = yield* Effect.forkChild(collectTwo);
        const secondSubscriber = yield* Effect.forkChild(collectTwo);
        yield* Effect.yieldNow;

        const shortcut = {
          key: "k",
          metaKey: false,
          ctrlKey: false,
          shiftKey: false,
          altKey: false,
          modKey: true,
        };
        const request = yield* awaitRequest(socket);
        socket.serverMessage(
          encodeJson({
            _tag: "Chunk",
            requestId: request.id,
            values: [
              {
                version: 1,
                type: "keybindingsUpdated",
                payload: {
                  keybindings: [{ command: "terminal.toggle", shortcut }],
                  issues: [],
                },
              },
            ],
          }),
        );

        const firstEvents = Array.from(yield* Fiber.join(firstSubscriber));
        const secondEvents = Array.from(yield* Fiber.join(secondSubscriber));
        expect(firstEvents.map((event) => event.type)).toEqual(["snapshot", "keybindingsUpdated"]);
        expect(secondEvents).toEqual(firstEvents);

        const replay = yield* session.subscribeServerConfig({}).pipe(Stream.runHead);
        expect(replay).toMatchObject({
          _tag: "Some",
          value: {
            type: "snapshot",
            config: { keybindings: [{ command: "terminal.toggle", shortcut }] },
          },
        });
      }),
    ),
  );

  it.effect.each([
    { usageLimitSources: true },
    { environmentThemes: true, usageLimitSources: true },
  ])("replays usage sources and removal with %j", (options) =>
    Effect.scoped(
      Effect.gen(function* () {
        const { factory, sockets } = yield* makeFactory(options);
        const session = yield* factory.connect(PREPARED);
        const ready = yield* Effect.forkChild(session.ready);
        const socket = yield* awaitSocket(sockets);
        socket.open();
        yield* completeInitialConfig(socket, encodeServerConfig(SOURCE_SERVER_CONFIG), options);
        yield* Fiber.join(ready);
        const observed = yield* Queue.unbounded<ServerConfigStreamEventType>();
        yield* session.subscribeServerConfig(options).pipe(
          Stream.runForEach((event) => Queue.offer(observed, event)),
          Effect.forkChild,
        );
        expect((yield* Queue.take(observed)).type).toBe("snapshot");
        const themes: ServerConfigStreamEventType[] = options.environmentThemes
          ? [{ version: 1, type: "environmentThemesUpdated", payload: { themes: [] } }]
          : [];
        for (const event of themes) {
          yield* publishConfigEvents(socket, [event]);
          expect(yield* Queue.take(observed)).toEqual(event);
        }
        const events: ServerConfigStreamEventType[] = [
          SOURCE_EVENT,
          { version: 1, type: "usageLimitSourcesUpdated", payload: { sources: [] } },
          SOURCE_EVENT,
        ];
        for (const event of events) {
          yield* publishConfigEvents(socket, [event]);
          expect(yield* Queue.take(observed)).toEqual(event);
          const started = yield* Deferred.make<void>();
          const replay = yield* session.subscribeServerConfig(options).pipe(
            Stream.tap(() => Deferred.succeed(started, undefined)),
            Stream.takeUntil((item) => item.type === "keybindingsUpdated"),
            Stream.runCollect,
            Effect.forkChild,
          );
          yield* Deferred.await(started);
          const marker: ServerConfigStreamEventType = {
            version: 1,
            type: "keybindingsUpdated",
            payload: { keybindings: [], issues: [] },
          };
          yield* publishConfigEvents(socket, [marker]);
          expect(yield* Queue.take(observed)).toEqual(marker);
          const replayed = Array.from(yield* Fiber.join(replay));
          expect(replayed.slice(1)).toEqual([...themes, event, marker]);
          let projection = applyServerConfigProjection(Option.none(), {
            version: 1,
            type: "snapshot",
            config: SOURCE_SERVER_CONFIG,
          });
          projection = applyServerConfigProjection(projection, SOURCE_EVENT);
          for (const item of replayed) projection = applyServerConfigProjection(projection, item);
          expect(Option.getOrThrow(projection).config.usageLimitSources).toEqual(
            event.type === "usageLimitSourcesUpdated" && event.payload.sources.length > 0
              ? event.payload.sources
              : undefined,
          );
        }
        expect(socket.sent.map((message) => decodeJson(message)).filter(isRpcRequest)).toHaveLength(
          1,
        );
      }),
    ),
  );

  it.effect("replays theme updates and deletion as authoritative events", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { factory, sockets } = yield* makeFactory({ environmentThemes: true });
        const session = yield* factory.connect(PREPARED);
        const readyFiber = yield* Effect.forkChild(session.ready);
        const socket = yield* awaitSocket(sockets);
        socket.open();
        yield* completeInitialConfig(socket, ENCODED_THEME_SERVER_CONFIG, {
          environmentThemes: true,
        });
        yield* Fiber.join(readyFiber);

        const firstThemes = [
          {
            id: "nightfall",
            name: "Nightfall",
            appearance: "dark" as const,
            canvas: "#1a1b26",
            accent: "#7aa2f7",
          },
        ];
        const replacementThemes = [
          {
            id: "midnight",
            name: "Midnight",
            appearance: "dark" as const,
            canvas: "#000000",
            accent: "#ffffff",
          },
        ];
        const subscriberStarted = yield* Deferred.make<void>();
        const subscriber = yield* session.subscribeServerConfig({ environmentThemes: true }).pipe(
          Stream.mapEffect((event) =>
            Deferred.succeed(subscriberStarted, undefined).pipe(Effect.as(event)),
          ),
          Stream.take(4),
          Stream.runCollect,
          Effect.forkChild,
        );
        yield* Deferred.await(subscriberStarted);
        yield* publishConfigEvents(socket, [
          {
            version: 1,
            type: "environmentThemesUpdated",
            payload: { themes: firstThemes },
          },
          {
            version: 1,
            type: "environmentThemesUpdated",
            payload: { themes: replacementThemes },
          },
          {
            version: 1,
            type: "environmentThemesUpdated",
            payload: { themes: [] },
          },
        ]);

        const liveEvents = Array.from(yield* Fiber.join(subscriber));
        expect(liveEvents.map((event) => event.type)).toEqual([
          "snapshot",
          "environmentThemesUpdated",
          "environmentThemesUpdated",
          "environmentThemesUpdated",
        ]);
        expect(liveEvents[2]).toMatchObject({ payload: { themes: replacementThemes } });

        const replay = Array.from(
          yield* session
            .subscribeServerConfig({ environmentThemes: true })
            .pipe(Stream.take(2), Stream.runCollect),
        );
        expect(replay.map((event) => event.type)).toEqual(["snapshot", "environmentThemesUpdated"]);
        expect(replay[1]).toMatchObject({ payload: { themes: [] } });

        let projection = applyServerConfigProjection(Option.none(), {
          version: 1,
          type: "snapshot",
          config: THEME_SERVER_CONFIG,
        });
        projection = applyServerConfigProjection(projection, {
          version: 1,
          type: "environmentThemesUpdated",
          payload: { themes: firstThemes },
        });
        for (const event of replay) {
          projection = applyServerConfigProjection(projection, event);
        }
        expect(Option.getOrThrow(projection).config.environmentThemes).toBeUndefined();
      }),
    ),
  );

  it.effect("recovers a slow subscriber after it misses theme and usage-source deletion", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { factory, sockets } = yield* makeFactory({
          environmentThemes: true,
          usageLimitSources: true,
        });
        const session = yield* factory.connect(PREPARED);
        const readyFiber = yield* Effect.forkChild(session.ready);
        const socket = yield* awaitSocket(sockets);
        socket.open();
        yield* completeInitialConfig(socket, encodeServerConfig(SOURCE_SERVER_CONFIG), {
          environmentThemes: true,
          usageLimitSources: true,
        });
        yield* Fiber.join(readyFiber);

        const slowSubscriberStarted = yield* Deferred.make<void>();
        const releaseSlowSubscriber = yield* Deferred.make<void>();
        let firstEvent = true;
        const slowSubscriber = yield* session
          .subscribeServerConfig({ environmentThemes: true, usageLimitSources: true })
          .pipe(
            Stream.mapEffect((event) => {
              if (!firstEvent) return Effect.succeed(event);
              firstEvent = false;
              return Deferred.succeed(slowSubscriberStarted, undefined).pipe(
                Effect.andThen(Deferred.await(releaseSlowSubscriber)),
                Effect.as(event),
              );
            }),
            Stream.take(4),
            Stream.runCollect,
            Effect.forkChild,
          );
        yield* Deferred.await(slowSubscriberStarted);

        const firstThemes = [
          {
            id: "nightfall",
            name: "Nightfall",
            appearance: "dark" as const,
            canvas: "#1a1b26",
            accent: "#7aa2f7",
          },
        ];
        const themeEvents: ServerConfigStreamEventType[] = [
          {
            version: 1,
            type: "environmentThemesUpdated",
            payload: { themes: firstThemes },
          },
          {
            version: 1,
            type: "environmentThemesUpdated",
            payload: {
              themes: [{ ...firstThemes[0]!, name: "Nightfall 2" }],
            },
          },
          {
            version: 1,
            type: "environmentThemesUpdated",
            payload: { themes: [] },
          },
        ];
        const settingsEvents = Array.from({ length: 65 }, (): ServerConfigStreamEventType => ({
          version: 1,
          type: "settingsUpdated",
          payload: { settings: DEFAULT_SERVER_SETTINGS },
        }));
        const sourceEvents: ServerConfigStreamEventType[] = [
          SOURCE_EVENT,
          { version: 1, type: "usageLimitSourcesUpdated", payload: { sources: [] } },
        ];
        const allEvents = [...themeEvents, ...sourceEvents, ...settingsEvents];
        const observedByFastSubscriber = yield* Queue.unbounded<ServerConfigStreamEventType>();
        yield* session
          .subscribeServerConfig({ environmentThemes: true, usageLimitSources: true })
          .pipe(
            Stream.runForEach((event) => Queue.offer(observedByFastSubscriber, event)),
            Effect.forkChild,
          );
        expect((yield* Queue.take(observedByFastSubscriber)).type).toBe("snapshot");
        for (const event of allEvents) {
          yield* publishConfigEvents(socket, [event]);
          expect(yield* Queue.take(observedByFastSubscriber)).toEqual(event);
        }
        yield* Deferred.succeed(releaseSlowSubscriber, undefined);

        const recovered = Array.from(yield* Fiber.join(slowSubscriber));
        expect(recovered.map((event) => event.type)).toEqual([
          "snapshot",
          "snapshot",
          "environmentThemesUpdated",
          "usageLimitSourcesUpdated",
        ]);
        expect(recovered[2]).toMatchObject({ payload: { themes: [] } });
        expect(recovered[3]).toMatchObject({ payload: { sources: [] } });

        let projection = applyServerConfigProjection(Option.none(), {
          version: 1,
          type: "snapshot",
          config: SOURCE_SERVER_CONFIG,
        });
        projection = applyServerConfigProjection(projection, themeEvents[0]!);
        projection = applyServerConfigProjection(projection, SOURCE_EVENT);
        for (const event of recovered.slice(1)) {
          projection = applyServerConfigProjection(projection, event);
        }
        expect(Option.getOrThrow(projection).config.environmentThemes).toBeUndefined();
        expect(Option.getOrThrow(projection).config.usageLimitSources).toBeUndefined();
      }),
    ),
  );

  it.effect("closes the session when the config source dies", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { factory, sockets } = yield* makeFactory();
        const session = yield* factory.connect(PREPARED);
        const readyFiber = yield* Effect.forkChild(session.ready);
        const socket = yield* awaitSocket(sockets);
        socket.open();
        yield* completeInitialConfig(socket);
        yield* Fiber.join(readyFiber);

        const closedFiber = yield* session.closed.pipe(Effect.exit, Effect.forkChild);
        socket.serverMessage(
          encodeJson({
            _tag: "Defect",
            defect: encodeDefect(new Error("config stream died")),
          }),
        );

        const closed = yield* Fiber.join(closedFiber);
        expect(Exit.isFailure(closed)).toBe(true);
        if (Exit.isFailure(closed)) {
          expect(Cause.hasDies(closed.cause)).toBe(true);
        }
      }),
    ),
  );

  it.effect.each([{ failure: "defect" as const }, { failure: "typed" as const }])(
    "keeps durable config state alive after an owned $failure failure",
    ({ failure }) =>
      Effect.scoped(
        Effect.gen(function* () {
          const { factory, sockets } = yield* makeFactory({ environmentThemes: true });
          const firstSession = yield* factory.connect(PREPARED);
          const firstReady = yield* Effect.forkChild(firstSession.ready);
          const firstSocket = yield* awaitSocket(sockets);
          firstSocket.open();
          yield* completeInitialConfig(firstSocket, ENCODED_THEME_SERVER_CONFIG, {
            environmentThemes: true,
          });
          yield* Fiber.join(firstReady);

          const activeSession = yield* SubscriptionRef.make(Option.some(firstSession));
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
          const configState = yield* makeEnvironmentServerConfigState({
            environmentThemes: true,
          }).pipe(
            Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
            Effect.provideService(Persistence.EnvironmentCacheStore, cache),
          );
          const awaitConfig = (predicate: (config: ServerConfigType) => boolean) =>
            SubscriptionRef.changes(configState).pipe(
              Stream.filter(Option.isSome),
              Stream.map((projection) => projection.value.config),
              Stream.filter(predicate),
              Stream.runHead,
              Effect.map(Option.getOrThrow),
            );

          const firstThemes = [
            {
              id: "first-theme",
              name: "First theme",
              appearance: "dark" as const,
              canvas: "#111111",
              accent: "#ffffff",
            },
          ];
          const firstThemeState = yield* awaitConfig(
            (config) => config.environmentThemes?.[0]?.id === "first-theme",
          ).pipe(Effect.forkChild);
          yield* publishConfigEvents(firstSocket, [
            {
              version: 1,
              type: "environmentThemesUpdated",
              payload: { themes: firstThemes },
            },
          ]);
          expect((yield* Fiber.join(firstThemeState)).environmentThemes).toEqual(firstThemes);

          const firstClosed = yield* firstSession.closed.pipe(Effect.exit, Effect.forkChild);
          const firstRequest = yield* awaitRequest(firstSocket);
          firstSocket.serverMessage(
            failure === "defect"
              ? encodeJson({
                  _tag: "Defect",
                  defect: encodeDefect(new Error("config stream died")),
                })
              : encodeJson({
                  _tag: "Exit",
                  requestId: firstRequest.id,
                  exit: {
                    _tag: "Failure",
                    cause: [
                      {
                        _tag: "Fail",
                        error: {
                          _tag: "EnvironmentAuthorizationError",
                          message: "config subscription rejected",
                          requiredScope: "orchestration:read",
                        },
                      },
                    ],
                  },
                }),
          );
          const firstClosedExit = yield* Fiber.join(firstClosed);
          expect(Exit.isFailure(firstClosedExit)).toBe(true);
          if (failure === "typed" && Exit.isFailure(firstClosedExit)) {
            expect(Cause.squash(firstClosedExit.cause)).toBeInstanceOf(ConnectionBlockedError);
            expect(Cause.squash(firstClosedExit.cause)).toMatchObject({ reason: "permission" });
          }
          yield* SubscriptionRef.set(activeSession, Option.none());

          const recoveredConfig = {
            ...THEME_SERVER_CONFIG,
            environment: {
              ...THEME_SERVER_CONFIG.environment,
              label: "Recovered environment",
            },
          } satisfies ServerConfigType;
          const secondSession = yield* factory.connect(PREPARED);
          const secondReady = yield* Effect.forkChild(secondSession.ready);
          const secondSocket = yield* awaitSocket(sockets, 1);
          secondSocket.open();
          yield* completeInitialConfig(secondSocket, encodeServerConfig(recoveredConfig), {
            environmentThemes: true,
          });
          yield* Fiber.join(secondReady);

          const recoveredState = yield* awaitConfig(
            (config) => config.environment.label === "Recovered environment",
          ).pipe(Effect.forkChild);
          yield* SubscriptionRef.set(activeSession, Option.some(secondSession));
          expect((yield* Fiber.join(recoveredState)).environmentThemes).toEqual(firstThemes);

          const recoveredThemes = [
            {
              id: "recovered-theme",
              name: "Recovered theme",
              appearance: "dark" as const,
              canvas: "#000000",
              accent: "#eeeeee",
            },
          ];
          const liveRecoveredState = yield* awaitConfig(
            (config) => config.environmentThemes?.[0]?.id === "recovered-theme",
          ).pipe(Effect.forkChild);
          yield* publishConfigEvents(secondSocket, [
            {
              version: 1,
              type: "environmentThemesUpdated",
              payload: { themes: recoveredThemes },
            },
          ]);
          expect((yield* Fiber.join(liveRecoveredState)).environmentThemes).toEqual(
            recoveredThemes,
          );
        }),
      ),
  );

  it.effect("rejects a server config for a different environment", () =>
    Effect.gen(function* () {
      const { factory, sockets } = yield* makeFactory();
      const session = yield* factory.connect(PREPARED);
      const readyFiber = yield* Effect.forkChild(Effect.flip(session.ready));
      const configFiber = yield* session
        .subscribeServerConfig({})
        .pipe(Stream.runHead, Effect.flip, Effect.forkChild);
      const customConfigFiber = yield* session
        .subscribeServerConfig({ environmentThemes: true })
        .pipe(Stream.runHead, Effect.flip, Effect.forkChild);
      const socket = yield* awaitSocket(sockets);
      socket.open();
      yield* completeInitialConfig(socket, {
        ...ENCODED_SERVER_CONFIG,
        environment: {
          ...ENCODED_SERVER_CONFIG.environment,
          environmentId: "environment-2",
        },
      });

      const error = yield* Fiber.join(readyFiber);
      expect(error).toMatchObject({
        reason: "configuration",
        message: "Connected environment environment-2 does not match environment-1.",
      });
      expect((yield* Fiber.join(configFiber))._tag).toBe("RpcClientError");
      expect((yield* Fiber.join(customConfigFiber))._tag).toBe("RpcClientError");
    }),
  );

  it.effect("keeps reading replies after closing a stream with a full buffer", () =>
    Effect.gen(function* () {
      const { factory, sockets } = yield* makeFactory();
      const session = yield* factory.connect(PREPARED);
      const readyFiber = yield* Effect.forkChild(session.ready);
      const socket = yield* awaitSocket(sockets);
      socket.open();
      yield* completeInitialConfig(socket);
      yield* Fiber.join(readyFiber);

      const consuming = yield* Deferred.make<void>();
      const streamFiber = yield* session.client[WS_METHODS.subscribeServerConfig]({}).pipe(
        Stream.runForEach(() =>
          Deferred.succeed(consuming, undefined).pipe(Effect.andThen(Effect.never)),
        ),
        Effect.forkChild,
      );
      const streamRequest = yield* awaitRequest(socket, 1);
      const snapshot = { version: 1, type: "snapshot", config: ENCODED_SERVER_CONFIG };
      socket.serverMessage(
        encodeJson({
          _tag: "Chunk",
          requestId: streamRequest.id,
          values: Array.from({ length: 64 }, () => snapshot),
        }),
      );
      yield* Deferred.await(consuming);
      yield* Fiber.interrupt(streamFiber);

      const probeFiber = yield* Effect.forkChild(session.probe);
      const probeRequest = yield* awaitRequest(socket, 2);
      expect(probeRequest).toMatchObject({ tag: WS_METHODS.serverProbe });
      socket.serverMessage(
        encodeJson({
          _tag: "Exit",
          requestId: probeRequest.id,
          exit: { _tag: "Success", value: {} },
        }),
      );
      yield* Fiber.join(probeFiber);
    }).pipe(Effect.scoped),
  );

  it.effect("reaches ready when a newer server sends unknown config members", () =>
    Effect.gen(function* () {
      const { factory, sockets } = yield* makeFactory();
      const session = yield* factory.connect(PREPARED);
      const readyFiber = yield* Effect.forkChild(session.ready);
      const socket = yield* awaitSocket(sockets);
      socket.open();

      const shortcut = {
        key: "p",
        metaKey: false,
        ctrlKey: false,
        shiftKey: false,
        altKey: false,
        modKey: true,
      };
      yield* completeInitialConfig(socket, {
        ...ENCODED_SERVER_CONFIG,
        keybindings: [
          { command: "someFuture.toggle", shortcut },
          { command: "terminal.toggle", shortcut },
        ],
        issues: [{ kind: "keybindings.future-issue", message: "From a newer server" }],
        availableEditors: ["some-future-editor", "zed"],
      });
      yield* Fiber.join(readyFiber);

      const config = yield* session.initialConfig;
      expect(config.keybindings).toEqual([{ command: "terminal.toggle", shortcut }]);
      expect(config.issues).toEqual([]);
      expect(config.availableEditors).toEqual(["zed"]);
    }),
  );
});

describe("RpcSessionFactory transport selection", () => {
  it.effect("does not diagnose WebSocket auth when a selected native QUIC session closes", () =>
    Effect.gen(function* () {
      const pair = memorySessionPair();
      let requests = 0;
      const { factory, sockets } = yield* makeFactory(
        {},
        { connect: async () => pair.client },
        {
          fetch: () => {
            requests++;
            return Promise.resolve(new Response(null, { status: 401 }));
          },
        },
      );
      const connecting = yield* factory
        .connect({ ...QUIC_PREPARED, httpAuthorization: BEARER_PREPARED.httpAuthorization })
        .pipe(Effect.forkScoped);
      const control = yield* acceptQuicControl(pair);
      yield* control.write(quicReady, true);
      const session = yield* Fiber.join(connecting);
      yield* completeQuicConfig(pair);
      yield* session.ready;
      pair.server.close(0);
      expect((yield* Effect.flip(session.closed))._tag).toBe("ConnectionTransientError");
      expect(sockets).toHaveLength(0);
      expect(requests).toBe(0);
    }).pipe(Effect.scoped),
  );
  it.effect(
    "waits for full Ready before creating any application RPC and selects the real QUIC protocol",
    () =>
      Effect.gen(function* () {
        const pair = memorySessionPair();
        const urls: string[] = [];
        const { factory, sockets } = yield* makeFactory(
          {},
          {
            connect: async ({ url }) => {
              urls.push(url);
              return pair.client;
            },
          },
        );
        const connecting = yield* factory.connect(QUIC_PREPARED).pipe(Effect.forkScoped);
        const control = yield* acceptQuicControl(pair);
        expect(sockets).toHaveLength(0);
        expect(pair.opened()).toBe(1);
        yield* control.write(quicReady, true);
        const session = yield* Fiber.join(connecting);
        expect(session.transport).toBe("webtransport");
        expect(new URL(urls[0]!).pathname).toBe("/rpc/custom");
        expect(new URL(urls[0]!).searchParams.get("wsTicket")).toBe("issued-rpc-ticket");
        expect(new URL(urls[0]!).searchParams.get("connectionMethod")).toBe("direct");
        yield* completeQuicConfig(pair);
        yield* session.ready;
        expect(yield* session.initialConfig).toEqual(SERVER_CONFIG);
        expect(sockets).toHaveLength(0);
      }).pipe(Effect.scoped),
  );

  it.effect("rejects the candidate without touching the WSS winner's lifecycle", () =>
    Effect.gen(function* () {
      const pair = memorySessionPair({ neverAcknowledgeClose: true });
      const { factory, sockets } = yield* makeFactory({}, { connect: async () => pair.client });
      const connecting = yield* factory.connect(QUIC_PREPARED).pipe(Effect.forkScoped);
      const control = yield* acceptQuicControl(pair);
      yield* control.write({ ...quicReady, environmentId: "wrong-environment" }, true);
      const session = yield* Fiber.join(connecting);
      expect(session.transport).toBe("websocket");
      const socket = yield* awaitSocket(sockets);
      socket.open();
      yield* completeInitialConfig(socket);
      yield* session.ready;
      socket.close();
      expect((yield* Effect.flip(session.closed)).detail).toContain("disconnected");
    }).pipe(Effect.scoped),
  );

  it.effect("shares one two-second deadline across connect and control negotiation", () =>
    Effect.gen(function* () {
      const pair = memorySessionPair({ neverAcknowledgeClose: true });
      const started = Promise.withResolvers<AbortSignal>();
      const connected = Promise.withResolvers<ReliableSession>();
      const { factory, sockets } = yield* makeFactory(
        {},
        {
          connect: ({ signal }) => {
            started.resolve(signal);
            return connected.promise;
          },
        },
      );
      const connecting = yield* factory.connect(QUIC_PREPARED).pipe(Effect.forkScoped);
      yield* Effect.promise(() => started.promise);
      yield* TestClock.adjust(1500);
      connected.resolve(pair.client);
      yield* acceptQuicControl(pair);
      yield* TestClock.adjust(499);
      expect(sockets).toHaveLength(0);
      yield* TestClock.adjust(1);
      const session = yield* Fiber.join(connecting);
      expect(session.transport).toBe("websocket");
      const socket = yield* awaitSocket(sockets);
      socket.open();
      yield* completeInitialConfig(socket);
      yield* session.ready;
    }).pipe(Effect.scoped),
  );

  it.effect("falls back at two seconds and closes an ignored late native connect completion", () =>
    Effect.gen(function* () {
      const started = Promise.withResolvers<AbortSignal>();
      const late = Promise.withResolvers<ReliableSession>();
      const closed = Promise.withResolvers<void>();
      const { factory, sockets } = yield* makeFactory(
        {},
        {
          connect: ({ signal }) => {
            started.resolve(signal);
            return late.promise;
          },
        },
      );
      const connecting = yield* factory.connect(QUIC_PREPARED).pipe(Effect.forkScoped);
      const signal = yield* Effect.promise(() => started.promise);
      yield* TestClock.adjust(2000);
      const session = yield* Fiber.join(connecting);
      expect(session.transport).toBe("websocket");
      expect(signal.aborted).toBe(true);
      const socket = yield* awaitSocket(sockets);
      socket.open();
      yield* completeInitialConfig(socket);
      yield* session.ready;
      late.resolve({
        closed: new Promise(() => {}),
        close: () => closed.resolve(),
        openBidirectionalStream: () => Promise.reject(new Error("must not open")),
      });
      yield* Effect.promise(() => closed.promise);
      yield* session.ready;
    }).pipe(Effect.scoped),
  );

  it.effect.each(["unsupported", "expired"] as const)(
    "uses WSS without a candidate for %s capability",
    (reason) =>
      Effect.gen(function* () {
        let connects = 0;
        const connector = {
          connect: () => {
            connects++;
            return Promise.reject(new Error("unused"));
          },
        };
        const { factory, sockets } = yield* makeFactory(
          {},
          reason === "unsupported" ? undefined : connector,
        );
        const session = yield* factory.connect(
          reason === "expired"
            ? {
                ...QUIC_PREPARED,
                rpcTicket: { value: Redacted.make("expired"), expiresAtEpochMs: 0 },
              }
            : QUIC_PREPARED,
        );
        expect(session.transport).toBe("websocket");
        expect(connects).toBe(0);
        const socket = yield* awaitSocket(sockets);
        socket.open();
        yield* completeInitialConfig(socket);
        yield* session.ready;
      }).pipe(Effect.scoped),
  );

  it.effect("preserves cooldown between sessions after native SessionRejected", () =>
    Effect.gen(function* () {
      let connects = 0;
      const { factory } = yield* makeFactory(
        {},
        {
          connect: () => {
            connects++;
            return Promise.reject(new Error("SessionRejected"));
          },
        },
      );
      const connection = {
        ...QUIC_PREPARED,
        httpAuthorization: {
          _tag: "Dpop" as const,
          accessToken: "still-valid",
          expiresAtEpochMs: Number.MAX_SAFE_INTEGER,
        },
      };
      yield* factory.connect(connection).pipe(Effect.scoped);
      yield* factory.connect(connection).pipe(Effect.scoped);
      expect(connects).toBe(1);
      expect(connection.httpAuthorization.accessToken).toBe("still-valid");
      yield* TestClock.adjust(30_000);
      yield* factory.connect(connection).pipe(Effect.scoped);
      expect(connects).toBe(2);
    }).pipe(Effect.scoped),
  );

  it.effect.each(["normal-disposal", "peer-failure"] as const)(
    "counts %s correctly across owner scopes",
    (reason) =>
      Effect.gen(function* () {
        const pairs = [memorySessionPair(), memorySessionPair()];
        let connects = 0;
        const { factory } = yield* makeFactory(
          {},
          { connect: async () => pairs[connects++]!.client },
        );
        const owner = yield* Scope.make();
        const connecting = yield* factory
          .connect(QUIC_PREPARED)
          .pipe(Effect.provideService(Scope.Scope, owner), Effect.forkScoped);
        const control = yield* acceptQuicControl(pairs[0]!);
        yield* control.write(quicReady, true);
        const session = yield* Fiber.join(connecting);
        yield* completeQuicConfig(pairs[0]!);
        yield* session.ready;
        if (reason === "peer-failure") {
          pairs[0]!.rawServer.close({ closeCode: 17 });
          yield* Effect.flip(session.closed);
        }
        yield* Scope.close(owner, Exit.void);
        const next = yield* factory.connect(QUIC_PREPARED).pipe(Effect.forkScoped);
        if (reason === "normal-disposal") {
          const control = yield* acceptQuicControl(pairs[1]!);
          yield* control.write(quicReady, true);
        }
        const second = yield* Fiber.join(next);
        expect(second.transport).toBe(reason === "normal-disposal" ? "webtransport" : "websocket");
        expect(connects).toBe(reason === "normal-disposal" ? 2 : 1);
      }).pipe(Effect.scoped),
  );

  it.effect("retains a Ready-to-publication control failure for the selected session", () =>
    Effect.gen(function* () {
      const pair = memorySessionPair();
      const { factory, sockets } = yield* makeFactory({}, { connect: async () => pair.client });
      const connecting = yield* factory.connect(QUIC_PREPARED).pipe(Effect.forkScoped);
      const control = yield* acceptQuicControl(pair);
      yield* control.write(quicReady, true);
      yield* control.write({ _tag: "Eof" }, true);
      const session = yield* Fiber.join(connecting);
      expect(session.transport).toBe("webtransport");
      expect((yield* Effect.flip(session.ready)).reason).toBe("transport");
      expect(sockets).toHaveLength(0);
    }).pipe(Effect.scoped),
  );

  it.effect("does not replay a mutation whose committed response is lost after selection", () =>
    Effect.gen(function* () {
      const pair = memorySessionPair();
      const { factory, sockets } = yield* makeFactory({}, { connect: async () => pair.client });
      const connecting = yield* factory.connect(QUIC_PREPARED).pipe(Effect.forkScoped);
      const control = yield* acceptQuicControl(pair);
      yield* control.write(quicReady, true);
      const session = yield* Fiber.join(connecting);
      yield* completeQuicConfig(pair);
      yield* session.ready;
      const mutation = yield* session.client[WS_METHODS.serverUpdateSettings]({ patch: {} }).pipe(
        Effect.exit,
        Effect.forkScoped,
      );
      const request = yield* acceptQuicRequest(pair);
      expect(request.request.tag).toBe(WS_METHODS.serverUpdateSettings);
      let committed = 0;
      committed++;
      pair.rawServer.close({ closeCode: 17 });
      expect(Exit.isFailure(yield* Fiber.join(mutation))).toBe(true);
      expect(
        Exit.isFailure(
          yield* session.client[WS_METHODS.serverUpdateSettings]({ patch: {} }).pipe(Effect.exit),
        ),
      ).toBe(true);
      expect(committed).toBe(1);
      expect(pair.opened()).toBe(3);
      expect(sockets).toHaveLength(0);
    }).pipe(Effect.scoped),
  );
});

it.effect("old arm uses the ticket socket without bearer upgrade headers", () =>
  Effect.gen(function* () {
    const { factory, sockets } = yield* makeFactory().pipe(
      Effect.provideService(ClientCapabilities.WebSocketConstructorWithHeaders, () => {
        throw new Error("Old arm must use its ticket constructor");
      }),
    );
    const session = yield* factory
      .connect({
        ...PREPARED,
        socketUrl: "wss://environment.example.test/ws?wsTicket=old-ticket",
        httpAuthorization: { _tag: "Bearer", token: "secret" },
      })
      .pipe(Effect.provideService(AttemptReconnectArm, "old"));
    const ready = yield* Effect.forkChild(session.ready);
    const socket = yield* awaitSocket(sockets);
    expect(socket.url).toContain("wsTicket=old-ticket");
    socket.open();
    yield* completeInitialConfig(socket);
    yield* Fiber.join(ready);
  }),
);
