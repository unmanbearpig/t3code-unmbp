// @effect-diagnostics nodeBuiltinImport:off - Isolated benchmark listener and IPC fixture control.
import * as NodeFS from "node:fs";
import * as NodeHttp from "node:http";
import * as NodeHttps from "node:https";
import * as NodeCrypto from "node:crypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeHttpPlatform from "@effect/platform-node/NodeHttpPlatform";
import {
  AuthStandardClientScopes,
  ServerConfig as ServerConfigSchema,
  EnvironmentAuthorizationError,
  EventId,
  ProjectId,
  ThreadId,
  WS_METHODS,
  EnvironmentHttpApi,
  ORCHESTRATION_V2_WS_METHODS,
  ORCHESTRATION_PROTOCOL_VERSION,
  OrchestrationV2GetThreadProjectionError,
  OrchestrationV2GetShellSnapshotError,
  OrchestrationV2RpcSchemas,
  WsRpcGroup as ServerWsRpcGroup,
  WsSubscribeServerConfigRpc,
  TerminalAttachStreamEvent,
  TerminalObserveInput,
  TerminalError,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { HttpRouter } from "effect/http";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import { Rpc, RpcGroup, RpcSerialization, RpcServer } from "effect/rpc";
import * as SqlitePersistence from "../../src/persistence/Sqlite.ts";
import * as OrchestrationEventStore from "../../src/persistence/OrchestrationEventStore.ts";
import * as EventStore from "../../src/orchestration-v2/EventStore.ts";
import * as EventSink from "../../src/orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../../src/orchestration-v2/ProjectionStore.ts";
import * as ThreadManagementService from "../../src/orchestration-v2/ThreadManagementService.ts";
import * as ProjectService from "../../src/project/ProjectService.ts";
import * as ProjectStore from "../../src/orchestration-v2/ProjectStore.ts";
import * as ProjectEnrichmentService from "../../src/project/ProjectEnrichmentService.ts";
import * as OrchestrationHttp from "../../src/orchestration-v2/http.ts";
import * as ServerHttp from "../../src/http.ts";
import * as EnvironmentAuth from "../../src/auth/EnvironmentAuth.ts";
import * as AuthHttp from "../../src/auth/http.ts";
import * as ServerSecretStore from "../../src/auth/ServerSecretStore.ts";
import * as ServerEnvironment from "../../src/environment/ServerEnvironment.ts";
import * as ServerConfig from "../../src/config.ts";
import * as AnalyticsService from "../../src/telemetry/AnalyticsService.ts";
import * as RpcAuthorization from "../../src/auth/RpcAuthorization.ts";
import { guardHttpResponseWriteErrors } from "../../src/httpResponseErrorGuard.ts";
import * as AuthenticatedRpcSession from "../../src/AuthenticatedRpcSession.ts";
import * as RpcSessionLifetime from "../../src/RpcSessionLifetime.ts";
import { makeRpcSessionProtocol } from "../../src/RpcSessionProtocol.ts";
import * as BackgroundPolicy from "../../src/background/BackgroundPolicy.ts";
import * as HostPowerMonitor from "../../src/background/HostPowerMonitor.ts";
import * as ServerSettings from "../../src/serverSettings.ts";
import * as ServerRuntimeStartup from "../../src/serverRuntimeStartup.ts";
import * as WebTransportAvailability from "../../src/transport/WebTransportAvailability.ts";
import * as WebTransportListener from "../../src/transport/WebTransportListener.ts";
import * as Ws from "../../src/ws.ts";
import { withTerminalOutputWindow } from "../../src/terminal/OutputProtocol.ts";
import { wallClockMs } from "./protocol.ts";
import {
  buildBoundedThreadProjection,
  THREAD_HISTORY_PAGE_POLICY,
  THREAD_HISTORY_SNAPSHOT_ROW_LIMIT,
} from "../../src/orchestration-v2/threadHistoryPaging.ts";
import { makeResourceControl } from "./resourceUsage.ts";
import {
  subscribeOrchestrationV2Thread,
  subscribeOrchestrationV2Shell,
  WS_RPC_SERVER_OPTIONS,
} from "../../src/AuthenticatedRpcSession.ts";
import { projectThreadProjectionForWire } from "../../src/orchestration-v2/WireProjection.ts";
import {
  operationMessageEvents,
  operationMessageText,
  operationServerConfig,
  operationThreadCreated,
  operationThreadId,
  OperationControlResponse,
} from "./operationsFixture.ts";

const [scheme, portText, certPath, keyPath, scratch, workload] = process.argv.slice(2);
if (!scratch || !certPath || !keyPath || !portText || !["http", "https"].includes(scheme!))
  throw new Error("reconnectServer.ts http|https port cert key scratch");
const port = Number(portText);
export const reconnectConfig = {
  ...operationServerConfig,
  environment: {
    ...operationServerConfig.environment,
    orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
    capabilities: { ...operationServerConfig.environment.capabilities, connectionProbe: true },
  },
};
const persistence = EventSink.layer.pipe(
  Layer.provideMerge(EventStore.layerFromOrchestrationEventStore),
  Layer.provideMerge(ProjectionStore.layer),
  Layer.provideMerge(OrchestrationEventStore.layer),
  Layer.provideMerge(SqlitePersistence.layerMemory),
);
// Fixture writes bypass provider execution. Snapshot, SQL persistence, replay budget,
// wire projection, live coalescing, and HTTP handlers remain the production implementations.
const management = Layer.unwrap(
  Effect.gen(function* () {
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const sink = yield* EventSink.EventSinkV2;
    return Layer.mock(ThreadManagementService.ThreadManagementService)({
      ensureLegacyTranscript: () => Effect.void,
      getThreadSnapshot: (id) => projections.getThreadSnapshot(id).pipe(Effect.orDie),
      getThreadSnapshotWindow: (id, options) =>
        projections.getThreadSnapshotWindow(id, options).pipe(Effect.orDie),
      getThreadShell: (id) => projections.getThreadShell(id).pipe(Effect.orDie),
      readShellSnapshot: (options) =>
        projections.readShellSnapshot(options).pipe(Effect.map(Effect.orDie), Effect.orDie),
      streamStoredEventsFrom: (input) =>
        sink.stream({ ...input, bounded: true }).pipe(Stream.orDie),
    });
  }),
);
const enrichment = Layer.unwrap(
  Effect.gen(function* () {
    const changes = yield* PubSub.unbounded<never>();
    return Layer.mock(ProjectEnrichmentService.ProjectEnrichmentService)({
      getAvailable: () =>
        Effect.succeed({
          repositoryIdentity: null,
          faviconPath: null,
          repositoryIdentityResolved: true,
        }),
      subscribeChanges: PubSub.subscribe(changes),
    });
  }),
);
const fixtureServices = management.pipe(
  Layer.provideMerge(
    Layer.unwrap(
      Effect.map(ProjectStore.ProjectStoreV2, (store) =>
        Layer.mock(ProjectService.ProjectService)({
          getShell: (id) => store.getShell(id).pipe(Effect.orDie),
        }),
      ),
    ).pipe(Layer.provide(ProjectStore.layer)),
  ),
  Layer.provideMerge(ProjectStore.layer),
  Layer.provideMerge(enrichment),
  Layer.provideMerge(persistence),
  Layer.provideMerge(NodeServices.layer),
);

const encodeListenerConfig = Schema.encodeSync(
  Schema.fromJsonString(
    Schema.Struct({
      url: Schema.String,
      host: Schema.String,
      port: Schema.Number,
      certPath: Schema.String,
      keyPath: Schema.String,
    }),
  ),
);
const serverConfig = Layer.effect(
  ServerConfig.ServerConfig,
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    return {
      ...config,
      devUrl: undefined,
      devAuthToken: undefined,
      port,
      host: "0.0.0.0",
      rpcWebTransport: encodeListenerConfig({
        url: `https://127.0.0.1:${port}/rpc`,
        host: "0.0.0.0",
        port,
        certPath,
        keyPath,
      }),
    };
  }),
).pipe(Layer.provide(ServerConfig.layerTest(process.cwd(), scratch)));
const environment = Layer.mergeAll(
  Layer.succeed(ServerEnvironment.ServerEnvironmentIdentity, {
    getEnvironmentId: Effect.succeed(reconnectConfig.environment.environmentId),
  }),
  Layer.effect(
    ServerEnvironment.ServerEnvironment,
    Effect.gen(function* () {
      const availability = yield* WebTransportAvailability.WebTransportAvailability;
      return {
        getEnvironmentId: Effect.succeed(reconnectConfig.environment.environmentId),
        getDescriptor: availability.get.pipe(
          Effect.map((endpoint) => ({
            ...reconnectConfig.environment,
            ...(Option.isSome(endpoint) ? { rpcWebTransport: endpoint.value } : {}),
          })),
        ),
      };
    }),
  ),
);
const secrets = Layer.mock(ServerSecretStore.ServerSecretStore)({
  // Only the signing key is needed. Keep it in memory along with the sessions database.
  getOrCreateRandom: (() => {
    const keys = new Map<string, Uint8Array>();
    return (name: string, size: number) =>
      Effect.sync(() => {
        if (!keys.has(name)) keys.set(name, NodeCrypto.randomBytes(size));
        return keys.get(name)!;
      });
  })(),
});
const auth = EnvironmentAuth.layer.pipe(
  Layer.provideMerge(fixtureServices),
  Layer.provideMerge(environment),
  Layer.provideMerge(secrets),
  Layer.provide(serverConfig),
  Layer.provideMerge(Layer.mock(AnalyticsService.AnalyticsService)({ record: () => Effect.void })),
);
const group = RpcGroup.make(
  Rpc.make(WS_METHODS.serverGetConfig, { payload: Schema.Struct({}), success: ServerConfigSchema }),
  Rpc.make(WS_METHODS.terminalObserve, {
    payload: TerminalObserveInput,
    success: TerminalAttachStreamEvent,
    error: TerminalError,
    stream: true,
  }),
  Rpc.make(ORCHESTRATION_V2_WS_METHODS.getThreadProjection, {
    payload: OrchestrationV2RpcSchemas.getThreadProjection.input,
    success: OrchestrationV2RpcSchemas.getThreadProjection.output,
    error: OrchestrationV2GetThreadProjectionError,
  }),
  Rpc.make(WS_METHODS.serverProbe, {
    payload: Schema.Struct({}),
    success: Schema.Struct({}),
    error: EnvironmentAuthorizationError,
  }),
  Rpc.make(ORCHESTRATION_V2_WS_METHODS.subscribeShell, {
    payload: OrchestrationV2RpcSchemas.subscribeShell.input,
    success: OrchestrationV2RpcSchemas.subscribeShell.output,
    error: OrchestrationV2GetShellSnapshotError,
    stream: true,
  }),
  WsSubscribeServerConfigRpc,
  Rpc.make(ORCHESTRATION_V2_WS_METHODS.subscribeThread, {
    payload: OrchestrationV2RpcSchemas.subscribeThread.input,
    success: OrchestrationV2RpcSchemas.subscribeThread.output,
    error: OrchestrationV2GetThreadProjectionError,
    stream: true,
  }),
);
const handlers = Layer.effectContext(
  Effect.gen(function* () {
    // Register the complete production wire group. Unrelated methods fail loudly if called.
    const context = yield* Effect.context<never>();
    const fallback = Context.makeUnsafe<Rpc.ToHandler<RpcGroup.Rpcs<typeof ServerWsRpcGroup>>>(
      new Map(
        [...ServerWsRpcGroup.requests.values()].map((rpc) => [
          rpc.key,
          {
            context,
            handler: () => Effect.die(new Error(`Unexpected benchmark RPC: ${rpc._tag}`)),
          },
        ]),
      ),
    );
    const measured = yield* group.toHandlers({
      [WS_METHODS.serverGetConfig]: () => Effect.succeed(reconnectConfig),
      [WS_METHODS.terminalObserve]: () =>
        Stream.fromIterable(
          Array.from({ length: 256 }, (_, index) => ({
            type: "output" as const,
            threadId: operationThreadId,
            terminalId: "benchmark-terminal",
            sequence: index,
            data: operationMessageText(index),
          })),
        ).pipe(Stream.rechunk(1)),
      [ORCHESTRATION_V2_WS_METHODS.getThreadProjection]: () =>
        Effect.map(ProjectionStore.ProjectionStoreV2, (store) => store).pipe(
          Effect.flatMap((store) => store.getThreadSnapshot(operationThreadId)),
          Effect.map((snapshot) => projectThreadProjectionForWire(snapshot.projection)),
          Effect.orDie,
        ),
      [WS_METHODS.serverProbe]: () => Effect.succeed({}),
      [ORCHESTRATION_V2_WS_METHODS.subscribeShell]: (input) =>
        Stream.unwrap(subscribeOrchestrationV2Shell(input)),
      subscribeServerConfig: () =>
        Stream.concat(
          Stream.make({
            version: 1 as const,
            type: "snapshot" as const,
            config: { ...reconnectConfig, shellResumeCompletionMarker: true },
          }),
          Stream.never,
        ),
      [ORCHESTRATION_V2_WS_METHODS.subscribeThread]: (input) =>
        Stream.unwrap(subscribeOrchestrationV2Thread(input)),
    });
    return Context.merge(fallback, measured);
  }),
);
// Only application handlers are fixture-owned. Admission, protocol preparation,
// session ownership/revocation, shutdown and terminal flow control are production code.
const terminalProtocol = {
  requests: 0,
  chunks: 0,
  wireAcks: 0,
  interrupts: 0,
  maxPendingChunks: 0,
};
function observeTerminalProtocol(
  protocol: RpcServer.Protocol["Service"],
): RpcServer.Protocol["Service"] {
  const pending = new Map<string, number>();
  return {
    ...protocol,
    run: (write) =>
      protocol.run((clientId, message) =>
        Effect.suspend(() => {
          if (message._tag === "Request" && message.tag === WS_METHODS.terminalObserve) {
            terminalProtocol.requests++;
            pending.set(`${clientId}:${message.id}`, 0);
          } else if (message._tag === "Ack") {
            const key = `${clientId}:${message.requestId}`;
            if (pending.has(key)) {
              terminalProtocol.wireAcks++;
              pending.set(key, Math.max(0, pending.get(key)! - 1));
            }
          } else if (
            message._tag === "Interrupt" &&
            pending.has(`${clientId}:${message.requestId}`)
          ) {
            terminalProtocol.interrupts++;
          }
          return write(clientId, message);
        }),
      ),
    send: (clientId, message, transferables) =>
      Effect.suspend(() => {
        if (message._tag === "Chunk") {
          const key = `${clientId}:${message.requestId}`;
          if (pending.has(key)) {
            terminalProtocol.chunks++;
            const size = pending.get(key)! + 1;
            pending.set(key, size);
            terminalProtocol.maxPendingChunks = Math.max(terminalProtocol.maxPendingChunks, size);
          }
        }
        return protocol.send(clientId, message, transferables);
      }),
  };
}
const applicationSession = Layer.effect(
  AuthenticatedRpcSession.AuthenticatedRpcSession,
  Effect.gen(function* () {
    const lifetime = yield* RpcSessionLifetime.RpcSessionLifetime;
    const handlerContext =
      yield* Effect.context<Rpc.ToHandler<RpcGroup.Rpcs<typeof ServerWsRpcGroup>>>();
    return AuthenticatedRpcSession.AuthenticatedRpcSession.of({
      run: (input) =>
        Effect.suspend(() => {
          const guarded = makeRpcSessionProtocol(observeTerminalProtocol(input.protocol));
          const shutdown = guarded.close.pipe(Effect.andThen(input.close ?? Effect.void));
          return lifetime.run(input.session, input.origin ?? {}, input.analyticsProps ?? {}, () => {
            const connection = Effect.gen(function* () {
              yield* RpcServer.make(ServerWsRpcGroup, WS_RPC_SERVER_OPTIONS).pipe(
                Effect.provideService(
                  RpcServer.Protocol,
                  withTerminalOutputWindow(guarded.protocol),
                ),
                Effect.provide(RpcAuthorization.layer(input.session.scopes)),
                Effect.provide(handlerContext),
                Effect.forkScoped,
              );
              return yield* input.lifetime;
            }).pipe(Effect.ensuring(shutdown), Effect.scoped);
            return Effect.scoped(
              (input.closed === undefined
                ? connection
                : Effect.raceFirst(connection, input.closed.pipe(Effect.as(undefined)))
              ).pipe(Effect.ensuring(shutdown)),
            );
          });
        }),
    });
  }),
).pipe(
  Layer.provide(handlers.pipe(Layer.provideMerge(RpcSerialization.layerJson))),
  Layer.provide(
    RpcSessionLifetime.layer.pipe(
      Layer.provide(
        BackgroundPolicy.layer.pipe(
          Layer.provide(Layer.effect(HostPowerMonitor.HostPowerMonitor, HostPowerMonitor.make())),
          Layer.provide(ServerSettings.layerTest()),
        ),
      ),
    ),
  ),
);
class Api extends HttpApi.make("environment").add(
  EnvironmentHttpApi.groups.orchestration,
  EnvironmentHttpApi.groups.auth,
  EnvironmentHttpApi.groups.metadata,
) {}
const routes = Layer.mergeAll(
  Ws.layer,
  HttpApiBuilder.layer(Api).pipe(
    Layer.provide(
      Layer.mergeAll(
        OrchestrationHttp.layer,
        AuthHttp.layer,
        ServerHttp.layerServerEnvironmentHttpApi,
      ),
    ),
    Layer.provide(AuthHttp.layerAuthenticatedAuth),
  ),
).pipe(
  Layer.provide(ServerHttp.layerHttpCompression),
  Layer.provide(NodeHttpPlatform.layer),
  Layer.provide(Layer.mock(AnalyticsService.AnalyticsService)({ record: () => Effect.void })),
);
const serverRequests: { path: string; connection: number; atMs: number; bytes: number }[] = [];
const requestSchema = Schema.Struct({ count: Schema.Number });
const decodeRequest = Schema.decodeUnknownSync(requestSchema);
const encodeResponse = Schema.encodeSync(Schema.toCodecJson(OperationControlResponse));
const encodeReady = Schema.encodeSync(
  Schema.toCodecJson(
    Schema.Struct({
      type: Schema.Literal("ready"),
      bearer: Schema.String,
      initial: OperationControlResponse,
    }),
  ),
);

await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const scope = yield* Effect.scope;
      const published = yield* Deferred.make<void>();
      const availability = Layer.effect(
        WebTransportAvailability.WebTransportAvailability,
        Effect.gen(function* () {
          const service = yield* WebTransportAvailability.WebTransportAvailability;
          return {
            ...service,
            set: (endpoint: Parameters<typeof service.set>[0]) =>
              service
                .set(endpoint)
                .pipe(
                  Effect.andThen(
                    Option.isSome(endpoint) ? Deferred.succeed(published, undefined) : Effect.void,
                  ),
                  Effect.asVoid,
                ),
          };
        }),
      ).pipe(Layer.provide(WebTransportAvailability.layer));
      const context = yield* Layer.buildWithScope(
        auth.pipe(Layer.provideMerge(availability)),
        scope,
      );
      const sink = Context.get(context, EventSink.EventSinkV2);
      const projections = Context.get(context, ProjectionStore.ProjectionStoreV2);
      const serverAuth = Context.get(context, EnvironmentAuth.EnvironmentAuth);
      const pairing = yield* serverAuth.issuePairingCredential({
        scopes: AuthStandardClientScopes,
      });
      const paired = yield* serverAuth.exchangeBootstrapCredentialForAccessToken(
        pairing.credential,
        undefined,
        {
          deviceType: "mobile",
          label: "Reconnect benchmark",
        },
      );
      const projects = Context.get(context, ProjectStore.ProjectStoreV2);
      for (let i = 0; i < 8; i++) {
        const projectId = ProjectId.make(i === 0 ? "operation-project" : `bench-project-${i}`);
        yield* projects.apply({
          sequence: i + 1,
          eventId: EventId.make(`bench-project-${i}`),
          aggregateKind: "project",
          aggregateId: projectId,
          occurredAt: "2026-10-14T00:00:00.000Z",
          commandId: null,
          causationEventId: null,
          correlationId: null,
          metadata: {},
          type: "project.created",
          payload: {
            projectId,
            title: `Reconnect project ${i}`,
            workspaceRoot: `/benchmark/project-${i}`,
            defaultModelSelection: null,
            scripts: [],
            createdAt: "2026-10-14T00:00:00.000Z",
            updatedAt: "2026-10-14T00:00:00.000Z",
          },
        });
      }
      const fixtureEvents = Array.from({ length: 159 }, (_, i) => {
        if (operationThreadCreated.type !== "thread.created") throw new Error("Fixture changed");
        const id = ThreadId.make(`bench-shell-thread-${i}`);
        return {
          ...operationThreadCreated,
          id: EventId.make(`bench-shell-created-${i}`),
          threadId: id,
          payload: {
            ...operationThreadCreated.payload,
            id,
            projectId: ProjectId.make(i % 8 === 0 ? "operation-project" : `bench-project-${i % 8}`),
            title: `Thread ${i}: ${operationMessageText(i).slice(0, 100)}`,
            lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
          },
        };
      });
      yield* sink.write({ events: fixtureEvents });
      let messages = workload === "open-long" || workload === "bulk-overlap" ? 2000 : 100;
      yield* sink.write({
        events: [operationThreadCreated, ...operationMessageEvents(0, messages)],
      });
      const controlResponse = Effect.gen(function* () {
        const snapshot = yield* projections.getThreadSnapshot(operationThreadId);
        const window = yield* projections.getThreadSnapshotWindow(operationThreadId, {
          rowLimit: THREAD_HISTORY_SNAPSHOT_ROW_LIMIT,
          userTurnLimit: THREAD_HISTORY_PAGE_POLICY.maxUserTurns,
        });
        const bounded = buildBoundedThreadProjection({
          ...window,
          projection: projectThreadProjectionForWire(window.projection),
        });
        return {
          snapshot,
          windowMessageIds: bounded.projection.messages.map((m) => m.id),
          windowRunIds: bounded.projection.runs.map((r) => r.id),
          hasMoreHistory: bounded.hasMoreHistory,
          latestLocalTurnOrdinal: bounded.latestLocalTurnOrdinal,
        };
      });
      const initial = yield* controlResponse;
      const applicationContext = yield* Layer.buildWithScope(
        applicationSession.pipe(Layer.provide(Layer.succeedContext(context))),
        scope,
      );
      // Fixture command readiness follows seeding. Listener publication still
      // comes only from the production listener after native startup completes.
      const listening = yield* Layer.buildWithScope(
        WebTransportListener.layerListening.pipe(
          Layer.provide(Layer.succeedContext(Context.merge(context, applicationContext))),
          Layer.provide(serverConfig),
          Layer.provide(
            Layer.mock(ServerRuntimeStartup.ServerRuntimeStartup)({
              awaitCommandReady: Effect.void,
            }),
          ),
        ),
        scope,
      ).pipe(Effect.forkScoped);
      yield* Effect.raceFirst(
        Deferred.await(published),
        Fiber.join(listening).pipe(Effect.andThen(Effect.never)),
      ).pipe(Effect.timeout("10 seconds"));
      const listener = NodeHttpServer.layer(
        () => {
          const server =
            scheme === "http"
              ? NodeHttp.createServer()
              : NodeHttps.createServer({
                  cert: NodeFS.readFileSync(certPath),
                  key: NodeFS.readFileSync(keyPath),
                  minVersion: "TLSv1.3",
                  maxVersion: "TLSv1.3",
                });
          let nextConnection = 0;
          const connections = new WeakMap<object, number>();
          const record = (
            request: NodeHttp.IncomingMessage,
            response?: NodeHttp.ServerResponse,
          ) => {
            if (!connections.has(request.socket)) connections.set(request.socket, ++nextConnection);
            response?.setHeader("x-netbench-protocol", `HTTP/${request.httpVersion}`);
            const row = {
              path: new URL(request.url!, "http://bench").pathname,
              connection: connections.get(request.socket)!,
              atMs: Number(process.hrtime.bigint()) / 1e6,
              bytes: 0,
            };
            serverRequests.push(row);
            const socket = request.socket;
            const before = socket.bytesWritten;
            response?.once("finish", () => {
              row.bytes = socket.bytesWritten - before;
            });
          };
          server.prependListener("request", record);
          server.prependListener("upgrade", (request) => record(request));
          return guardHttpResponseWriteErrors(server);
        },
        {
          host: "0.0.0.0",
          port,
          gracefulShutdownTimeout: 0,
          websocket: { perMessageDeflate: true },
        },
      );
      yield* Layer.buildWithScope(
        HttpRouter.serve(routes, { disableListenLog: true }).pipe(
          Layer.provide(listener),
          Layer.provide(Layer.succeedContext(Context.merge(context, applicationContext))),
        ),
        scope,
      );
      const requests = yield* Queue.unbounded<unknown>();
      const resourceControl = makeResourceControl();
      const offer = (message: unknown) => {
        if (!resourceControl(message)) Queue.offerUnsafe(requests, message);
      };
      process.on("message", offer);
      yield* Effect.addFinalizer(() => Effect.sync(() => process.off("message", offer)));
      process.send?.(
        encodeReady({
          type: "ready",
          bearer: paired.access_token,
          initial: {
            startedAt: 0,
            sequence: initial.snapshot.snapshotSequence,
            messages,
            windowMessageIds: initial.windowMessageIds,
            windowRunIds: initial.windowRunIds,
            hasMoreHistory: initial.hasMoreHistory,
            latestLocalTurnOrdinal: initial.latestLocalTurnOrdinal,
            ...(workload === "open-long"
              ? {}
              : {
                  initialSnapshot: {
                    snapshotSequence: initial.snapshot.snapshotSequence,
                    projection: projectThreadProjectionForWire(initial.snapshot.projection),
                  },
                }),
          },
        }),
      );
      for (;;) {
        const raw = yield* Queue.take(requests);
        if (typeof raw === "object" && raw !== null && "inspect" in raw) {
          process.send?.({ serverRequests, terminalProtocol });
          continue;
        }
        const request = decodeRequest(raw);
        const startedAt = wallClockMs();
        if (request.count) {
          yield* sink.write({ events: operationMessageEvents(messages, request.count) });
          messages += request.count;
        }
        const full = yield* controlResponse;
        process.send?.(
          encodeResponse({
            startedAt,
            sequence: full.snapshot.snapshotSequence,
            messages,
            windowMessageIds: full.windowMessageIds,
            windowRunIds: full.windowRunIds,
            hasMoreHistory: full.hasMoreHistory,
            latestLocalTurnOrdinal: full.latestLocalTurnOrdinal,
          }),
        );
      }
    }),
  ).pipe(
    Effect.provide(NodeServices.layer),
    Effect.provideService(Logger.CurrentLoggers, new Set()),
  ),
);
