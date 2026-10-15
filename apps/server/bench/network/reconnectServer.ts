// @effect-diagnostics nodeBuiltinImport:off - Isolated benchmark listener and IPC fixture control.
import * as NodeFS from "node:fs";
import * as NodeHttp from "node:http";
import * as NodeHttps from "node:https";
import * as NodeCrypto from "node:crypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeHttpPlatform from "@effect/platform-node/NodeHttpPlatform";
import {
  AuthStandardClientScopes,
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
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
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
import { layerNodeHttpServer } from "../../src/httpServer.ts";
import {
  layerWebSocketRoute,
  subscribeOrchestrationV2Thread,
  subscribeOrchestrationV2Shell,
  WS_RPC_SERVER_OPTIONS,
} from "../../src/ws.ts";
import { projectThreadProjectionForWire } from "../../src/orchestration-v2/WireProjection.ts";
import {
  operationMessageEvents,
  operationMessageText,
  operationServerConfig,
  operationThreadCreated,
  operationThreadId,
  OperationControlResponse,
} from "./operationsFixture.ts";

const [scheme, portText, certPath, keyPath, scratch] = process.argv.slice(2);
if (!scratch || !certPath || !keyPath || !portText || !["http", "https"].includes(scheme!))
  throw new Error("reconnectServer.ts http|https port cert key scratch");
const port = Number(portText);
export const reconnectConfig = {
  ...operationServerConfig,
  environment: {
    ...operationServerConfig.environment,
    orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
    capabilities: {
      ...operationServerConfig.environment.capabilities,
      connectionProbe: true,
      websocketHello: 1 as const,
    },
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

const serverConfig = Layer.effect(
  ServerConfig.ServerConfig,
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    return { ...config, devUrl: undefined, devAuthToken: undefined, port, host: "0.0.0.0" };
  }),
).pipe(Layer.provide(ServerConfig.layerTest(process.cwd(), scratch)));
const environment = Layer.mergeAll(
  Layer.succeed(ServerEnvironment.ServerEnvironmentIdentity, {
    getEnvironmentId: Effect.succeed(reconnectConfig.environment.environmentId),
  }),
  Layer.succeed(ServerEnvironment.ServerEnvironment, {
    getEnvironmentId: Effect.succeed(reconnectConfig.environment.environmentId),
    getDescriptor: Effect.succeed(reconnectConfig.environment),
  }),
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
const ws = layerWebSocketRoute((session) =>
  Effect.gen(function* () {
    const { protocol, httpEffect } = yield* RpcServer.makeProtocolWithHttpEffectWebsocket;
    yield* RpcServer.make(ServerWsRpcGroup, WS_RPC_SERVER_OPTIONS).pipe(
      Effect.provideService(RpcServer.Protocol, protocol),
      Effect.provide(RpcAuthorization.layer(session.scopes)),
      Effect.forkScoped,
    );
    // @effect-diagnostics-next-line returnEffectInGen:off -- The upgrade route owns running this HTTP effect.
    return httpEffect;
  }).pipe(Effect.provide(handlers.pipe(Layer.provideMerge(RpcSerialization.layerJson)))),
);
class Api extends HttpApi.make("environment").add(
  EnvironmentHttpApi.groups.orchestration,
  EnvironmentHttpApi.groups.auth,
  EnvironmentHttpApi.groups.metadata,
) {}
const routes = Layer.mergeAll(
  ws,
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
      const context = yield* Layer.buildWithScope(auth, scope);
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
      let messages = 100;
      yield* sink.write({
        events: [operationThreadCreated, ...operationMessageEvents(0, messages)],
      });
      const snapshot = yield* projections.getThreadSnapshot(operationThreadId);
      const listener = layerNodeHttpServer({ host: "0.0.0.0", port }, () => {
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
        const record = (request: NodeHttp.IncomingMessage, response?: NodeHttp.ServerResponse) => {
          if (!connections.has(request.socket)) connections.set(request.socket, ++nextConnection);
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
        return server;
      });
      yield* Layer.buildWithScope(
        HttpRouter.serve(routes, { disableListenLog: true }).pipe(
          Layer.provide(listener),
          Layer.provide(Layer.succeedContext(context)),
        ),
        scope,
      );
      const requests = yield* Queue.unbounded<unknown>();
      process.on("message", (message) => Queue.offerUnsafe(requests, message));
      process.send?.(
        encodeReady({
          type: "ready",
          bearer: paired.access_token,
          initial: {
            startedAt: 0,
            sequence: snapshot.snapshotSequence,
            messages,
            windowMessageIds: [],
            windowRunIds: [],
            hasMoreHistory: false,
            latestLocalTurnOrdinal: null,
            initialSnapshot: {
              snapshotSequence: snapshot.snapshotSequence,
              projection: projectThreadProjectionForWire(snapshot.projection),
            },
          },
        }),
      );
      for (;;) {
        const raw = yield* Queue.take(requests);
        if (typeof raw === "object" && raw !== null && "inspect" in raw) {
          process.send?.({ serverRequests });
          continue;
        }
        const request = decodeRequest(raw);
        if (request.count) {
          yield* sink.write({ events: operationMessageEvents(messages, request.count) });
          messages += request.count;
        }
        const full = yield* projections.getThreadSnapshot(operationThreadId);
        process.send?.(
          encodeResponse({
            startedAt: 0,
            sequence: full.snapshotSequence,
            messages,
            windowMessageIds: [],
            windowRunIds: [],
            hasMoreHistory: false,
            latestLocalTurnOrdinal: null,
          }),
        );
      }
    }),
  ).pipe(
    Effect.provide(NodeServices.layer),
    Effect.provideService(Logger.CurrentLoggers, new Set()),
  ),
);
