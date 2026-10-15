// @effect-diagnostics nodeBuiltinImport:off - Isolated benchmark server with an IPC fixture control channel.
import * as NodeFS from "node:fs";
import * as NodeHttp from "node:http";
import * as NodeHttps from "node:https";

import { makeResourceControl } from "./resourceUsage.ts";

import * as NodeHttpPlatform from "@effect/platform-node/NodeHttpPlatform";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSocketServer from "@effect/platform-node/NodeSocketServer";
import { Http3Server, quicheLoaded } from "@fails-components/webtransport";
import {
  AuthOrchestrationReadScope,
  AuthSessionId,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentHttpApi,
  ORCHESTRATION_V2_WS_METHODS,
  OrchestrationV2GetThreadProjectionError,
  OrchestrationV2RpcSchemas,
  WsSubscribeServerConfigRpc,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import { HttpRouter } from "effect/http";
import { Rpc, RpcGroup, RpcSerialization, RpcServer } from "effect/rpc";
import * as SocketServer from "effect/socket/SocketServer";

import * as SqlitePersistence from "../../src/persistence/Sqlite.ts";
import * as OrchestrationEventStore from "../../src/persistence/OrchestrationEventStore.ts";
import * as EventStore from "../../src/orchestration-v2/EventStore.ts";
import * as EventSink from "../../src/orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../../src/orchestration-v2/ProjectionStore.ts";
import * as ThreadManagementService from "../../src/orchestration-v2/ThreadManagementService.ts";
import * as ProjectStore from "../../src/orchestration-v2/ProjectStore.ts";
import * as ProjectEnrichmentService from "../../src/project/ProjectEnrichmentService.ts";
import * as OrchestrationHttp from "../../src/orchestration-v2/http.ts";
import * as ServerHttp from "../../src/http.ts";
import { subscribeOrchestrationV2Thread } from "../../src/AuthenticatedRpcSession.ts";
import {
  THREAD_HISTORY_PAGE_POLICY,
  THREAD_HISTORY_SNAPSHOT_ROW_LIMIT,
} from "../../src/orchestration-v2/threadHistoryPaging.ts";
import { buildBoundedThreadProjection } from "../../src/orchestration-v2/threadHistoryPaging.ts";
import { projectThreadProjectionForWire } from "../../src/orchestration-v2/WireProjection.ts";
import { layerSocketServer, makeWebTransportSocketServer } from "./rpc.ts";
import { operationRpcSerialization } from "./operationsRpc.ts";
import { ports, quicFlowControl, wallClockMs } from "./protocol.ts";
import {
  operationMessageEvents,
  operationServerConfig,
  operationSettings,
  operationThreadCreated,
  operationThreadId,
  OperationControlResponse,
} from "./operationsFixture.ts";

const [certPath, keyPath, workload, httpSocketPath] = process.argv.slice(2);
if (!certPath || !keyPath || !httpSocketPath)
  throw new Error("Usage: operationsServer.ts <cert> <key> <workload> <http-socket>");

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
const services = management.pipe(
  Layer.provideMerge(ProjectStore.layer),
  Layer.provideMerge(enrichment),
  Layer.provideMerge(persistence),
  Layer.provideMerge(NodeServices.layer),
);
const group = RpcGroup.make(
  WsSubscribeServerConfigRpc,
  Rpc.make(ORCHESTRATION_V2_WS_METHODS.subscribeThread, {
    payload: OrchestrationV2RpcSchemas.subscribeThread.input,
    success: OrchestrationV2RpcSchemas.subscribeThread.output,
    error: OrchestrationV2GetThreadProjectionError,
    stream: true,
  }),
);
const handlers = group.toLayer({
  subscribeServerConfig: () =>
    Stream.concat(
      Stream.make({
        version: 1 as const,
        type: "snapshot" as const,
        config: operationServerConfig,
      }),
      Stream.never,
    ),
  [ORCHESTRATION_V2_WS_METHODS.subscribeThread]: (input) =>
    Stream.unwrap(subscribeOrchestrationV2Thread(input)),
});
class OperationApi extends HttpApi.make("environment").add(
  EnvironmentHttpApi.groups.orchestration,
) {}
const auth = Layer.succeed(EnvironmentAuthenticatedAuth, (effect) =>
  effect.pipe(
    Effect.provideService(EnvironmentAuthenticatedPrincipal, {
      sessionId: AuthSessionId.make("network-benchmark-session"),
      subject: "benchmark",
      method: "browser-session-cookie",
      scopes: new Set([AuthOrchestrationReadScope]),
    }),
  ),
);
const httpRoutes = HttpApiBuilder.layer(OperationApi).pipe(
  Layer.provide(OrchestrationHttp.layer),
  Layer.provide(auth),
  Layer.provide(ServerHttp.layerHttpCompression),
  Layer.provide(NodeHttpPlatform.layer),
);
const http = HttpRouter.serve(httpRoutes, { disableListenLog: true }).pipe(
  Layer.provideMerge(NodeHttpServer.layer(NodeHttp.createServer, { path: httpSocketPath })),
);
const controlRequest = Schema.Struct({ count: Schema.Number });
const decodeControlRequest = Schema.decodeUnknownEffect(controlRequest);
const encodeControlResponse = Schema.encodeEffect(Schema.toCodecJson(OperationControlResponse));

await quicheLoaded;
const quic = new Http3Server({
  port: ports.wt,
  host: "127.0.0.1",
  secret: "netbench",
  cert: NodeFS.readFileSync(certPath, "utf8"),
  privKey: NodeFS.readFileSync(keyPath, "utf8"),
  ...quicFlowControl,
});
quic.startServer();
await quic.ready;

await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const scope = yield* Effect.scope;
      const context = yield* Layer.buildWithScope(services, scope);
      const sink = yield* EventSink.EventSinkV2.pipe(Effect.provide(context));
      const projections = yield* ProjectionStore.ProjectionStoreV2.pipe(Effect.provide(context));
      let messages =
        workload === "open-long"
          ? operationSettings.longThreadMessages
          : operationSettings.initialMessages;
      yield* sink.write({
        events: [operationThreadCreated, ...operationMessageEvents(0, messages)],
      });

      const rpcServer = (serialization: Layer.Layer<RpcSerialization.RpcSerialization>) =>
        RpcServer.layer(group, {
          disableTracing: true,
          disableFatalDefects: true,
        }).pipe(
          Layer.provide(handlers),
          Layer.provide(Layer.fresh(RpcServer.layerProtocolSocketServer)),
          Layer.provide(serialization),
        );
      const ws = (port: number, perMessageDeflate: boolean) =>
        Layer.effect(
          SocketServer.SocketServer,
          NodeSocketServer.makeWebSocket({ host: "127.0.0.1", port, perMessageDeflate }),
        );
      const wss = Layer.effect(
        SocketServer.SocketServer,
        Effect.gen(function* () {
          const server = yield* Effect.acquireRelease(
            Effect.sync(() =>
              NodeHttps.createServer({
                cert: NodeFS.readFileSync(certPath),
                key: NodeFS.readFileSync(keyPath),
                minVersion: "TLSv1.3",
                maxVersion: "TLSv1.3",
              }),
            ),
            (server) =>
              Effect.sync(() => {
                server.closeAllConnections();
                server.close();
              }),
          );
          const [socket] = yield* Effect.all(
            [
              NodeSocketServer.makeWebSocket({ server, perMessageDeflate: true }),
              Effect.callback<void>((resume) => {
                const fail = (error: Error) => resume(Effect.die(error));
                server.once("error", fail);
                server.listen(ports.rpcWss, "127.0.0.1", () => resume(Effect.void));
                return Effect.sync(() => server.off("error", fail));
              }),
            ],
            { concurrency: "unbounded" },
          );
          return socket;
        }),
      );
      yield* Layer.buildWithScope(
        Layer.mergeAll(
          http,
          rpcServer(RpcSerialization.layerJson).pipe(Layer.provide(ws(ports.rpcWs, true))),
          rpcServer(RpcSerialization.layerJson).pipe(Layer.provide(ws(ports.rpcWsRaw, false))),
          rpcServer(RpcSerialization.layerJson).pipe(Layer.provide(wss)),
          rpcServer(operationRpcSerialization).pipe(
            Layer.provide(
              layerSocketServer(makeWebTransportSocketServer(quic.sessionStream("/rpc"), ports.wt)),
            ),
          ),
        ).pipe(Layer.provide(Layer.succeedContext(context))),
        scope,
      );

      const requests = yield* Queue.unbounded<unknown>();
      const resourceControl = makeResourceControl();
      const offer = (message: unknown) => {
        if (!resourceControl(message)) Queue.offerUnsafe(requests, message);
      };
      process.on("message", offer);
      yield* Effect.addFinalizer(() => Effect.sync(() => process.off("message", offer)));
      process.send?.("ready");
      let firstControl = true;
      while (true) {
        const request = yield* decodeControlRequest(yield* Queue.take(requests));
        const startedAt = wallClockMs();
        if (request.count > 0) {
          yield* sink.write({ events: operationMessageEvents(messages, request.count) });
          messages += request.count;
        }
        const full = yield* projections.getThreadSnapshot(operationThreadId);
        const window = yield* projections.getThreadSnapshotWindow(operationThreadId, {
          rowLimit: THREAD_HISTORY_SNAPSHOT_ROW_LIMIT,
          userTurnLimit: THREAD_HISTORY_PAGE_POLICY.maxUserTurns,
        });
        const bounded = buildBoundedThreadProjection({
          ...window,
          projection: projectThreadProjectionForWire(window.projection),
        });
        const response: OperationControlResponse = {
          startedAt,
          sequence: full.snapshotSequence,
          messages,
          windowMessageIds: bounded.projection.messages.map((message) => message.id),
          windowRunIds: bounded.projection.runs.map((run) => run.id),
          hasMoreHistory: bounded.hasMoreHistory,
          latestLocalTurnOrdinal: bounded.latestLocalTurnOrdinal,
          ...(firstControl && workload !== "open-new" && workload !== "open-long"
            ? {
                initialSnapshot: {
                  snapshotSequence: full.snapshotSequence,
                  projection: projectThreadProjectionForWire(full.projection),
                },
              }
            : {}),
        };
        firstControl = false;
        process.send?.(yield* encodeControlResponse(response));
      }
    }),
  ).pipe(Effect.provideService(Logger.CurrentLoggers, new Set())),
);
