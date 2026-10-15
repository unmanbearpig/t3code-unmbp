// @effect-diagnostics nodeBuiltinImport:off -- Isolated TLS and loopback UDP listener fixtures.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeDgram from "node:dgram";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { ORCHESTRATION_PROTOCOL_VERSION, AuthSessionId, EnvironmentId } from "@t3tools/contracts";
import { wrapStandardWebTransportSession } from "@t3tools/shared/rpcWebTransport";
import {
  makeRpcWebTransportClientProtocol,
  prepareRpcWebTransportClient,
} from "../../../../packages/client-runtime/src/rpc/webTransportProtocol.ts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as Queue from "effect/Queue";
import { Rpc, RpcClient, RpcGroup, RpcServer } from "effect/rpc";

import * as AuthenticatedRpcSession from "../AuthenticatedRpcSession.ts";
import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as SessionStore from "../auth/SessionStore.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as BackgroundPolicy from "../background/BackgroundPolicy.ts";
import * as HostPowerMonitor from "../background/HostPowerMonitor.ts";
import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as RpcSessionLifetime from "../RpcSessionLifetime.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ServerRuntimeStartup from "../serverRuntimeStartup.ts";
import * as AnalyticsService from "../telemetry/AnalyticsService.ts";
import * as WebTransportAvailability from "./WebTransportAvailability.ts";
import * as WebTransportBackend from "./WebTransportBackend.ts";
import * as WebTransportListener from "./WebTransportListener.ts";
import { parseWebTransportConfig, WebTransportUnavailableError } from "./WebTransportConfig.ts";

const validConfig = {
  url: "https://localhost:4443/rpc",
  host: "127.0.0.1",
  port: 4443,
  certPath: "/cert.pem",
  keyPath: "/key.pem",
};

it("validates the entire opt-in and rejects unsafe or incomplete advertisements", () => {
  expect(parseWebTransportConfig(JSON.stringify(validConfig))).toEqual(validConfig);
  for (const patch of [
    { url: "http://localhost/rpc" },
    { url: "https://user@localhost/rpc" },
    { url: "https://@localhost/rpc" },
    { url: "HTTPS://@localhost/rpc" },
    { url: " https://@localhost/rpc" },
    { url: "https://localhost/rpc?" },
    { url: "https://localhost/rpc#" },
    { url: "https://localhost/rpc?ticket=x" },
    { url: "https://localhost/rpc#x" },
    { host: " " },
    { port: 0 },
    { port: 65536 },
    { port: 1.5 },
    { certPath: "" },
    { keyPath: "" },
  ])
    expect(() => parseWebTransportConfig(JSON.stringify({ ...validConfig, ...patch }))).toThrow();
  for (const value of ["not json", "{}", '{"url":"https://localhost/rpc"}'])
    expect(() => parseWebTransportConfig(value)).toThrow();
});

const certificates = Effect.fnUntraced(function* () {
  const directory = yield* Effect.acquireRelease(
    Effect.promise(() =>
      NodeFSP.mkdtemp(NodePath.join(NodeOS.homedir(), "codex-tmp", "t3-quic-listener-")),
    ),
    (dir) => Effect.promise(() => NodeFSP.rm(dir, { recursive: true, force: true })),
  );
  const certPath = NodePath.join(directory, "cert.pem");
  const keyPath = NodePath.join(directory, "key.pem");
  yield* Effect.sync(() =>
    NodeChildProcess.execFileSync(
      "openssl",
      [
        "req",
        "-x509",
        "-newkey",
        "ec",
        "-pkeyopt",
        "ec_paramgen_curve:prime256v1",
        "-nodes",
        "-keyout",
        keyPath,
        "-out",
        certPath,
        "-days",
        "10",
        "-subj",
        "/CN=localhost",
        "-addext",
        "subjectAltName=DNS:localhost,IP:127.0.0.1",
      ],
      { stdio: "ignore" },
    ),
  );
  const cert = yield* Effect.promise(() => NodeFSP.readFile(certPath, "utf8"));
  return { certPath, keyPath, cert };
});

const freeUdpPort = Effect.promise(
  () =>
    new Promise<number>((resolve, reject) => {
      const socket = NodeDgram.createSocket("udp4");
      socket.once("error", reject);
      socket.bind(0, "127.0.0.1", () => {
        const address = socket.address();
        socket.close(() => resolve(address.port));
      });
    }),
);

const configLayer = (raw: string | undefined) =>
  Layer.effect(
    ServerConfig.ServerConfig,
    Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      return { ...config, ...(raw === undefined ? {} : { rpcWebTransport: raw }) };
    }),
  ).pipe(
    Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-quic-listener-state-" })),
  );
const startupLayer = Layer.mock(ServerRuntimeStartup.ServerRuntimeStartup)({
  awaitCommandReady: Effect.void,
});

it.effect(
  "does not load the optional backend when disabled, malformed, or missing certificate files",
  () =>
    Effect.gen(function* () {
      for (const raw of [
        undefined,
        "{",
        JSON.stringify({ url: validConfig.url }),
        JSON.stringify(validConfig),
      ]) {
        let loads = 0;
        yield* Effect.gen(function* () {
          const listener = yield* WebTransportListener.WebTransportListener;
          yield* listener.listen;
          const availability = yield* WebTransportAvailability.WebTransportAvailability;
          expect(Option.isNone(yield* availability.get)).toBe(true);
        }).pipe(
          Effect.scoped,
          Effect.provide(
            WebTransportListener.layer.pipe(
              Layer.provideMerge(WebTransportAvailability.layer),
              Layer.provide(
                Layer.succeed(WebTransportBackend.WebTransportBackend, {
                  load: Effect.sync(() => {
                    loads++;
                    throw new Error("disabled backend loaded");
                  }),
                }),
              ),
              Layer.provide(Layer.mock(AuthenticatedRpcSession.AuthenticatedRpcSession)({})),
              Layer.provide(Layer.mock(EnvironmentAuth.EnvironmentAuth)({})),
              Layer.provide(
                Layer.mock(ServerEnvironment.ServerEnvironmentIdentity)({
                  getEnvironmentId: Effect.succeed(EnvironmentId.make("unused")),
                }),
              ),
              Layer.provide(startupLayer),
              Layer.provide(configLayer(raw)),
            ),
          ),
        );
        expect(loads).toBe(0);
      }
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "disables only the listener for a mismatched key, hostname, expiry, or unavailable backend",
  () =>
    Effect.gen(function* () {
      const cert = yield* certificates();
      const other = yield* certificates();
      const x509 = new NodeCrypto.X509Certificate(cert.cert);
      for (const stage of ["hostname", "key", "expiry", "backend"] as const) {
        yield* TestClock.setTime(
          Date.parse(stage === "expiry" ? x509.validTo : x509.validFrom) + 1000,
        );
        const raw = JSON.stringify({
          ...validConfig,
          ...cert,
          ...(stage === "hostname" ? { url: "https://different.example:4443/rpc" } : {}),
          ...(stage === "key" ? { keyPath: other.keyPath } : {}),
        });
        let loads = 0;
        yield* Effect.gen(function* () {
          const listener = yield* WebTransportListener.WebTransportListener;
          yield* listener.listen;
          const availability = yield* WebTransportAvailability.WebTransportAvailability;
          expect(Option.isNone(yield* availability.get)).toBe(true);
        }).pipe(
          Effect.provide(
            WebTransportListener.layer.pipe(
              Layer.provideMerge(WebTransportAvailability.layer),
              Layer.provide(
                Layer.succeed(WebTransportBackend.WebTransportBackend, {
                  load: Effect.sync(() => {
                    loads++;
                  }).pipe(
                    Effect.andThen(
                      Effect.fail(new WebTransportUnavailableError({ stage: "backend" })),
                    ),
                  ),
                }),
              ),
              Layer.provide(Layer.mock(AuthenticatedRpcSession.AuthenticatedRpcSession)({})),
              Layer.provide(Layer.mock(EnvironmentAuth.EnvironmentAuth)({})),
              Layer.provide(
                Layer.mock(ServerEnvironment.ServerEnvironmentIdentity)({
                  getEnvironmentId: Effect.succeed(EnvironmentId.make("fixture")),
                }),
              ),
              Layer.provide(startupLayer),
              Layer.provide(configLayer(raw)),
            ),
          ),
        );
        expect(loads).toBe(stage === "backend" ? 1 : 0);
      }
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

const Group = RpcGroup.make(
  Rpc.make("server.probe", { payload: Schema.Struct({}), success: Schema.String }),
  Rpc.make("listen", { success: Schema.String, stream: true }),
);

it.live(
  "authenticates real CONNECT tickets, serves RPC, bounds a missing control stream, and closes revoked sessions",
  (test) =>
    Effect.gen(function* () {
      const native = yield* WebTransportBackend.WebTransportBackend;

      const available = yield* native.load.pipe(Effect.option);
      if (Option.isNone(available)) return test.skip("Optional QUIC backend unavailable");
      const { WebTransport } = yield* Effect.promise(
        () => import("@fails-components/webtransport"),
      );

      const cert = yield* certificates();

      const port = yield* freeUdpPort;
      const config = { ...validConfig, ...cert, port, url: `https://127.0.0.1:${port}/rpc` };
      const published = yield* Deferred.make<void>();
      const stopped = yield* Deferred.make<void>();
      const interrupted = yield* Deferred.make<void>();
      const handlerStarted = yield* Deferred.make<void>();
      const acceptedOrigins: unknown[] = [];
      const endpointRef = yield* Ref.make(Option.none<{ url: string; wireVersion: number }>());
      const availabilityLayer = Layer.succeed(WebTransportAvailability.WebTransportAvailability, {
        get: Ref.get(endpointRef),
        set: (endpoint) =>
          Ref.set(endpointRef, endpoint).pipe(
            Effect.andThen(
              Option.isSome(endpoint)
                ? Deferred.succeed(published, undefined)
                : Deferred.succeed(stopped, undefined),
            ),
            Effect.asVoid,
          ),
      });
      const authLayer = EnvironmentAuth.layer.pipe(
        Layer.provideMerge(SqlitePersistence.layerMemory),
        Layer.provide(ServerSecretStore.layer),
        Layer.provideMerge(ServerEnvironment.layerIdentity),
      );
      const lifetimeLayer = RpcSessionLifetime.layer.pipe(
        Layer.provideMerge(
          BackgroundPolicy.layer.pipe(
            Layer.provide(Layer.effect(HostPowerMonitor.HostPowerMonitor, HostPowerMonitor.make())),
            Layer.provide(ServerSettings.layerTest()),
          ),
        ),
        Layer.provide(AnalyticsService.AnalyticsService.layerTest),
      );
      const applicationSessionLayer = Layer.effect(
        AuthenticatedRpcSession.AuthenticatedRpcSession,
        Effect.gen(function* () {
          const lifetime = yield* RpcSessionLifetime.RpcSessionLifetime;
          return AuthenticatedRpcSession.AuthenticatedRpcSession.of({
            run: ({
              session,
              protocol,
              lifetime: transportLifetime,
              origin,
              analyticsProps: props,
            }) =>
              lifetime.run(session, origin ?? {}, props ?? {}, () =>
                Effect.scoped(
                  Effect.gen(function* () {
                    acceptedOrigins.push(origin);
                    yield* RpcServer.make(Group).pipe(
                      Effect.provideService(RpcServer.Protocol, protocol),
                      Effect.provide(
                        Group.toLayer({
                          "server.probe": () => Effect.succeed("alive"),
                          listen: () =>
                            Stream.fromEffect(Deferred.succeed(handlerStarted, undefined)).pipe(
                              Stream.flatMap(() => Stream.never),
                              Stream.ensuring(Deferred.succeed(interrupted, undefined)),
                            ),
                        }),
                      ),
                      Effect.forkScoped,
                    );
                    return yield* transportLifetime;
                  }),
                ),
              ),
          });
        }),
      ).pipe(Layer.provideMerge(lifetimeLayer));

      yield* Effect.gen(function* () {
        const auth = yield* EnvironmentAuth.EnvironmentAuth;
        const sessions = yield* SessionStore.SessionStore;
        const listener = yield* WebTransportListener.WebTransportListener;
        const identity = yield* ServerEnvironment.ServerEnvironmentIdentity;
        const listenerFiber = yield* listener.listen.pipe(Effect.forkScoped);

        yield* Deferred.await(published);
        expect(Option.getOrThrow(yield* Ref.get(endpointRef)).url).toBe(config.url);
        const issued = yield* auth.issueSession();
        const ticket = yield* auth.issueWebSocketTicket(issued);
        const hashes = [
          {
            algorithm: "sha-256",
            value: new Uint8Array(
              NodeCrypto.createHash("sha256")
                .update(new NodeCrypto.X509Certificate(cert.cert).raw)
                .digest(),
            ),
          },
        ];
        const connect = (ticketValue: string) =>
          new WebTransport(
            `${config.url}?wsTicket=${encodeURIComponent(ticketValue)}&clientSurface=web&clientAppVersion=test`,
            {
              requireUnreliable: true,
              serverCertificateHashes: hashes,
            },
          );

        const rejected = connect("invalid");
        void rejected.closed.catch(() => undefined);
        expect(
          yield* Effect.promise(() =>
            rejected.ready.then(
              () => false,
              () => true,
            ),
          ),
        ).toBe(true);

        const idle = connect(ticket.ticket);
        void idle.closed.catch(() => undefined);

        yield* Effect.promise(() => idle.ready);
        // No control stream is ever created. The listener owns the whole initial deadline.
        yield* Effect.promise(() =>
          idle.closed.then(
            () => undefined,
            () => undefined,
          ),
        );

        const revoked = yield* auth.issueSession();
        const revokedTicket = yield* auth.issueWebSocketTicket(revoked);
        const admittedBeforeRevoke = connect(revokedTicket.ticket);
        void admittedBeforeRevoke.closed.catch(() => undefined);
        yield* Effect.promise(() => admittedBeforeRevoke.ready);
        yield* sessions.revoke(revoked.sessionId);
        const rejectedPrepare = yield* prepareRpcWebTransportClient(
          wrapStandardWebTransportSession(admittedBeforeRevoke),
          {
            environmentId: yield* identity.getEnvironmentId,
            orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
          },
        ).pipe(Effect.scoped, Effect.option);
        expect(Option.isNone(rejectedPrepare)).toBe(true);
        expect(acceptedOrigins).toHaveLength(0);
        const raw = connect(ticket.ticket);
        void raw.closed.catch(() => undefined);

        yield* Effect.promise(() => raw.ready);
        const wrapped = wrapStandardWebTransportSession(raw);
        yield* Effect.addFinalizer(() => Effect.sync(() => wrapped.close(0)));

        const prepared = yield* prepareRpcWebTransportClient(wrapped, {
          environmentId: yield* identity.getEnvironmentId,
          orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
        });
        const protocol = yield* makeRpcWebTransportClientProtocol(prepared);
        const client = yield* RpcClient.make(Group).pipe(
          Effect.provideService(RpcClient.Protocol, protocol),
        );

        expect(yield* client["server.probe"]({})).toBe("alive");
        expect(acceptedOrigins).toEqual([{ surface: "web", appVersion: "test" }]);
        yield* client.listen().pipe(Stream.runDrain, Effect.ignore, Effect.forkScoped);
        yield* Deferred.await(handlerStarted);
        expect(
          (yield* sessions.listActive()).find((entry) => entry.sessionId === issued.sessionId)
            ?.connected,
        ).toBe(true);

        yield* sessions.revoke(issued.sessionId);
        yield* Deferred.await(interrupted);
        yield* Effect.promise(() =>
          raw.closed.then(
            () => undefined,
            () => undefined,
          ),
        );

        yield* Fiber.interrupt(listenerFiber);
        expect(Option.isNone(yield* Ref.get(endpointRef))).toBe(true);
      }).pipe(
        Effect.provide(
          WebTransportListener.layer.pipe(
            Layer.provideMerge(applicationSessionLayer),
            Layer.provideMerge(authLayer),
            Layer.provide(availabilityLayer),
            Layer.provide(startupLayer),
            Layer.provide(configLayer(JSON.stringify(config))),
          ),
        ),
      );
    }).pipe(
      Effect.scoped,
      Effect.provide(Layer.merge(WebTransportBackend.layer, NodeServices.layer)),
    ),
);

it.effect(
  "publishes only a ready listener, caps sessions/admissions, and clears discovery on failure",
  () =>
    Effect.gen(function* () {
      const cert = yield* certificates();
      yield* TestClock.setTime(
        Date.parse(new NodeCrypto.X509Certificate(cert.cert).validFrom) + 1000,
      );
      const published = yield* Deferred.make<void>();
      const activeAuth = yield* Queue.make<void>();
      const authGate = yield* Deferred.make<EnvironmentAuth.AuthenticatedSession>();
      const endpoint = yield* Ref.make(Option.none<{ url: string; wireVersion: number }>());
      let callbackResults = 0;
      const created = Promise.withResolvers<FakeNativeServer>();
      class FakeNativeServer {
        readonly readiness = Promise.withResolvers<void>();
        readonly ending = Promise.withResolvers<void>();
        readonly ready = this.readiness.promise;
        readonly closed = this.ending.promise;
        request!: (input: { header: unknown }) => Promise<{ status: number }>;
        controller!: ReadableStreamDefaultController<unknown>;
        readonly incoming = new ReadableStream<unknown>({
          start: (controller) => {
            this.controller = controller;
          },
        });
        readonly options: unknown;
        constructor(options: unknown) {
          this.options = options;
          created.resolve(this);
        }
        startServer() {}
        resultsAtStop = 0;
        stopServer() {
          this.resultsAtStop = callbackResults;
          this.controller.close();
        }
        setRequestCallback(callback: typeof this.request) {
          this.request = callback;
        }
        sessionStream() {
          return this.incoming;
        }
      }
      const admittedSession = {
        sessionId: AuthSessionId.make("admission-session"),
        subject: "test",
        method: "bearer-access-token" as const,
        scopes: ["orchestration:read"] as const,
      };
      const nativeLayer = Layer.succeed(WebTransportBackend.WebTransportBackend, {
        load: Effect.succeed({
          Http3Server:
            FakeNativeServer as unknown as typeof import("@fails-components/webtransport").Http3Server,
        }),
      });
      yield* Effect.gen(function* () {
        const listener = yield* WebTransportListener.WebTransportListener;
        const fiber = yield* listener.listen.pipe(Effect.forkScoped);
        const native = yield* Effect.promise(() => created.promise);
        expect(Option.isNone(yield* Ref.get(endpoint))).toBe(true);
        expect(native.options).toMatchObject({
          initialBidirectionalStreams: 80,
          initialBidirectionalReceiveStreams: 80,
          initialBidirectionalSendStreams: 80,
        });
        native.readiness.resolve();
        yield* Deferred.await(published);
        const request = (ticket = "valid") =>
          native.request({ header: { ":path": `/rpc?wsTicket=${ticket}` } });
        for (let i = 0; i < 64; i++)
          expect((yield* Effect.promise(() => request())).status).toBe(200);
        expect((yield* Effect.promise(() => request())).status).toBe(403);
        yield* TestClock.adjust("2 seconds");
        expect((yield* Effect.promise(() => request())).status).toBe(200);
        const pending = Array.from({ length: 16 }, () => request("slow"));
        for (let i = 0; i < 16; i++) yield* Queue.take(activeAuth);
        expect((yield* Effect.promise(() => request())).status).toBe(403);
        yield* TestClock.adjust("2 seconds");
        expect(
          (yield* Effect.promise(() => Promise.all(pending))).map((value) => value.status),
        ).toEqual(Array(16).fill(403));
        expect((yield* Effect.promise(() => request())).status).toBe(200);
        const cancelling = [request("slow"), request("slow")];
        for (const promise of cancelling)
          void promise.then(() => {
            callbackResults++;
          });
        yield* Queue.take(activeAuth);
        yield* Queue.take(activeAuth);
        native.ending.reject(new Error("fixture listener failure"));
        yield* Fiber.join(fiber);
        expect(
          (yield* Effect.promise(() => Promise.all(cancelling))).map((result) => result.status),
        ).toEqual([403, 403]);
        expect(native.resultsAtStop).toBe(2);
        expect(Option.isNone(yield* Ref.get(endpoint))).toBe(true);
      }).pipe(
        Effect.provide(
          WebTransportListener.layer.pipe(
            Layer.provide(nativeLayer),
            Layer.provide(
              Layer.succeed(WebTransportAvailability.WebTransportAvailability, {
                get: Ref.get(endpoint),
                set: (value) =>
                  Ref.set(endpoint, value).pipe(
                    Effect.andThen(
                      Option.isSome(value) ? Deferred.succeed(published, undefined) : Effect.void,
                    ),
                    Effect.asVoid,
                  ),
              }),
            ),
            Layer.provide(Layer.mock(AuthenticatedRpcSession.AuthenticatedRpcSession)({})),
            Layer.provide(
              Layer.mock(EnvironmentAuth.EnvironmentAuth)({
                authenticateRpcTicket: (ticket) =>
                  ticket === "slow"
                    ? Queue.offer(activeAuth, undefined).pipe(
                        Effect.andThen(Deferred.await(authGate)),
                      )
                    : Effect.succeed(admittedSession),
              }),
            ),
            Layer.provide(
              Layer.mock(ServerEnvironment.ServerEnvironmentIdentity)({
                getEnvironmentId: Effect.succeed(EnvironmentId.make("fixture")),
              }),
            ),
            Layer.provide(startupLayer),
            Layer.provide(configLayer(JSON.stringify({ ...validConfig, ...cert }))),
          ),
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
