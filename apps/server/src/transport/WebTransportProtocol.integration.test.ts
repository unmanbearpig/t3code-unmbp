// @effect-diagnostics nodeBuiltinImport:off - Isolated native QUIC integration fixture.
import { describe, expect, it } from "@effect/vitest";
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import {
  RPC_WEBTRANSPORT_LIMITS,
  createTransportByteBudget,
  wrapStandardWebTransportSession,
} from "@t3tools/shared/rpcWebTransport";
import {
  prepareRpcWebTransportClient,
  makeRpcWebTransportClientProtocol,
} from "../../../../packages/client-runtime/src/rpc/webTransportProtocol.ts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Exit from "effect/Exit";
import type * as RpcMessage from "effect/rpc/RpcMessage";
import { Rpc, RpcClient, RpcGroup, RpcServer } from "effect/rpc";
import { prepareRpcWebTransportServer } from "./WebTransportProtocol.ts";
const optionalNativeBackend = "@fails-components/webtransport-transport-http3-quiche";

const openFixture = Effect.fnUntraced(function* () {
  const { Http3Server, WebTransport, quicheLoaded } = yield* Effect.promise(
    () => import("@fails-components/webtransport"),
  );
  yield* Effect.promise(() => quicheLoaded);
  const directory = yield* Effect.acquireRelease(
    Effect.promise(() =>
      NodeFSP.mkdtemp(NodePath.join(NodeOS.homedir(), "codex-tmp", "t3-quic-wire-test-")),
    ),
    (path) => Effect.promise(() => NodeFSP.rm(path, { recursive: true, force: true })),
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
  const privKey = yield* Effect.promise(() => NodeFSP.readFile(keyPath, "utf8"));
  const server = new Http3Server({
    host: "127.0.0.1",
    port: 0,
    cert,
    privKey,
    secret: "isolated-wire-test",
    // CONNECT and control also need native QUIC credit. Application credit stays 64.
    initialBidirectionalStreams: 80,
    initialBidirectionalReceiveStreams: 80,
    initialBidirectionalSendStreams: 80,
  });
  const sessions = server.sessionStream("/rpc").getReader();
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      server.stopServer();
    }),
  );
  server.startServer();
  yield* Effect.promise(() => server.ready);
  const clientOptions = {
    requireUnreliable: true,
    serverCertificateHashes: [
      {
        algorithm: "sha-256",
        value: new Uint8Array(
          NodeCrypto.createHash("sha256").update(new NodeCrypto.X509Certificate(cert).raw).digest(),
        ),
      },
    ],
    initialBidirectionalReceiveStreams: 80,
    initialBidirectionalSendStreams: 80,
  };
  const client = new WebTransport(`https://127.0.0.1:${server.address()!.port}/rpc`, clientOptions);
  void client.closed.catch(() => undefined);
  const clientSession = wrapStandardWebTransportSession(client);
  yield* Effect.addFinalizer(() => Effect.sync(() => clientSession.close(0)));
  yield* Effect.promise(() => client.ready);
  const accepted = yield* Effect.promise(() => sessions.read());
  if (accepted.done || !accepted.value) return yield* Effect.die("No accepted QUIC session");
  void accepted.value.closed.catch(() => undefined);
  return { client: clientSession, server: wrapStandardWebTransportSession(accepted.value) };
});

describe("native QUIC protocol", () => {
  it.live(
    "completes a probe with all 63 ordinary streams occupied and transfers a >16 MiB response",
    (context) =>
      Effect.gen(function* () {
        const available = yield* Effect.promise(() =>
          import(optionalNativeBackend).then(
            () => true,
            () => false,
          ),
        );
        if (!available)
          return context.skip("Optional native QUIC backend is unavailable on this host");
        const pair = yield* openFixture();
        const expected = { environmentId: "native-fixture", orchestrationProtocolVersion: 1 };
        const preparedServer = yield* prepareRpcWebTransportServer(pair.server, {
          ...expected,
          budgets: {
            input: createTransportByteBudget(RPC_WEBTRANSPORT_LIMITS.listenerInputBytes),
            output: createTransportByteBudget(RPC_WEBTRANSPORT_LIMITS.listenerOutputBytes),
          },
        }).pipe(Effect.forkScoped);
        const preparedClient = yield* prepareRpcWebTransportClient(pair.client, expected).pipe(
          Effect.forkScoped,
        );
        const server = yield* Fiber.join(preparedServer);
        const arrived = yield* Queue.make<string | number>();
        const serverSawClientFin = yield* Deferred.make<void>();
        // These 18 MiB of base64 data do not fit the <=8 KiB compression threshold
        // per record, exercising reliable transport backpressure with identity records.
        const large = NodeCrypto.randomBytes(13 * 1024 * 1024).toString("base64");
        yield* server.protocol
          .run((_id, request) => {
            if (request._tag === "Request") {
              if (request.tag === "server.probe")
                return server.protocol.send(0, {
                  _tag: "Exit",
                  requestId: request.id,
                  exit: { _tag: "Success", value: "alive" },
                });
              if (request.tag === "large")
                return server.protocol.send(0, {
                  _tag: "Exit",
                  requestId: request.id,
                  exit: { _tag: "Success", value: large },
                });
              return Queue.offer(arrived, request.id).pipe(Effect.asVoid);
            }
            if (request._tag === "Interrupt")
              return server.protocol
                .send(0, {
                  _tag: "Exit",
                  requestId: request.requestId,
                  exit: { _tag: "Success", value: null },
                })
                .pipe(
                  Effect.andThen(Deferred.succeed(serverSawClientFin, undefined)),
                  Effect.asVoid,
                );
            return Effect.void;
          })
          .pipe(Effect.forkScoped);
        const prepared = yield* Fiber.join(preparedClient);
        const client = yield* makeRpcWebTransportClientProtocol(prepared);
        const responses = yield* Queue.make<RpcMessage.FromServerEncoded>();
        yield* client
          .run(1, (response) => Queue.offer(responses, response).pipe(Effect.asVoid))
          .pipe(Effect.forkScoped);
        for (let id = 0; id < 63; id++) {
          yield* client.send(1, { _tag: "Request", id, tag: "listen", payload: null, headers: [] });
          expect(yield* Queue.take(arrived)).toBe(id);
        }
        yield* client.send(1, {
          _tag: "Request",
          id: "probe",
          tag: "server.probe",
          payload: null,
          headers: [],
        });
        expect(yield* Queue.take(responses)).toMatchObject({
          _tag: "Exit",
          requestId: "probe",
          exit: { value: "alive" },
        });
        yield* client.send(1, { _tag: "Interrupt", requestId: 0 });
        expect(yield* Queue.take(responses)).toMatchObject({ _tag: "Exit", requestId: 0 });
        yield* client.send(1, {
          _tag: "Request",
          id: "large",
          tag: "large",
          payload: null,
          headers: [],
        });
        const result = yield* Queue.take(responses);
        if (result._tag === "ClientProtocolError") return yield* Effect.die(result.error);
        expect(result._tag).toBe("Exit");
        if (result._tag === "Exit" && result.exit._tag === "Success") {
          expect(typeof result.exit.value).toBe("string");
          expect(
            NodeCrypto.createHash("sha256").update(String(result.exit.value)).digest("hex"),
          ).toBe(NodeCrypto.createHash("sha256").update(large).digest("hex"));
        }
        yield* Deferred.await(serverSawClientFin);
        yield* server.close;
        yield* server.closed;
      }).pipe(Effect.scoped),
    30_000,
  );

  it.live(
    "ends the logical session and interrupts its real handler when the peer closes",
    (context) =>
      Effect.gen(function* () {
        const available = yield* Effect.promise(() =>
          import(optionalNativeBackend).then(
            () => true,
            () => false,
          ),
        );
        if (!available)
          return context.skip("Optional native QUIC backend is unavailable on this host");
        const pair = yield* openFixture();
        const expected = { environmentId: "peer-close-fixture", orchestrationProtocolVersion: 1 };
        const preparing = yield* prepareRpcWebTransportServer(pair.server, {
          ...expected,
          budgets: {
            input: createTransportByteBudget(RPC_WEBTRANSPORT_LIMITS.listenerInputBytes),
            output: createTransportByteBudget(RPC_WEBTRANSPORT_LIMITS.listenerOutputBytes),
          },
        }).pipe(Effect.forkScoped);
        const prepared = yield* prepareRpcWebTransportClient(pair.client, expected).pipe(
          Effect.forkScoped,
        );
        const server = yield* Fiber.join(preparing);
        const group = RpcGroup.make(Rpc.make("hold", { success: Schema.String }));
        const started = yield* Deferred.make<void>();
        const stopped = yield* Deferred.make<void>();
        yield* RpcServer.make(group).pipe(
          Effect.provide(
            group.toLayer({
              hold: () =>
                Deferred.succeed(started, undefined).pipe(
                  Effect.andThen(Effect.never),
                  Effect.onInterrupt(() => Deferred.succeed(stopped, undefined)),
                ),
            }),
          ),
          Effect.provideService(RpcServer.Protocol, server.protocol),
          Effect.forkScoped,
        );
        const protocol = yield* makeRpcWebTransportClientProtocol(yield* Fiber.join(prepared));
        const client = yield* RpcClient.make(group).pipe(
          Effect.provideService(RpcClient.Protocol, protocol),
        );
        const call = yield* client.hold().pipe(Effect.exit, Effect.forkScoped);
        yield* Deferred.await(started);
        pair.client.close(0);
        yield* server.closed;
        yield* Deferred.await(stopped);
        expect(Exit.isFailure(yield* Fiber.join(call))).toBe(true);
      }).pipe(Effect.scoped),
    10_000,
  );
});
