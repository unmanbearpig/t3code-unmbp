// @effect-diagnostics nodeBuiltinImport:off - Isolated benchmark fixture, never an app server.
import * as NodeFS from "node:fs";
import * as NodeHttps from "node:https";
import * as NodeSocketServer from "@effect/platform-node/NodeSocketServer";
import { Http3Server, quicheLoaded } from "@fails-components/webtransport";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { RpcServer } from "effect/rpc";
import * as SocketServer from "effect/socket/SocketServer";
import * as Stream from "effect/Stream";

import { operationMessageText } from "../../network/operationsFixture.ts";
import { ports, quicFlowControl, wallClockMs } from "../../network/protocol.ts";
import { BenchRpcs, layerSocketServer, makeWebTransportSocketServer } from "../../network/rpc.ts";
import { layerDeflatedJson } from "../../network/rpcDeflate.ts";

const [certPath, keyPath] = process.argv.slice(2);
if (!certPath || !keyPath) throw new Error("Expected certificate and key paths");
const cert = NodeFS.readFileSync(certPath, "utf8");
const key = NodeFS.readFileSync(keyPath, "utf8");
await quicheLoaded;
const quic = new Http3Server({
  host: "127.0.0.1",
  port: ports.wt,
  secret: "same-codec-control",
  cert,
  privKey: key,
  ...quicFlowControl,
});
quic.startServer();
await quic.ready;
const handlers = BenchRpcs.toLayer({
  ping: ({ text }) => Effect.succeed(text),
  subscribe: () => Stream.empty,
  snapshot: ({ messages, bytes }) =>
    Effect.sync(() => ({
      sentAt: wallClockMs(),
      messages: Array.from({ length: messages }, (_, index) => ({
        id: `message-${index}`,
        role: index % 2 === 0 ? "user" : "assistant",
        text: operationMessageText(index).slice(0, bytes),
        createdAt: "2026-10-14T00:00:00.000Z",
      })),
    })),
});
const wss = Layer.effect(
  SocketServer.SocketServer,
  Effect.gen(function* () {
    const server = yield* Effect.acquireRelease(
      Effect.sync(() =>
        NodeHttps.createServer({ cert, key, minVersion: "TLSv1.3", maxVersion: "TLSv1.3" }),
      ),
      (server) =>
        Effect.sync(() => {
          server.closeAllConnections();
          server.close();
        }),
    );
    const [socket] = yield* Effect.all(
      [
        NodeSocketServer.makeWebSocket({ server, perMessageDeflate: false }),
        Effect.callback<void>((resume) => {
          server.once("error", (cause) => resume(Effect.die(cause)));
          server.listen(ports.rpcWss, "127.0.0.1", () => resume(Effect.void));
        }),
      ],
      { concurrency: "unbounded" },
    );
    return socket;
  }),
);
const rpc = () =>
  RpcServer.layer(BenchRpcs, { disableTracing: true, disableFatalDefects: true }).pipe(
    Layer.provide(handlers),
    Layer.provide(Layer.fresh(RpcServer.layerProtocolSocketServer)),
    Layer.provide(layerDeflatedJson),
  );
await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const scope = yield* Effect.scope;
      yield* Layer.buildWithScope(
        Layer.mergeAll(
          rpc().pipe(Layer.provide(wss)),
          rpc().pipe(
            Layer.provide(
              layerSocketServer(makeWebTransportSocketServer(quic.sessionStream("/rpc"), ports.wt)),
            ),
          ),
        ),
        scope,
      );
      process.send?.("ready");
      return yield* Effect.never;
    }),
  ),
);
