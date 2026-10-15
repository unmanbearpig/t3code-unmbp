// @effect-diagnostics nodeBuiltinImport:off globalTimers:off - Raw transports and send timers are what this bench measures.
// Bench server, spawned by run.ts inside the impaired network namespace. Speaks the protocol.ts
// frames over plain WebSocket, WebSocket over TLS, and WebTransport (HTTP/3 over QUIC). Every
// "pipe" (a WebSocket, or one WebTransport bidirectional stream) answers on itself, so the
// client decides whether channels share a pipe or get their own.
import * as NodeFS from "node:fs";
import * as NodeHttp from "node:http";
import * as NodeHttps from "node:https";

import * as NodeSocketServer from "@effect/platform-node/NodeSocketServer";
import { Http3Server, quicheLoaded } from "@fails-components/webtransport";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import { RpcSerialization, RpcServer } from "effect/rpc";
import * as SocketServer from "effect/socket/SocketServer";
import { WebSocketServer } from "ws";

import {
  decodeFrame,
  encodeFrame,
  Kind,
  lengthPrefixed,
  makeFrameSplitter,
  ports,
  quicFlowControl,
  wallClockMs,
} from "./protocol.ts";
import {
  BenchRpcs,
  layerBenchHandlers,
  layerFramedJson,
  layerSocketServer,
  makeWebTransportSocketServer,
} from "./rpc.ts";

const [certPath, keyPath] = process.argv.slice(2);
if (!certPath || !keyPath) throw new Error("Usage: server.ts <cert.pem> <key.pem>");
const cert = NodeFS.readFileSync(certPath, "utf8");
const key = NodeFS.readFileSync(keyPath, "utf8");

function handleFrame(bytes: Uint8Array, send: (frame: Uint8Array) => void) {
  const frame = decodeFrame(bytes);
  switch (frame.kind) {
    case Kind.ping:
      send(encodeFrame({ ...frame, kind: Kind.pong }));
      return;
    case Kind.bulkRequest:
      send(
        encodeFrame({
          kind: Kind.bulkMessage,
          channel: frame.channel,
          seq: 0,
          sentAt: wallClockMs(),
          arg: frame.arg,
          payloadBytes: frame.arg,
        }),
      );
      return;
    case Kind.streamRequest: {
      const count = frame.seq;
      const intervalMs = frame.payload[0] ?? 10;
      let seq = 0;
      const timer = setInterval(() => {
        send(
          encodeFrame({
            kind: Kind.streamMessage,
            channel: frame.channel,
            seq,
            sentAt: wallClockMs(),
            arg: count,
            payloadBytes: frame.arg,
          }),
        );
        seq += 1;
        if (seq === count) clearInterval(timer);
      }, intervalMs);
      return;
    }
  }
}

function serveWebSockets(server: NodeHttp.Server | NodeHttps.Server, port: number) {
  const wss = new WebSocketServer({ server, perMessageDeflate: false });
  wss.on("connection", (socket) => {
    socket.on("message", (data: Buffer) => {
      handleFrame(new Uint8Array(data), (frame) => socket.send(frame));
    });
  });
  return new Promise<void>((resolve) => server.listen(port, "127.0.0.1", resolve));
}

async function serveWebTransport() {
  await quicheLoaded;
  const server = new Http3Server({
    port: ports.wt,
    host: "127.0.0.1",
    secret: "netbench",
    cert,
    privKey: key,
    ...quicFlowControl,
  });
  server.startServer();
  await server.ready;
  await serveRpc(makeWebTransportSocketServer(server.sessionStream("/rpc"), ports.wt));
  const sessions = server.sessionStream("/bench").getReader();
  void (async () => {
    for (;;) {
      const { value: session, done } = await sessions.read();
      if (done) return;
      void session.ready
        .then(async () => {
          const streams = session.incomingBidirectionalStreams.getReader();
          for (;;) {
            const next = await streams.read();
            if (next.done) return;
            const writer = next.value.writable.getWriter();
            const send = (frame: Uint8Array) =>
              void writer.write(lengthPrefixed(frame)).catch(() => {});
            const split = makeFrameSplitter((bytes) => handleFrame(bytes, send));
            void next.value.readable
              .pipeTo(new WritableStream({ write: (chunk: Uint8Array) => split(chunk) }))
              .catch(() => {});
          }
        })
        .catch(() => {});
    }
  })();
}

// Level 2: one RpcServer per transport, configured like the app's (AuthenticatedRpcSession.ts WS_RPC_SERVER_OPTIONS).
// WebSocket keeps the app's JSON-per-message and permessage-deflate; a WebTransport stream has no
// message boundaries, so it length-prefixes each JSON message.
function serveRpc(webTransport: SocketServer.SocketServer["Service"]) {
  const rpcServer = (serialization: Layer.Layer<RpcSerialization.RpcSerialization>) =>
    RpcServer.layer(BenchRpcs, { disableTracing: true, disableFatalDefects: true }).pipe(
      Layer.provide(layerBenchHandlers),
      // Fresh per server: layers memoize by reference, so the two would otherwise share one protocol.
      Layer.provide(Layer.fresh(RpcServer.layerProtocolSocketServer)),
      Layer.provide(serialization),
    );
  const webSocket = (port: number, perMessageDeflate: boolean) =>
    Layer.effect(
      SocketServer.SocketServer,
      NodeSocketServer.makeWebSocket({ host: "127.0.0.1", port, perMessageDeflate }),
    );
  const layer = Layer.mergeAll(
    rpcServer(RpcSerialization.layerJson).pipe(Layer.provide(webSocket(ports.rpcWs, true))),
    // Without compression, to separate what the transport does from what deflate saves.
    rpcServer(RpcSerialization.layerJson).pipe(Layer.provide(webSocket(ports.rpcWsRaw, false))),
    rpcServer(layerFramedJson).pipe(Layer.provide(layerSocketServer(webTransport))),
  );
  // Resolves once every server is listening; the scope stays open for the process lifetime.
  return Effect.runPromise(
    Effect.flatMap(Scope.make(), (scope) => Layer.buildWithScope(layer, scope)),
  );
}

await Promise.all([
  serveWebSockets(NodeHttp.createServer(), ports.ws),
  serveWebSockets(NodeHttps.createServer({ cert, key }), ports.wss),
  serveWebTransport(),
]);
process.send?.("ready");
