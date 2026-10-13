// Level 2 clients: the same RpcClient over four transport configurations.
// - rpc-ws: Socket.layerWebSocket + JSON, as packages/client-runtime/src/rpc/session.ts connects,
//   with the server's permessage-deflate. rpc-ws-raw is the same without compression.
// - rpc-wt: one WebTransport stream carrying every RPC as length-prefixed JSON; a drop-in Socket.
// - rpc-wt-multi: a WebTransport stream per request, so one slow response cannot block another.
import {
  WebTransport,
  type WebTransportBidirectionalStream,
  quicheLoaded,
} from "@fails-components/webtransport";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import * as Scope from "effect/Scope";
import { RpcClient, RpcClientError, RpcSerialization } from "effect/rpc";
import type { FromClientEncoded, FromServerEncoded } from "effect/rpc/RpcMessage";
import * as Socket from "effect/socket/Socket";

import type { TlsMaterial } from "./client.ts";
import { ports, quicFlowControl } from "./protocol.ts";
import { BenchRpcs, layerFramedJson, socketFromStream } from "./rpc.ts";

export const rpcTransportNames = ["rpc-ws", "rpc-ws-raw", "rpc-wt", "rpc-wt-multi"] as const;
export type RpcTransportName = (typeof rpcTransportNames)[number];

const openWebTransport = Effect.fnUntraced(function* (tls: TlsMaterial) {
  yield* Effect.promise(() => quicheLoaded);
  const transport = yield* Effect.acquireRelease(
    Effect.sync(
      () =>
        new WebTransport(`https://127.0.0.1:${ports.wt}/rpc`, {
          serverCertificateHashes: [{ algorithm: "sha-256", value: tls.certSha256 }],
          ...quicFlowControl,
        }),
    ),
    (transport) => Effect.sync(() => transport.close()),
  );
  // Wait outside acquireRelease so a deadline can interrupt a stalled handshake and close it.
  yield* Effect.promise(() => transport.ready);
  return transport;
});

// Matches the session's settings: no protocol-level retries, failures surface to the caller.
const protocolSocket = RpcClient.makeProtocolSocket({
  // Matches packages/client-runtime/src/rpc/protocol.ts, including long responses under loss.
  pingTimeout: "15 seconds",
  retryTransientErrors: false,
  retryPolicy: Schedule.recurs(0),
});

/**
 * Opens one WebTransport stream per RPC request. Acks and interrupts follow their request onto
 * its stream; the stream closes once the request's Exit arrives. Pings are dropped: each stream is
 * short-lived and QUIC's own idle timeout detects a dead peer.
 */
const makeProtocolStreamPerRequest = (transport: WebTransport) =>
  RpcClient.Protocol.make(
    Effect.fnUntraced(function* (writeResponse) {
      const serialization = yield* RpcSerialization.RpcSerialization;
      const scope = yield* Effect.scope;
      type Parser = ReturnType<typeof serialization.makeUnsafe>;
      const requests = new Map<
        string | number,
        {
          readonly parser: Parser;
          readonly writer: Promise<WritableStreamDefaultWriter<Uint8Array>>;
        }
      >();
      const encoder = new TextEncoder();

      const readResponses = (
        clientId: number,
        requestId: string | number,
        stream: WebTransportBidirectionalStream,
        parser: Parser,
        writer: Promise<WritableStreamDefaultWriter<Uint8Array>>,
      ) =>
        Effect.gen(function* () {
          const reader = (stream.readable as ReadableStream<Uint8Array>).getReader();
          while (true) {
            const next = yield* Effect.promise(() => reader.read());
            if (next.done) return;
            if (!requests.has(requestId)) continue;
            for (const response of parser.decode(next.value) as Array<FromServerEncoded>) {
              yield* writeResponse(clientId, response);
              if (response._tag === "Exit") {
                requests.delete(requestId);
                // Finish our side, then keep reading until the server's FIN: QUIC only frees the
                // stream (and its slot in the concurrent stream limit) once both ends are consumed.
                // Through the writer: the stream itself is locked to it, so stream.writable.close() rejects.
                yield* Effect.promise(() => writer.then((w) => w.close()).catch(() => {}));
                break;
              }
            }
          }
        });

      const send = (clientId: number, message: FromClientEncoded) =>
        Effect.gen(function* () {
          if (message._tag === "Ping" || message._tag === "Eof") return;
          const requestId = message._tag === "Request" ? message.id : message.requestId;
          if (message._tag === "Request") {
            const parser = serialization.makeUnsafe();
            // Queues behind the peer's stream limit instead of failing when it is reached.
            const opened = transport.createBidirectionalStream({
              sendGroup: null,
              waitUntilAvailable: true,
            });
            const writer = opened.then((stream) => stream.writable.getWriter());
            writer.catch(() => {});
            // Registered before awaiting, so an interrupt sent meanwhile finds this stream.
            requests.set(requestId, { parser, writer });
            const stream = yield* Effect.tryPromise(() => opened);
            yield* Effect.forkIn(readResponses(clientId, requestId, stream, parser, writer), scope);
          }
          const request = requests.get(requestId);
          if (!request) return;
          const encoded = request.parser.encode(message);
          if (encoded === undefined) return;
          const streamWriter = yield* Effect.tryPromise(() => request.writer);
          yield* Effect.tryPromise(() =>
            streamWriter.write(typeof encoded === "string" ? encoder.encode(encoded) : encoded),
          );
        }).pipe(
          Effect.mapError(
            (cause) =>
              new RpcClientError.RpcClientError({
                reason: new RpcClientError.RpcClientDefect({
                  message: "stream write failed",
                  cause,
                }),
              }),
          ),
        );

      return {
        send,
        supportsAck: true,
        supportsTransferables: false,
        codecFor: serialization.codecFor,
      };
    }),
  );

/** Builds a BenchRpcs client for `transport` in the current scope. */
export const makeRpcClient = (transport: RpcTransportName, tls: TlsMaterial) =>
  Effect.gen(function* () {
    const scope = yield* Effect.scope;
    let protocol: Layer.Layer<RpcClient.Protocol, never, Scope.Scope>;
    switch (transport) {
      case "rpc-ws":
      case "rpc-ws-raw":
        protocol = Layer.effect(RpcClient.Protocol, protocolSocket).pipe(
          Layer.provide(
            Socket.layerWebSocket(
              `ws://127.0.0.1:${transport === "rpc-ws" ? ports.rpcWs : ports.rpcWsRaw}/`,
            ),
          ),
          Layer.provide(Socket.layerWebSocketConstructorGlobal),
          Layer.provide(RpcSerialization.layerJson),
        );
        break;
      case "rpc-wt": {
        const session = yield* openWebTransport(tls);
        const socket = yield* socketFromStream(() => session.createBidirectionalStream());
        protocol = Layer.effect(RpcClient.Protocol, protocolSocket).pipe(
          Layer.provide(Layer.succeed(Socket.Socket, socket)),
          Layer.provide(layerFramedJson),
        );
        break;
      }
      case "rpc-wt-multi": {
        const session = yield* openWebTransport(tls);
        protocol = Layer.effect(RpcClient.Protocol, makeProtocolStreamPerRequest(session)).pipe(
          Layer.provide(layerFramedJson),
        );
        break;
      }
    }
    const context = yield* Layer.buildWithScope(protocol, scope);
    return yield* RpcClient.make(BenchRpcs).pipe(Effect.provide(context));
  });
