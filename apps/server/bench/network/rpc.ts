// @effect-diagnostics effectFnOpportunity:off - Protocol benchmarks must not add tracing spans to measured operations.
// Level 2: the app's Effect RPC stack (RpcServer/RpcClient over a Socket) with T3-shaped payloads.
// Shared by the bench server and client.
import type {
  WebTransportBidirectionalStream,
  WebTransportSession,
} from "@fails-components/webtransport";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as NetAddress from "effect/net/NetAddress";
import { Rpc, RpcGroup, RpcSerialization } from "effect/rpc";
import * as Schema from "effect/Schema";
import * as Socket from "effect/socket/Socket";
import * as SocketServer from "effect/socket/SocketServer";
import * as Stream from "effect/Stream";

import { lengthPrefixed, makeFrameSplitter, wallClockMs } from "./protocol.ts";
import { rpcTextSettings } from "./settings.ts";

/** Shaped like an orchestration event carrying a streamed assistant message delta. */
export const BenchEvent = Schema.Struct({
  sequence: Schema.Number,
  sentAt: Schema.Number,
  type: Schema.String,
  threadId: Schema.String,
  messageId: Schema.String,
  text: Schema.String,
});

/** Shaped like a thread snapshot: the response a client waits on when opening a long thread. */
export const BenchSnapshot = Schema.Struct({
  sentAt: Schema.Number,
  messages: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      role: Schema.String,
      text: Schema.String,
      createdAt: Schema.String,
    }),
  ),
});

export class BenchRpcs extends RpcGroup.make(
  Rpc.make("ping", { payload: { text: Schema.String }, success: Schema.String }),
  Rpc.make("subscribe", {
    payload: { count: Schema.Number, intervalMs: Schema.Number, bytes: Schema.Number },
    success: BenchEvent,
    stream: true,
  }),
  Rpc.make("snapshot", {
    payload: { messages: Schema.Number, bytes: Schema.Number },
    success: BenchSnapshot,
  }),
) {}

// Synthetic text with a small vocabulary, repeated across messages. Its compression ratio is
// workload-specific; rpc-ws-raw separates transport performance from the savings of deflate.
const words = rpcTextSettings.vocabulary.split(" ");
const text = (bytes: number) => {
  let seed = bytes;
  let out = "";
  while (out.length < bytes) {
    seed =
      (seed * rpcTextSettings.multiplier + rpcTextSettings.increment) % rpcTextSettings.modulus;
    out += `${words[seed % words.length]} `;
  }
  return out.slice(0, bytes);
};

export const layerBenchHandlers = BenchRpcs.toLayer({
  ping: ({ text }) => Effect.succeed(text),
  subscribe: ({ count, intervalMs, bytes }) => {
    const body = text(bytes);
    return Stream.tick(`${intervalMs} millis`).pipe(
      Stream.take(count),
      Stream.map((_, sequence) => ({
        sequence,
        sentAt: wallClockMs(),
        type: "thread.message-delta",
        threadId: "thread-bench",
        messageId: "message-bench",
        text: body,
      })),
      // Events are stamped when produced, like domain events, then wait for RPC acks in this
      // buffer; without it the ack backpressure would delay the stamping and hide the wait.
      Stream.buffer({ capacity: "unbounded" }),
    );
  },
  snapshot: ({ messages, bytes }) =>
    Effect.sync(() => {
      const body = text(bytes);
      return {
        sentAt: wallClockMs(),
        messages: Array.from({ length: messages }, (_, index) => ({
          id: `message-${index}`,
          role: index % 2 === 0 ? "user" : "assistant",
          text: body,
          createdAt: rpcTextSettings.createdAt,
        })),
      };
    }),
});

/**
 * JSON with a u32 length prefix per message: the WebSocket path's one-JSON-per-message model on a
 * byte stream. Effect's NDJSON parser rescans its whole buffer for a newline on every chunk, which
 * turns one 4MB message arriving in ~3000 QUIC chunks into seconds of CPU.
 */
export const layerFramedJson = Layer.succeed(RpcSerialization.RpcSerialization)(
  RpcSerialization.RpcSerialization.of({
    contentType: "application/x-length-prefixed-json",
    includesFraming: true,
    codecFor: RpcSerialization.json.codecFor,
    makeUnsafe: () => {
      const encoder = new TextEncoder();
      const decoder = new TextDecoder();
      let decoded: Array<unknown> = [];
      const split = makeFrameSplitter((frame) => {
        const value: unknown = JSON.parse(decoder.decode(frame));
        if (Array.isArray(value)) decoded.push(...value);
        else decoded.push(value);
      });
      return {
        decode: (bytes) => {
          split(typeof bytes === "string" ? encoder.encode(bytes) : bytes);
          const items = decoded;
          decoded = [];
          return items;
        },
        encode: (message) => lengthPrefixed(encoder.encode(JSON.stringify(message))),
      };
    },
  }),
);

/** Wraps a WebTransport bidirectional stream as an Effect Socket. */
export const socketFromStream = (open: () => Promise<Socket.InputTransformStream>) =>
  Socket.fromTransformStream(
    Effect.tryPromise({
      try: open,
      catch: (cause) =>
        new Socket.SocketError({ reason: new Socket.SocketOpenError({ kind: "Unknown", cause }) }),
    }),
  );

/**
 * A SocketServer whose sockets are the bidirectional streams WebTransport clients open, so
 * RpcServer's socket protocol serves them unchanged. One stream is one RPC "client".
 */
export const makeWebTransportSocketServer = (
  sessions: ReadableStream<WebTransportSession>,
  port: number,
): SocketServer.SocketServer["Service"] => ({
  address: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", port),
  run: Effect.fnUntraced(function* (handler) {
    const forEachValue = Effect.fnUntraced(function* <A, R>(
      stream: ReadableStream<A>,
      f: (value: A) => Effect.Effect<void, never, R>,
    ) {
      const reader = stream.getReader();
      while (true) {
        const next = yield* Effect.promise(() => reader.read());
        if (next.done) return;
        yield* Effect.forkDetach(f(next.value));
      }
    });
    yield* forEachValue(sessions, (session) =>
      Effect.promise(() => session.ready).pipe(
        Effect.andThen(
          forEachValue(
            session.incomingBidirectionalStreams as ReadableStream<WebTransportBidirectionalStream>,
            (stream) => {
              // The Socket takes a writer it never closes, so it gets a forwarding writable and
              // this keeps the real writer. Closing it after the handler sends our FIN; without
              // that a finished stream keeps counting against the client's stream limit.
              const writer = stream.writable.getWriter();
              const writable = new WritableStream<Uint8Array>({
                write: (chunk) => writer.write(chunk),
              });
              return Effect.flatMap(
                socketFromStream(() => Promise.resolve({ readable: stream.readable, writable })),
                (socket) =>
                  Effect.scoped(handler(socket)).pipe(
                    Effect.ignoreCause,
                    Effect.ensuring(Effect.promise(() => writer.close().catch(() => {}))),
                  ),
              );
            },
          ),
        ),
        Effect.ignoreCause,
      ),
    );
    return yield* Effect.never;
  }),
});

export const layerSocketServer = (server: SocketServer.SocketServer["Service"]) =>
  Layer.succeed(SocketServer.SocketServer, server);
