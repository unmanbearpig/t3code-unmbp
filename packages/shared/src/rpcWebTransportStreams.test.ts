// @effect-diagnostics nodeBuiltinImport:off - Node byte streams reproduce queued EOF ordering.
import { describe, expect, it } from "@effect/vitest";
import * as NodeStreamWeb from "node:stream/web";
import * as Effect from "effect/Effect";
import {
  createTransportByteBudget,
  makeRpcWebTransportFrameChannel,
  rpcWebTransportDefaultLimits,
  wrapStandardWebTransportSession,
  type StandardWebTransportStream,
} from "./rpcWebTransport.ts";

const openReadable = Effect.fnUntraced(function* (
  readable: StandardWebTransportStream["readable"],
) {
  const session = wrapStandardWebTransportSession({
    closed: new Promise(() => {}),
    incomingBidirectionalStreams: new NodeStreamWeb.ReadableStream<StandardWebTransportStream>(),
    async createBidirectionalStream() {
      return { readable, writable: new NodeStreamWeb.WritableStream<Uint8Array>() };
    },
    close() {},
  });
  yield* Effect.addFinalizer(() => Effect.sync(() => session.close(0)));
  const stream = yield* Effect.promise((signal) => session.openBidirectionalStream(signal));
  return { session, stream };
});

function record(payload: Uint8Array) {
  const bytes = new Uint8Array(10 + payload.byteLength);
  const header = new DataView(bytes.buffer);
  header.setUint8(0, 1);
  header.setUint8(1, 1);
  header.setUint32(2, payload.byteLength);
  header.setUint32(6, payload.byteLength);
  bytes.set(payload, 10);
  return bytes;
}

describe("standard WebTransport reader EOF", () => {
  it.effect("settles a pending FIN-only BYOB read through the backend's zero response", () =>
    Effect.gen(function* () {
      const readable = new NodeStreamWeb.ReadableStream({
        type: "bytes",
        pull(controller) {
          const request = controller.byobRequest;
          expect(request).not.toBeNull();
          controller.close();
          request?.respond(0);
        },
      });
      const { stream } = yield* openReadable(readable);
      expect(yield* Effect.promise((signal) => stream.read(60, signal))).toBeNull();
    }).pipe(Effect.scoped),
  );

  it.effect("drains and accounts a default reader's final remainder before EOF", () =>
    Effect.gen(function* () {
      const payload = new TextEncoder().encode(JSON.stringify("x".repeat(58)));
      const wire = record(payload);
      const readable = new NodeStreamWeb.ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(wire.slice());
          controller.close();
        },
      });
      const { stream } = yield* openReadable(readable);
      const input = createTransportByteBudget(1024);
      makeRpcWebTransportFrameChannel(
        stream,
        input,
        createTransportByteBudget(1024),
        rpcWebTransportDefaultLimits,
      );
      expect(yield* Effect.promise((signal) => stream.read(10, signal))).toEqual(wire.slice(0, 10));
      expect(input.usedBytes).toBe(70);
      expect(yield* Effect.promise((signal) => stream.read(60, signal))).toEqual(payload);
      expect(input.usedBytes).toBe(0);
      expect(yield* Effect.promise((signal) => stream.read(60, signal))).toBeNull();
    }).pipe(Effect.scoped),
  );

  describe.each(["bytes", "default"] as const)("%s reader", (kind) => {
    it.effect("preserves the native read failure", () =>
      Effect.gen(function* () {
        const failure = new Error("Native reader failed");
        const source = {
          start(controller: { error: (cause: unknown) => void }) {
            controller.error(failure);
          },
        };
        const readable =
          kind === "bytes"
            ? new NodeStreamWeb.ReadableStream({ ...source, type: "bytes" })
            : new NodeStreamWeb.ReadableStream<Uint8Array>(source);
        const { stream } = yield* openReadable(readable);
        expect(
          yield* Effect.promise((signal) =>
            stream.read(60, signal).catch((cause: unknown) => cause),
          ),
        ).toBe(failure);
      }).pipe(Effect.scoped),
    );

    it.effect("rejects a read whose backing allocation cannot fit a zero byte budget", () =>
      Effect.gen(function* () {
        const source = {
          start(controller: { enqueue: (bytes: Uint8Array) => void; close: () => void }) {
            controller.enqueue(new Uint8Array(70));
            controller.close();
          },
        };
        const readable =
          kind === "bytes"
            ? new NodeStreamWeb.ReadableStream({ ...source, type: "bytes" })
            : new NodeStreamWeb.ReadableStream<Uint8Array>(source);
        const { stream } = yield* openReadable(readable);
        const input = createTransportByteBudget(0);
        makeRpcWebTransportFrameChannel(
          stream,
          input,
          createTransportByteBudget(1024),
          rpcWebTransportDefaultLimits,
        );
        expect(
          yield* Effect.promise((signal) =>
            stream.read(10, signal).catch((cause: unknown) => cause),
          ),
        ).toMatchObject({ category: "capacity" });
        expect(input.usedBytes).toBe(0);
      }).pipe(Effect.scoped),
    );

    it.effect.each(["abort", "dispose"] as const)(
      "cancels a pending read on %s without native acknowledgement",
      (action) =>
        Effect.gen(function* () {
          const started = Promise.withResolvers<void>();
          const source = {
            pull() {
              started.resolve();
            },
            cancel() {
              return new Promise<void>(() => {});
            },
          };
          const readable =
            kind === "bytes"
              ? new NodeStreamWeb.ReadableStream({ ...source, type: "bytes" })
              : new NodeStreamWeb.ReadableStream<Uint8Array>(source);
          const { stream, session } = yield* openReadable(readable);
          const input = createTransportByteBudget(1024);
          makeRpcWebTransportFrameChannel(
            stream,
            input,
            createTransportByteBudget(1024),
            rpcWebTransportDefaultLimits,
          );
          // @effect-diagnostics-next-line abortControllerInEffect:off - Exercises caller-owned cancellation.
          const abort = new AbortController();
          const pending = stream.read(60, abort.signal).catch((cause: unknown) => cause);
          yield* Effect.promise(() => started.promise);
          if (action === "abort") abort.abort();
          else session.close(0);
          expect(yield* Effect.promise(() => pending)).toMatchObject({ category: "aborted" });
          expect(input.usedBytes).toBe(0);
        }).pipe(Effect.scoped),
    );
  });

  it.effect("drains queued final 70 bytes as header 10 and body 60 before EOF", () =>
    Effect.gen(function* () {
      let controller!: NodeStreamWeb.ReadableByteStreamController;
      const readable = new NodeStreamWeb.ReadableStream({
        type: "bytes",
        start(value) {
          controller = value;
        },
      });
      const { stream } = yield* openReadable(readable);
      // First fulfill a positive BYOB read, as in the production boundary capture.
      const prefix = new Uint8Array([1, 2, 3, 4, 5]);
      // @effect-diagnostics-next-line abortControllerInEffect:off - Exercises caller-owned cancellation.
      const first = stream.read(5, new AbortController().signal);
      controller.enqueue(prefix.slice());
      expect(yield* Effect.promise(() => first)).toEqual(prefix);
      const payload = new TextEncoder().encode(JSON.stringify("x".repeat(58)));
      const wire = record(payload);
      controller.enqueue(wire.slice());
      controller.close();
      expect(yield* Effect.promise((signal) => stream.read(10, signal))).toEqual(wire.slice(0, 10));
      expect(yield* Effect.promise((signal) => stream.read(60, signal))).toEqual(payload);
      expect(yield* Effect.promise((signal) => stream.read(60, signal))).toBeNull();
    }).pipe(Effect.scoped),
  );

  it.effect("decodes a queued final record without truncating its last body", () =>
    Effect.gen(function* () {
      const value = "x".repeat(58);
      const readable = new NodeStreamWeb.ReadableStream({
        type: "bytes",
        start(controller) {
          controller.enqueue(record(new TextEncoder().encode(JSON.stringify(value))));
          controller.close();
        },
      });
      const { stream } = yield* openReadable(readable);
      const input = createTransportByteBudget(1024);
      const output = createTransportByteBudget(1024);
      const channel = makeRpcWebTransportFrameChannel(
        stream,
        input,
        output,
        rpcWebTransportDefaultLimits,
      );
      expect(yield* channel.read()).toBe(value);
      expect(yield* channel.read()).toBeNull();
      expect(input.usedBytes).toBe(0);
      expect(output.usedBytes).toBe(0);
    }).pipe(Effect.scoped),
  );
});
