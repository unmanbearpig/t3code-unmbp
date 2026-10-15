import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import { deflateSync } from "fflate";
import {
  createTransportByteBudget,
  makeRpcWebTransportFrameChannel,
  rpcWebTransportDefaultLimits,
  RPC_WEBTRANSPORT_LIMITS as limits,
  RPC_WEBTRANSPORT_PREFACE,
  wrapStandardWebTransportSession,
  type ReliableByteStream,
} from "./rpcWebTransport.ts";
import { memorySessionPair, memoryStreamPair } from "./rpcWebTransport.test-support.ts";

function bytesStream(chunks: Uint8Array[]): ReliableByteStream {
  let offset = 0;
  return {
    async read(max) {
      const current = chunks[0];
      if (!current) return null;
      const part = current.slice(offset, offset + max);
      offset += part.byteLength;
      if (offset === current.byteLength) {
        chunks.shift();
        offset = 0;
      }
      return part;
    },
    async write(bytes) {
      chunks.push(bytes.slice());
    },
    async finish() {},
    reset() {},
    stop() {},
  };
}

function record(payload: Uint8Array, decoded = payload.byteLength, flags = 1) {
  const header = new Uint8Array(10);
  const view = new DataView(header.buffer);
  view.setUint8(0, 1);
  view.setUint8(1, flags);
  view.setUint32(2, payload.byteLength);
  view.setUint32(6, decoded);
  return [header, payload];
}

function channel(
  stream: ReliableByteStream,
  budget = createTransportByteBudget(limits.sessionInputBytes),
) {
  return makeRpcWebTransportFrameChannel(
    stream,
    budget,
    createTransportByteBudget(limits.sessionOutputBytes),
    rpcWebTransportDefaultLimits,
  );
}

describe("WebTransport records", () => {
  it.effect("round trips fragmented Unicode and an encoded message above 16 MiB", () =>
    Effect.gen(function* () {
      const chunks: Uint8Array[] = [];
      const stream = bytesStream(chunks);
      const frames = channel(stream);
      const text = "€😀a".repeat(2_400_000);
      yield* frames.write({ text });
      expect(chunks.length).toBeGreaterThan(500);
      expect(yield* frames.read()).toEqual({ text });
    }),
  );

  it.effect(
    "accepts exactly 100 MiB and rejects the next decoded record before reading its body",
    () =>
      Effect.gen(function* () {
        const chunks: Uint8Array[] = [];
        const frames = channel(bytesStream(chunks));
        const text = "a".repeat(limits.messageBytes - 2);
        yield* frames.write(text);
        expect(chunks.length).toBe(3200);
        expect(yield* frames.read()).toBe(text);
        const over = record(new Uint8Array(1), 1, 1)[0]!;
        const nonfinal = record(new Uint8Array(limits.recordBytes), limits.recordBytes, 0);
        const headerOnly = Array.from({ length: 1600 }, () => nonfinal).flat();
        const fail = yield* channel(bytesStream([...headerOnly, over]))
          .read()
          .pipe(Effect.exit);
        expect(Exit.isFailure(fail)).toBe(true);
      }),
  );

  it.effect.each([
    { name: "empty", bytes: record(new Uint8Array(0)) },
    { name: "unknown flags", bytes: record(new Uint8Array([49]), 1, 5) },
    { name: "short non-final", bytes: record(new Uint8Array([49]), 1, 0) },
    { name: "bad identity length", bytes: record(new Uint8Array([49]), 2, 1) },
    { name: "truncated", bytes: record(new Uint8Array([49]), 2, 1).slice(0, 1) },
    { name: "invalid UTF8", bytes: record(new Uint8Array([0xff])) },
    { name: "invalid JSON", bytes: record(new Uint8Array([0x7b])) },
    { name: "invalid deflate", bytes: record(new Uint8Array([0xff, 0xff]), 65536, 3) },
    { name: "oversized compressed record", bytes: record(new Uint8Array(8193), 65536, 3) },
    {
      name: "short deflate output",
      bytes: record(deflateSync(new TextEncoder().encode('"abc"')), 100, 3),
    },
  ])("rejects $name records and releases accounting", ({ bytes }) =>
    Effect.gen(function* () {
      const budget = createTransportByteBudget(limits.sessionInputBytes);
      const result = yield* channel(bytesStream(bytes), budget).read().pipe(Effect.exit);
      expect(Exit.isFailure(result)).toBe(true);
      expect(budget.usedBytes).toBe(0);
    }),
  );

  it.effect("rejects bounded expansion bombs and documents allowed compressed trailing bytes", () =>
    Effect.gen(function* () {
      const compressedBomb = deflateSync(new Uint8Array(7_900_000), { level: 9 });
      const bomb = new Uint8Array(limits.compressedRecordBytes);
      bomb.set(compressedBomb);
      expect(bomb.byteLength).toBeLessThanOrEqual(limits.compressedRecordBytes);
      const result = yield* channel(bytesStream(record(bomb, limits.recordBytes, 3)))
        .read()
        .pipe(Effect.exit);
      expect(Exit.isFailure(result)).toBe(true);
      const text = '"' + "a".repeat(1000) + '"';
      const compressed = deflateSync(new TextEncoder().encode(text));
      const trailing = new Uint8Array(compressed.byteLength + 2);
      trailing.set(compressed);
      trailing.set([0xff, 0xff], compressed.byteLength);
      expect(yield* channel(bytesStream(record(trailing, text.length, 3))).read()).toBe(
        "a".repeat(1000),
      );
    }),
  );

  it.effect("charges nested shared budgets and releases each lease once", () =>
    Effect.sync(() => {
      const listener = createTransportByteBudget(100);
      const first = listener.child(80);
      const second = listener.child(80);
      const lease = first.acquire(70);
      expect(() => second.acquire(40)).toThrow("budget exhausted");
      expect(second.usedBytes).toBe(0);
      expect(listener.usedBytes).toBe(70);
      lease.release();
      lease.release();
      expect(listener.usedBytes).toBe(0);
      const next = second.acquire(80);
      next.release();
    }),
  );

  it.effect("enforces one aggregate input ceiling across concurrent session decoders", () =>
    Effect.gen(function* () {
      const aggregate = createTransportByteBudget(200_000);
      const first = aggregate.child(150_000);
      const second = aggregate.child(150_000);
      let signal!: () => void;
      const entered = new Promise<void>((resolve) => {
        signal = resolve;
      });
      const h = record(new Uint8Array(limits.recordBytes), limits.recordBytes, 0)[0]!;
      const stalled = bytesStream([h]);
      const read = stalled.read;
      let header = true;
      stalled.read = (max, abort) => {
        if (header) {
          header = false;
          return read(max, abort);
        }
        signal();
        return new Promise(() => {});
      };
      const pending = yield* channel(stalled, first).read().pipe(Effect.forkChild);
      yield* Effect.promise(() => entered);
      expect(aggregate.usedBytes).toBe(limits.recordBytes * 2);
      const denied = yield* channel(bytesStream([h]), second)
        .read()
        .pipe(Effect.exit);
      expect(Exit.isFailure(denied)).toBe(true);
      expect(second.usedBytes).toBe(0);
      yield* Fiber.interrupt(pending);
      expect(aggregate.usedBytes).toBe(0);
    }),
  );

  it.effect(
    "fails assembly-copy exhaustion instead of waiting and charges large backing buffers",
    () =>
      Effect.gen(function* () {
        const budget = createTransportByteBudget(150);
        const payload = new TextEncoder().encode('"' + "a".repeat(98) + '"');
        const result = yield* channel(
          bytesStream(record(deflateSync(payload), payload.byteLength, 3)),
          budget,
        )
          .read()
          .pipe(Effect.exit);
        expect(Exit.isFailure(result)).toBe(true);
        expect(budget.usedBytes).toBe(0);
        const backing = new Uint8Array(1024);
        const stream = bytesStream([]);
        stream.read = async () => backing.subarray(0, 10);
        const tiny = createTransportByteBudget(100);
        expect(Exit.isFailure(yield* channel(stream, tiny).read().pipe(Effect.exit))).toBe(true);
        expect(tiny.usedBytes).toBe(0);
      }),
  );

  it.effect(
    "releases serialized bytes and compressor storage when a blocked write is cancelled",
    () =>
      Effect.gen(function* () {
        const output = createTransportByteBudget(limits.sessionOutputBytes);
        let signal!: () => void;
        const entered = new Promise<void>((resolve) => {
          signal = resolve;
        });
        const stream = bytesStream([]);
        stream.write = () =>
          new Promise(() => {
            signal();
          });
        const c = makeRpcWebTransportFrameChannel(
          stream,
          createTransportByteBudget(100),
          output,
          rpcWebTransportDefaultLimits,
        );
        const sending = yield* c.write("a".repeat(100_000)).pipe(Effect.forkChild);
        yield* Effect.promise(() => entered);
        expect(output.usedBytes).toBeGreaterThan(100_000);
        yield* Fiber.interrupt(sending);
        expect(output.usedBytes).toBe(0);
        expect(Exit.isFailure(yield* c.write("a".repeat(8192), true).pipe(Effect.exit))).toBe(true);
        expect(output.usedBytes).toBe(0);
      }),
  );

  it.effect(
    "uses two seconds for preface and header, then refreshes a 15 second assembly deadline on byte progress",
    () =>
      Effect.gen(function* () {
        const pulls = Array.from({ length: 4 }, () => {
          let signal!: () => void;
          const entered = new Promise<void>((resolve) => {
            signal = resolve;
          });
          let provide!: (bytes: Uint8Array) => void;
          const bytes = new Promise<Uint8Array>((resolve) => {
            provide = resolve;
          });
          return { entered, signal, bytes, provide };
        });
        let index = 0;
        const scripted = bytesStream([]);
        scripted.read = () => {
          const pull = pulls[index++]!;
          pull.signal();
          return pull.bytes;
        };
        const c = channel(scripted);
        const reader = yield* Effect.gen(function* () {
          yield* c.readPreface();
          return yield* c.read({ first: true });
        }).pipe(Effect.forkChild);
        yield* Effect.promise(() => pulls[0]!.entered);
        pulls[0]!.provide(new Uint8Array([...RPC_WEBTRANSPORT_PREFACE, 1]));
        yield* Effect.promise(() => pulls[1]!.entered);
        pulls[1]!.provide(record(new Uint8Array([34, 97, 34]))[0]!);
        yield* Effect.promise(() => pulls[2]!.entered);
        yield* TestClock.adjust(3000);
        pulls[2]!.provide(new Uint8Array([34]));
        yield* Effect.promise(() => pulls[3]!.entered);
        yield* TestClock.adjust(3000);
        pulls[3]!.provide(new Uint8Array([97, 34]));
        expect(yield* Fiber.join(reader)).toBe("a");
      }),
  );

  it.effect.each(["preface", "header", "body", "next-record"] as const)(
    "times out a missing %s and releases its bytes",
    (part) =>
      Effect.gen(function* () {
        const initial =
          part === "preface"
            ? []
            : [
                new Uint8Array([...RPC_WEBTRANSPORT_PREFACE, 1]),
                ...(part === "header"
                  ? []
                  : record(
                      new Uint8Array(limits.recordBytes),
                      limits.recordBytes,
                      part === "next-record" ? 0 : 1,
                    ).slice(0, part === "body" ? 1 : 2)),
              ];
        const source = bytesStream(initial);
        const read = source.read;
        let blocked!: () => void;
        const entered = new Promise<void>((resolve) => {
          blocked = resolve;
        });
        source.read = (max, signal) =>
          initial.length > 0
            ? read(max, signal)
            : new Promise(() => {
                blocked();
              });
        const budget = createTransportByteBudget(limits.sessionInputBytes);
        const c = channel(source, budget);
        const task = yield* Effect.gen(function* () {
          yield* c.readPreface();
          return yield* c.read({ first: true });
        }).pipe(Effect.exit, Effect.forkChild);
        yield* Effect.promise(() => entered);
        yield* TestClock.adjust(part === "preface" || part === "header" ? 2000 : 15000);
        expect(Exit.isFailure(yield* Fiber.join(task))).toBe(true);
        expect(budget.usedBytes).toBe(0);
      }),
  );

  it.effect("starts the idle deadline when a later message header begins", () =>
    Effect.gen(function* () {
      const source = bytesStream([new Uint8Array([1])]);
      const read = source.read;
      let first = true;
      let signal!: () => void;
      const entered = new Promise<void>((resolve) => {
        signal = resolve;
      });
      source.read = (max, abort) => {
        if (first) {
          first = false;
          return read(max, abort);
        }
        signal();
        return new Promise(() => {});
      };
      const budget = createTransportByteBudget(1000);
      const pending = yield* channel(source, budget).read().pipe(Effect.exit, Effect.forkChild);
      yield* Effect.promise(() => entered);
      yield* TestClock.adjust(15000);
      expect(Exit.isFailure(yield* Fiber.join(pending))).toBe(true);
      expect(budget.usedBytes).toBe(0);
    }),
  );

  it.effect("cancels an unacknowledged native open and disposes a late completion", () =>
    Effect.gen(function* () {
      let complete!: (stream: ReturnType<typeof memoryStreamPair>[0]) => void;
      const late = new Promise<ReturnType<typeof memoryStreamPair>[0]>((resolve) => {
        complete = resolve;
      });
      const [stream] = memoryStreamPair();
      let closes = 0;
      const raw = memorySessionPair().rawClient;
      const session = wrapStandardWebTransportSession({
        ...raw,
        closed: new Promise(() => {}),
        createBidirectionalStream: () => late,
        close() {
          closes++;
        },
      });
      const abort = new AbortController();
      const open = session.openBidirectionalStream(abort.signal).catch((cause: unknown) => cause);
      abort.abort();
      expect(yield* Effect.promise(() => open)).toMatchObject({ category: "aborted" });
      expect(yield* Effect.promise(() => session.closed)).toMatchObject({ category: "aborted" });
      expect(closes).toBe(1);
      complete(stream);
      yield* Effect.promise(() => late);
      // Late ownership is disposed by the promise continuation before this milestone.
      expect(yield* Effect.promise(() => stream.readable.getReader().read())).toMatchObject({
        done: true,
      });
    }),
  );

  it.effect("keeps a standard reader's retained backing allocation charged between messages", () =>
    Effect.gen(function* () {
      const [local, remote] = memoryStreamPair();
      const raw = memorySessionPair().rawClient;
      const session = wrapStandardWebTransportSession({
        ...raw,
        createBidirectionalStream: async () => local,
        closed: new Promise(() => {}),
      });
      const stream = yield* Effect.promise((signal) => session.openBidirectionalStream(signal));
      const budget = createTransportByteBudget(1000);
      const c = channel(stream, budget);
      const messages = [
        record(new TextEncoder().encode('"one"')),
        record(new TextEncoder().encode('"two"')),
      ].flat();
      const combined = new Uint8Array(messages.reduce((size, bytes) => size + bytes.byteLength, 0));
      let offset = 0;
      for (const bytes of messages) {
        combined.set(bytes, offset);
        offset += bytes.byteLength;
      }
      const writer = remote.writable.getWriter();
      const sent = yield* Effect.promise(() => writer.write(combined)).pipe(Effect.forkChild);
      expect(yield* c.read()).toBe("one");
      yield* Fiber.join(sent);
      expect(budget.usedBytes).toBe(combined.byteLength);
      expect(yield* c.read()).toBe("two");
      expect(budget.usedBytes).toBe(0);
      session.close(0);
    }),
  );
});
