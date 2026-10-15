import { WS_METHODS } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import { Rpc, RpcClient, RpcGroup, RpcServer } from "effect/rpc";
import * as Schema from "effect/Schema";
import * as SocketServer from "effect/socket/SocketServer";
import * as Stream from "effect/Stream";

import { withTerminalOutputWindow } from "../../../src/terminal/OutputProtocol.ts";
import { makeFramedJson } from "./framedJson.ts";
import { openQuicFixture } from "./quicFixture.ts";
import { makeRequestStreamProtocol } from "./requestStreams.ts";
import { layerFramedJsonV1 } from "./serialization.ts";

const TestRpcs = RpcGroup.make(
  Rpc.make("echo", { payload: { text: Schema.String }, success: Schema.String }),
  Rpc.make("identity", { success: Schema.Number }),
  Rpc.make("block", { payload: { label: Schema.String }, success: Schema.Void }),
  Rpc.make("chunks", { payload: { count: Schema.Number }, success: Schema.Number, stream: true }),
  Rpc.make(WS_METHODS.terminalObserve, { success: Schema.String, stream: true }),
);

const openHarness = Effect.gen(function* () {
  const fixture = yield* Effect.acquireRelease(
    Effect.promise(() => openQuicFixture()),
    (value) => Effect.promise(() => value.close()),
  );
  const started = yield* Queue.unbounded<string>();
  const stopped = yield* Queue.unbounded<string>();
  const output = yield* Queue.unbounded<string>();
  const handlers = TestRpcs.toLayer({
    echo: ({ text }) => Effect.succeed(text),
    identity: (_input, metadata) => Effect.succeed(metadata.client.id),
    block: ({ label }) =>
      Queue.offer(started, label).pipe(
        Effect.andThen(Effect.never),
        Effect.ensuring(Queue.offer(stopped, label)),
      ),
    chunks: ({ count }) => Stream.fromIterable(Array.from({ length: count }, (_, index) => index)),
    [WS_METHODS.terminalObserve]: () => Stream.fromQueue(output),
  });
  const protocol = yield* RpcServer.makeProtocolSocketServer.pipe(
    Effect.provideService(SocketServer.SocketServer, fixture.socketServer),
    Effect.provide(layerFramedJsonV1),
  );
  yield* RpcServer.make(TestRpcs, { disableTracing: true, disableFatalDefects: true }).pipe(
    Effect.provide(handlers),
    Effect.provideService(RpcServer.Protocol, withTerminalOutputWindow(protocol)),
    Effect.forkScoped,
  );
  const session = yield* Effect.promise(() => fixture.connect());
  const clientProtocol = yield* makeRequestStreamProtocol(session);
  const client = yield* RpcClient.make(TestRpcs).pipe(
    Effect.provideService(RpcClient.Protocol, clientProtocol),
  );
  yield* Effect.addFinalizer(() => Effect.promise(() => fixture.close()));
  return { ...fixture, session, client, started, stopped, output, protocol };
});

it.live(
  "runs complete concurrent RPCs on independent real QUIC streams and interrupts blocked work",
  () =>
    Effect.gen(function* () {
      const { client, session, started, stopped } = yield* openHarness;
      expect(session.reliability).toBe("supports-unreliable");
      const blocked = yield* client.block({ label: "long" }).pipe(Effect.forkScoped);
      expect(yield* Queue.take(started)).toBe("long");
      const texts = Array.from({ length: 12 }, (_, index) => `${index}:acción:🐈:`.repeat(1000));
      expect(
        yield* Effect.forEach(texts, (text) => client.echo({ text }), { concurrency: "unbounded" }),
      ).toEqual(texts);
      expect(yield* Stream.runCollect(client.chunks({ count: 2000 }))).toEqual(
        Array.from({ length: 2000 }, (_, index) => index),
      );
      yield* Fiber.interrupt(blocked);
      expect(yield* Queue.take(stopped)).toBe("long");
      expect(yield* client.echo({ text: "still connected" })).toBe("still connected");
    }).pipe(Effect.scoped),
);

it.live.each(["fin", "reset", "session"] as const)(
  "fails every pending and future call when the peer closes with %s",
  (mode) =>
    Effect.gen(function* () {
      const harness = yield* openHarness;
      const calls = yield* Effect.forEach(["a", "b", "c"], (label) =>
        harness.client.block({ label }).pipe(Effect.exit, Effect.forkScoped),
      );
      for (let index = 0; index < calls.length; index++) yield* Queue.take(harness.started);
      if (mode === "fin") yield* Effect.promise(() => harness.streams[0]!.close());
      else if (mode === "reset") harness.streams[0]!.reset();
      else harness.serverSessions[0]!.close();
      const exits = yield* Effect.forEach(calls, Fiber.join);
      expect(exits.every(Exit.isFailure)).toBe(true);
      expect(Exit.isFailure(yield* Effect.exit(harness.client.echo({ text: "after close" })))).toBe(
        true,
      );
      for (let index = 0; index < calls.length; index++) yield* Queue.take(harness.stopped);
    }).pipe(Effect.scoped),
);

it.live("exposes the benchmark's per-stream client identity mismatch", () =>
  Effect.gen(function* () {
    const { client } = yield* openHarness;
    const identities = yield* Effect.forEach([1, 2, 3], () => client.identity());
    expect(new Set(identities).size).toBe(3);
  }).pipe(Effect.scoped),
);

it.live(
  "routes terminal Acks and Interrupt on their original stream with the production output window",
  () =>
    Effect.gen(function* () {
      const { session, output, client } = yield* openHarness;
      const stream = yield* Effect.promise(() => session.createBidirectionalStream());
      const writer = stream.writable.getWriter();
      const reader = stream.readable.getReader();
      const parser = makeFramedJson();
      const received: unknown[] = [];
      const readOne = Effect.gen(function* () {
        while (received.length === 0) {
          const next = yield* Effect.promise(() => reader.read());
          if (next.done) throw new Error("Terminal stream closed unexpectedly");
          received.push(...parser.decode(next.value));
        }
        return received.shift();
      });
      yield* Effect.promise(() =>
        writer.write(
          parser.encode({
            _tag: "Request",
            id: "terminal",
            tag: WS_METHODS.terminalObserve,
            payload: null,
            headers: [],
          }),
        ),
      );
      for (let index = 0; index < 8; index++) {
        yield* Queue.offer(output, String(index));
        expect(yield* readOne).toEqual({
          _tag: "Chunk",
          requestId: "terminal",
          values: [String(index)],
        });
      }
      yield* Queue.offer(output, "after ack");
      expect(yield* client.echo({ text: "concurrent" })).toBe("concurrent");
      yield* Effect.promise(() =>
        writer.write(parser.encode({ _tag: "Ack", requestId: "terminal" })),
      );
      expect(yield* readOne).toEqual({
        _tag: "Chunk",
        requestId: "terminal",
        values: ["after ack"],
      });
      yield* Effect.promise(() =>
        writer.write(parser.encode({ _tag: "Interrupt", requestId: "terminal" })),
      );
      expect(yield* readOne).toMatchObject({ _tag: "Exit", requestId: "terminal" });
      yield* Effect.promise(() => writer.close());
      yield* Effect.promise(() => reader.cancel());
    }).pipe(Effect.scoped),
);
