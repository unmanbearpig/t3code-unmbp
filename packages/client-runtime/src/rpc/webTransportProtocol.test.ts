import { describe, expect, it } from "@effect/vitest";
import {
  createTransportByteBudget,
  makeRpcWebTransportFrameChannel,
  rpcWebTransportDefaultLimits,
  transportTry,
} from "@t3tools/shared/rpcWebTransport";
import { memorySessionPair } from "../../../shared/src/rpcWebTransport.test-support.ts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as TestClock from "effect/testing/TestClock";
import * as RpcClient from "effect/rpc/RpcClient";
import type * as RpcMessage from "effect/rpc/RpcMessage";
import {
  prepareRpcWebTransportClient,
  makeRpcWebTransportClientProtocol,
} from "./webTransportProtocol.ts";

const expected = { environmentId: "test", orchestrationProtocolVersion: 17 };
const ready = {
  _tag: "Ready",
  wireVersion: 1,
  orchestrationProtocolVersion: 17,
  environmentId: "test",
  encoding: "deflate",
  limits: rpcWebTransportDefaultLimits,
};

const handshake = Effect.fnUntraced(function* (override: Record<string, unknown> = {}) {
  const pair = memorySessionPair();
  const controls = yield* Queue.make<unknown>();
  const accepted = yield* Effect.gen(function* () {
    const stream = yield* transportTry((signal) => pair.server.acceptBidirectionalStream(signal));
    const channel = makeRpcWebTransportFrameChannel(
      stream,
      createTransportByteBudget(65536),
      createTransportByteBudget(65536),
      rpcWebTransportDefaultLimits,
    );
    expect(yield* channel.readPreface()).toBe("control");
    expect(yield* channel.read({ first: true, control: true })).toMatchObject({ _tag: "Hello" });
    yield* channel.write({ ...ready, ...override }, true);
    yield* Effect.gen(function* () {
      while (true) yield* Queue.offer(controls, yield* channel.read({ control: true }));
    }).pipe(Effect.ignore, Effect.forkScoped);
    return { channel, stream };
  }).pipe(Effect.forkScoped);
  const prepared = yield* prepareRpcWebTransportClient(pair.client, expected).pipe(Effect.exit);
  return { ...pair, prepared, accepted, controls };
});

describe("WebTransport client negotiation and liveness", () => {
  it.effect.each([
    { environmentId: "wrong" },
    { orchestrationProtocolVersion: 18 },
    { wireVersion: 2 },
    { encoding: "other" },
    { limits: { ...rpcWebTransportDefaultLimits, messageBytes: 0 } },
    { limits: { ...rpcWebTransportDefaultLimits, ordinaryRequestStreams: 64 } },
    { limits: { ...rpcWebTransportDefaultLimits, pendingOrdinaryRequests: -1 } },
  ])("rejects invalid Ready %# before attaching winner hooks", (override) =>
    Effect.gen(function* () {
      const events: string[] = [];
      const fixture = yield* handshake(override).pipe(
        Effect.provideService(RpcClient.ConnectionHooks, {
          onConnect: Effect.sync(() => {
            events.push("connect");
          }),
          onDisconnect: Effect.sync(() => {
            events.push("disconnect");
          }),
        }),
      );
      expect(Exit.isFailure(fixture.prepared)).toBe(true);
      expect(events).toEqual([]);
    }).pipe(Effect.scoped),
  );

  it.effect(
    "attaches hooks only after selection and calls ping timeout before disconnect even if writes stall",
    () =>
      Effect.gen(function* () {
        const fixture = yield* handshake();
        if (Exit.isFailure(fixture.prepared))
          return yield* Effect.failCause(fixture.prepared.cause);
        const events: string[] = [];
        const ended = yield* Deferred.make<void>();
        const protocol = yield* makeRpcWebTransportClientProtocol(fixture.prepared.value).pipe(
          Effect.provideService(RpcClient.ConnectionHooks, {
            onConnect: Effect.sync(() => {
              events.push("connect");
            }),
            onPingTimeout: Effect.sync(() => {
              events.push("ping-timeout");
            }),
            onDisconnect: Effect.sync(() => {
              events.push("disconnect");
            }).pipe(Effect.andThen(Deferred.succeed(ended, undefined))),
          }),
        );
        expect(events).toEqual(["connect"]);
        // No server decoded frames arrive after Ready. Pings are read but never answered.
        yield* TestClock.adjust(15000);
        yield* Deferred.await(ended);
        expect(events).toEqual(["connect", "ping-timeout", "disconnect"]);
        const failures = yield* Queue.make<RpcMessage.FromServerEncoded>();
        yield* protocol
          .run(9, (message) => Queue.offer(failures, message).pipe(Effect.asVoid))
          .pipe(Effect.forkScoped);
        expect((yield* Queue.take(failures))._tag).toBe("ClientProtocolError");
        expect(
          Exit.isFailure(
            yield* protocol
              .send(9, { _tag: "Request", id: 1, tag: "write", payload: null, headers: [] })
              .pipe(Effect.exit),
          ),
        ).toBe(true);
      }).pipe(Effect.scoped),
  );

  it.effect("counts only decoded frames as liveness and rejects a remote ClientProtocolError", () =>
    Effect.gen(function* () {
      const fixture = yield* handshake();
      if (Exit.isFailure(fixture.prepared)) return yield* Effect.failCause(fixture.prepared.cause);
      const protocol = yield* makeRpcWebTransportClientProtocol(fixture.prepared.value);
      const failures = yield* Queue.make<RpcMessage.FromServerEncoded>();
      yield* protocol
        .run(1, (message) => Queue.offer(failures, message).pipe(Effect.asVoid))
        .pipe(Effect.forkScoped);
      const { channel: serverControl } = yield* Fiber.join(fixture.accepted);
      yield* serverControl.write({ _tag: "ClientProtocolError", error: {} }, true);
      const failure = yield* Queue.take(failures);
      expect(failure._tag).toBe("ClientProtocolError");
      if (failure._tag === "ClientProtocolError")
        expect(failure.error.message).toContain("Invalid RPC transport envelope");
    }).pipe(Effect.scoped),
  );

  it.effect("does not let partial record byte progress refresh decoded-frame liveness", () =>
    Effect.gen(function* () {
      const fixture = yield* handshake();
      if (Exit.isFailure(fixture.prepared)) return yield* Effect.failCause(fixture.prepared.cause);
      const ended = yield* Deferred.make<void>();
      let timedOut = false;
      const protocol = yield* makeRpcWebTransportClientProtocol(fixture.prepared.value).pipe(
        Effect.provideService(RpcClient.ConnectionHooks, {
          onConnect: Effect.void,
          onPingTimeout: Effect.sync(() => {
            timedOut = true;
          }),
          onDisconnect: Deferred.succeed(ended, undefined).pipe(Effect.asVoid),
        }),
      );
      const failures = yield* Queue.make<RpcMessage.FromServerEncoded>();
      yield* protocol
        .run(1, (message) => Queue.offer(failures, message).pipe(Effect.asVoid))
        .pipe(Effect.forkScoped);
      const { stream } = yield* Fiber.join(fixture.accepted);
      const header = new Uint8Array(10);
      const view = new DataView(header.buffer);
      view.setUint8(0, 1);
      view.setUint8(1, 1);
      view.setUint32(2, 8192);
      view.setUint32(6, 8192);
      yield* transportTry((signal) => stream.write(header, signal));
      for (let i = 0; i < 3; i++) {
        yield* TestClock.adjust(4000);
        yield* transportTry((signal) => stream.write(new Uint8Array([0x20]), signal));
      }
      yield* TestClock.adjust(3000);
      yield* Deferred.await(ended);
      expect(timedOut).toBe(true);
      const failure = yield* Queue.take(failures);
      expect(failure._tag).toBe("ClientProtocolError");
      if (failure._tag === "ClientProtocolError")
        expect(failure.error.message).toContain("RPC transport ping timeout");
    }).pipe(Effect.scoped),
  );
});
