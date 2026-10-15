import { expect, it } from "@effect/vitest";
import { EnvironmentId, ORCHESTRATION_PROTOCOL_VERSION, ReconnectHello } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as Socket from "effect/socket/Socket";
import {
  PrimaryConnectionTarget,
  type PreparedConnection,
  type ConnectionAttemptError,
} from "../connection/model.ts";
import * as Hello from "./hello.ts";

const json = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const target = new PrimaryConnectionTarget({
  environmentId: EnvironmentId.make("expected"),
  label: "Expected",
  httpBaseUrl: "https://test",
  wsBaseUrl: "wss://test",
});
const connection: PreparedConnection = {
  ...target,
  target,
  socketUrl: "wss://test/ws?handshake=hello-v1",
  httpAuthorization: { _tag: "Bearer", token: "secret" },
  handshake: { _tag: "Hello", version: 1 },
};
const hello: typeof ReconnectHello.Type = {
  _tag: "Hello",
  handshakeVersion: 1,
  environment: {
    environmentId: target.environmentId,
    label: "Expected",
    serverVersion: "test",
    orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
    capabilities: { repositoryIdentity: true, websocketHello: 1 },
    platform: { os: "linux", arch: "x64" },
  },
  resume: {},
};
const harness = Effect.gen(function* () {
  const input = yield* Queue.unbounded<[string, ...string[]]>();
  const writes = yield* Queue.unbounded<string>();
  const failure = yield* Deferred.make<never, ConnectionAttemptError>();
  let readers = 0;
  const raw = Socket.make({
    reader: Effect.sync(() => {
      readers++;
      return { pull: Queue.take(input), upgrade: Socket.SocketUpgradeError.unsupported };
    }),
    writer: Effect.succeed({
      write: (frame) => Queue.offer(writes, String(frame)).pipe(Effect.asVoid),
      writeAll: (frames) =>
        Effect.forEach(frames, (frame) => Queue.offer(writes, String(frame))).pipe(Effect.asVoid),
    }),
  });
  return {
    socket: yield* Hello.wrap(raw, connection, failure),
    input,
    writes,
    failure,
    readers: () => readers,
  };
});
it.effect(
  "sends no RPC before Hello, releases queued writes after auth sent, and consumes Authenticated before RPC",
  () =>
    Effect.gen(function* () {
      const h = yield* harness;
      const writer = yield* h.socket.writer;
      const write = yield* writer.write("rpc-request").pipe(Effect.forkChild);
      const read = yield* h.socket.reader.pipe(Effect.forkChild);
      yield* TestClock.adjust(0);
      expect(yield* Queue.size(h.writes)).toBe(0);
      yield* Queue.offer(h.input, [json(hello)]);
      const reader = yield* Fiber.join(read);
      yield* Fiber.join(write);
      expect(yield* Queue.take(h.writes)).toBe(
        json({ _tag: "BearerAuth", handshakeVersion: 1, token: "secret" }),
      );
      expect(yield* Queue.take(h.writes)).toBe("rpc-request");
      yield* Queue.offer(h.input, [
        json({ _tag: "Authenticated", handshakeVersion: 1 }),
        "rpc-reply",
      ]);
      expect(yield* reader.pull).toEqual(["rpc-reply"]);
      expect(h.readers()).toBe(1);
    }),
);
it.effect.each(["identity", "protocol"] as const)("rejects %s before sending the token", (kind) =>
  Effect.gen(function* () {
    const h = yield* harness;
    const wrong = {
      ...hello,
      environment: {
        ...hello.environment,
        ...(kind === "identity"
          ? { environmentId: EnvironmentId.make("other") }
          : { orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION + 1 }),
      },
    };
    yield* Queue.offer(h.input, [json(wrong)]);
    yield* h.socket.reader.pipe(Effect.flip);
    const error = yield* Deferred.await(h.failure).pipe(Effect.flip);
    expect(error._tag).toBe("ConnectionBlockedError");
    expect(error.reason).toBe(kind === "identity" ? "configuration" : "unsupported");
    expect(yield* Queue.size(h.writes)).toBe(0);
  }),
);
it.effect("marks missing Hello for bounded legacy discovery without sending the token", () =>
  Effect.gen(function* () {
    const h = yield* harness;
    const pending = yield* h.socket.reader.pipe(Effect.flip, Effect.forkChild);
    yield* TestClock.adjust(5000);
    yield* Fiber.join(pending);
    expect(yield* Deferred.await(h.failure).pipe(Effect.flip)).toMatchObject({
      helloFallback: true,
    });
    expect(yield* Queue.size(h.writes)).toBe(0);
  }),
);
it.effect("credential rejection after Hello cannot fall back", () =>
  Effect.gen(function* () {
    const h = yield* harness;
    yield* Queue.offer(h.input, [json(hello)]);
    const reader = yield* h.socket.reader;
    yield* Queue.offer(h.input, [
      json({ _tag: "HandshakeError", handshakeVersion: 1, code: "auth_rejected" }),
    ]);
    yield* reader.pull.pipe(Effect.flip);
    const error = yield* Deferred.await(h.failure).pipe(Effect.flip);
    expect(error).toMatchObject({ _tag: "ConnectionBlockedError", reason: "authentication" });
    expect("helloFallback" in error).toBe(false);
  }),
);

it.effect(
  "rejects unsupported handshake versions and explicit early credential rejection without fallback",
  () =>
    Effect.gen(function* () {
      for (const frame of [
        { ...hello, handshakeVersion: 2 },
        { _tag: "HandshakeError", handshakeVersion: 1, code: "auth_rejected" },
      ]) {
        const h = yield* harness;
        yield* Queue.offer(h.input, [json(frame)]);
        yield* h.socket.reader.pipe(Effect.flip);
        const error = yield* Deferred.await(h.failure).pipe(Effect.flip);
        expect(error._tag).toBe("ConnectionBlockedError");
        expect("helloFallback" in error).toBe(false);
        expect(yield* Queue.size(h.writes)).toBe(0);
      }
    }),
);
