import { expect, it } from "@effect/vitest";
import {
  AuthSessionId,
  EnvironmentId,
  ORCHESTRATION_PROTOCOL_VERSION,
  type ReconnectHello,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as Rpc from "effect/rpc/Rpc";
import * as RpcGroup from "effect/rpc/RpcGroup";
import * as RpcServer from "effect/rpc/RpcServer";
import * as RpcSerialization from "effect/rpc/RpcSerialization";
import * as Socket from "effect/socket/Socket";
import * as EnvironmentAuth from "./EnvironmentAuth.ts";
import * as Hello from "./WebSocketHello.ts";

const json = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const hello: ReconnectHello = {
  _tag: "Hello",
  handshakeVersion: 1,
  environment: {
    environmentId: EnvironmentId.make("hello-test"),
    label: "Hello test",
    platform: { os: "linux", arch: "x64" },
    serverVersion: "test",
    orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
    capabilities: { repositoryIdentity: true, websocketHello: 1 },
  },
  resume: { threadResumeCompletionMarker: true, shellResumeCompletionMarker: true },
};
const session: EnvironmentAuth.AuthenticatedSession = {
  sessionId: AuthSessionId.make("test-session"),
  subject: "test",
  method: "bearer-access-token",
  scopes: ["orchestration:read"],
};
const authFrame = json({ _tag: "BearerAuth", handshakeVersion: 1, token: "credential" });
const rpcFrame = json({ _tag: "Request", id: "1", tag: "run", payload: {}, headers: [] });
const makeSocket = Effect.gen(function* () {
  const input = yield* Queue.unbounded<[string, ...string[]]>();
  const writes = yield* Queue.unbounded<string>();
  let readers = 0;
  const socket = Socket.make({
    reader: Effect.sync(() => {
      readers++;
      return { pull: Queue.take(input), upgrade: Socket.SocketUpgradeError.unsupported };
    }),
    writer: Effect.succeed({
      write: (frame) =>
        typeof frame === "string" ? Queue.offer(writes, frame).pipe(Effect.asVoid) : Effect.void,
      writeAll: (frames) =>
        Effect.forEach(frames, (frame) => Queue.offer(writes, String(frame))).pipe(Effect.asVoid),
    }),
  });
  return { socket, input, writes, readers: () => readers };
});
const authLayer = (
  authenticate: EnvironmentAuth.EnvironmentAuth["Service"]["authenticateWebSocketFrame"] = () =>
    Effect.succeed(session),
) => Layer.mock(EnvironmentAuth.EnvironmentAuth)({ authenticateWebSocketFrame: authenticate });

it.effect(
  "sends Hello before auth, adopts exactly one reader, and executes pipelined RPC through the existing helper",
  () =>
    Effect.gen(function* () {
      const transport = yield* makeSocket;
      const releaseAuth = yield* Deferred.make<void>();
      const authStarted = yield* Deferred.make<void>();
      const called = yield* Deferred.make<void>();
      let executions = 0;
      const group = RpcGroup.make(
        Rpc.make("run", { payload: Schema.Struct({}), success: Schema.Void }),
      );
      const serving = yield* Effect.gen(function* () {
        const authenticated = yield* Hello.authenticate(transport.socket, hello);
        expect(authenticated).toBeDefined();
        const { protocol, httpEffect } = yield* RpcServer.makeProtocolWithHttpEffectWebsocket;
        yield* RpcServer.make(group, { disableTracing: true }).pipe(
          Effect.provideService(RpcServer.Protocol, protocol),
          Effect.provide(
            group.toLayer({
              run: () =>
                Effect.sync(() => {
                  executions++;
                }).pipe(Effect.andThen(Deferred.succeed(called, undefined)), Effect.asVoid),
            }),
          ),
          Effect.forkScoped,
        );
        const base = HttpServerRequest.fromWeb(new Request("http://test/ws"));
        const request = new Proxy(base, {
          get: (target, key, receiver) =>
            key === "upgrade"
              ? Effect.succeed(authenticated!.socket)
              : Reflect.get(target, key, receiver),
        });
        return yield* httpEffect.pipe(
          Effect.provideService(HttpServerRequest.HttpServerRequest, request),
        );
      }).pipe(
        Effect.provide(
          Layer.merge(
            RpcSerialization.layerJson,
            authLayer(() =>
              Deferred.succeed(authStarted, undefined).pipe(
                Effect.andThen(Deferred.await(releaseAuth)),
                Effect.as(session),
              ),
            ),
          ),
        ),
        Effect.forkScoped,
      );
      expect(yield* decodeJson(yield* Queue.take(transport.writes))).toEqual(hello);
      yield* Queue.offer(transport.input, [authFrame, rpcFrame]);
      yield* Deferred.await(authStarted);
      expect(executions).toBe(0);
      yield* Deferred.succeed(releaseAuth, undefined);
      yield* Deferred.await(called);
      expect(executions).toBe(1);
      expect(transport.readers()).toBe(1);
      expect(yield* Queue.take(transport.writes)).toContain('"Authenticated"');
      yield* Fiber.interrupt(serving);
    }),
);

it.effect.each([
  { name: "RPC first", frame: rpcFrame, code: "invalid_frame" },
  {
    name: "oversized",
    frame: json({ _tag: "BearerAuth", handshakeVersion: 1, token: "x".repeat(8192) }),
    code: "limit_exceeded",
  },
  {
    name: "missing bearer",
    frame: json({ _tag: "BearerAuth", handshakeVersion: 1 }),
    code: "invalid_frame",
  },
] as const)("rejects $name auth without adopting RPC", ({ frame, code }) =>
  Effect.gen(function* () {
    const transport = yield* makeSocket;
    yield* Queue.offer(transport.input, [frame, rpcFrame]);
    let authenticated = false;
    const result = yield* Hello.authenticate(transport.socket, hello).pipe(
      Effect.provide(
        authLayer(() =>
          Effect.sync(() => {
            authenticated = true;
            return session;
          }),
        ),
      ),
    );
    expect(result).toBeUndefined();
    expect(authenticated).toBe(false);
    yield* Queue.take(transport.writes);
    expect(yield* Queue.take(transport.writes)).toBe(
      json({ _tag: "HandshakeError", handshakeVersion: 1, code }),
    );
  }),
);

it.effect("discards queued RPC after credential rejection without exposing the credential", () =>
  Effect.gen(function* () {
    const transport = yield* makeSocket;
    yield* Queue.offer(transport.input, [authFrame, rpcFrame]);
    const result = yield* Hello.authenticate(transport.socket, hello).pipe(
      Effect.provide(
        authLayer(() =>
          Effect.fail(
            new EnvironmentAuth.ServerAuthInvalidCredentialError({ diagnostic: "credential" }),
          ),
        ),
      ),
    );
    expect(result).toBeUndefined();
    yield* Queue.take(transport.writes);
    expect(yield* Queue.take(transport.writes)).toBe(
      json({ _tag: "HandshakeError", handshakeVersion: 1, code: "auth_rejected" }),
    );
  }),
);

it.effect("expires a late auth frame from upgrade time", () =>
  Effect.gen(function* () {
    const transport = yield* makeSocket;
    const pending = yield* Hello.authenticate(transport.socket, hello).pipe(
      Effect.provide(authLayer()),
      Effect.forkChild,
    );
    yield* Queue.take(transport.writes);
    yield* TestClock.adjust(5000);
    expect(yield* Fiber.join(pending)).toBeUndefined();
    expect(yield* Queue.take(transport.writes)).toBe(
      json({ _tag: "HandshakeError", handshakeVersion: 1, code: "auth_timeout" }),
    );
  }),
);

it.effect.each(["frames", "bytes"] as const)(
  "bounds pre-auth buffered %s while verification is pending",
  (limit) =>
    Effect.gen(function* () {
      const transport = yield* makeSocket;
      const started = yield* Deferred.make<void>();
      const pending = yield* Hello.authenticate(transport.socket, hello).pipe(
        Effect.provide(
          authLayer(() => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never))),
        ),
        Effect.forkChild,
      );
      yield* Queue.take(transport.writes);
      yield* Queue.offer(transport.input, [authFrame]);
      yield* Deferred.await(started);
      yield* Queue.offer(
        transport.input,
        limit === "frames" ? ["", ...Array.from({ length: 16 }, () => "")] : ["x".repeat(65537)],
      );
      expect(yield* Fiber.join(pending)).toBeUndefined();
      expect(yield* Queue.take(transport.writes)).toBe(
        json({ _tag: "HandshakeError", handshakeVersion: 1, code: "limit_exceeded" }),
      );
    }),
);

it.effect("does not adopt a socket rejected by raw admission during verification", () =>
  Effect.gen(function* () {
    const transport = yield* makeSocket;
    yield* Queue.offer(transport.input, [authFrame, rpcFrame]);
    const result = yield* Hello.authenticate(transport.socket, hello, () => false).pipe(
      Effect.provide(authLayer()),
    );
    expect(result).toBeUndefined();
    yield* Queue.take(transport.writes);
    expect(yield* Queue.take(transport.writes)).toBe(
      json({ _tag: "HandshakeError", handshakeVersion: 1, code: "limit_exceeded" }),
    );
  }),
);

it.effect("charges reader acquisition against the same five-second deadline", () =>
  Effect.gen(function* () {
    const transport = yield* makeSocket;
    const socket = Socket.make({
      ...transport.socket,
      reader: transport.socket.reader.pipe(Effect.delay(4000)),
    });
    const pending = yield* Hello.authenticate(socket, hello).pipe(
      Effect.provide(authLayer()),
      Effect.forkChild,
    );
    yield* TestClock.adjust(4000);
    yield* Queue.take(transport.writes);
    yield* TestClock.adjust(1000);
    expect(yield* Fiber.join(pending)).toBeUndefined();
    expect(yield* Queue.take(transport.writes)).toBe(
      json({ _tag: "HandshakeError", handshakeVersion: 1, code: "auth_timeout" }),
    );
  }),
);
