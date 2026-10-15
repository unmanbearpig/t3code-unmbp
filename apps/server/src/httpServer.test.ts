import { NodeWS } from "@effect/platform-node/NodeSocket";
import { expect, it } from "@effect/vitest";
import { AuthSessionId, EnvironmentId, ORCHESTRATION_PROTOCOL_VERSION } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServer from "effect/http/HttpServer";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as Socket from "effect/socket/Socket";
import * as NetAddress from "effect/net/NetAddress";

import * as EnvironmentAuth from "./auth/EnvironmentAuth.ts";
import * as SessionStore from "./auth/SessionStore.ts";
import * as ServerEnvironment from "./environment/ServerEnvironment.ts";
import { layerNodeHttpServer } from "./httpServer.ts";
import * as AnalyticsService from "./telemetry/AnalyticsService.ts";
import { layerWebSocketRoute } from "./ws.ts";

it.effect.each(["hello", "legacy"] as const)(
  "serves %s upgrades through the production listener and authenticated route",
  (mode) =>
    Effect.gen(function* () {
      const methods: string[] = [];
      const session: EnvironmentAuth.AuthenticatedSession = {
        sessionId: AuthSessionId.make("listener-session"),
        subject: "test",
        method: "bearer-access-token",
        scopes: ["orchestration:read"],
      };
      const dependencies = Layer.mergeAll(
        Layer.mock(EnvironmentAuth.EnvironmentAuth)({
          authenticateWebSocketUpgrade: () =>
            Effect.sync(() => {
              methods.push("upgrade");
              return session;
            }),
          authenticateWebSocketFrame: (token) =>
            Effect.sync(() => {
              expect(token).toBe("frame-credential");
              methods.push("frame");
              return session;
            }),
        }),
        Layer.mock(SessionStore.SessionStore)({
          cookieName: "test-session",
          legacyCookieName: undefined,
          recordClientConnection: () => Effect.void,
          markConnected: () => Effect.void,
          markDisconnected: () => Effect.void,
          awaitInvalidation: () => Effect.never,
        }),
        Layer.mock(ServerEnvironment.ServerEnvironment)({
          getDescriptor: Effect.succeed({
            environmentId: EnvironmentId.make("listener-test"),
            label: "Listener test",
            platform: { os: "linux", arch: "x64" },
            serverVersion: "test",
            orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
            capabilities: { repositoryIdentity: true, websocketHello: 1 },
          }),
        }),
        AnalyticsService.AnalyticsService.layerTest,
      );
      const routes = layerWebSocketRoute(() =>
        Effect.succeed(
          Effect.gen(function* () {
            const request = yield* HttpServerRequest.HttpServerRequest;
            const socket = yield* request.upgrade;
            const reader = yield* socket.reader;
            const writer = yield* socket.writer;
            return yield* Effect.forever(reader.pull.pipe(Effect.flatMap(writer.writeAll)));
          }).pipe(Effect.orDie),
        ),
      );
      const context = yield* Layer.build(
        HttpRouter.serve(routes, { disableLogger: true, disableListenLog: true }).pipe(
          Layer.provide(dependencies),
          Layer.provideMerge(layerNodeHttpServer({ host: "127.0.0.1", port: 0 })),
        ),
      );
      const { address } = Context.get(context, HttpServer.HttpServer);
      if (!NetAddress.isInetAddress(address)) throw new Error("Expected a TCP test listener");
      const query =
        mode === "hello"
          ? "handshake=hello-v1"
          : `orchestrationProtocol=${ORCHESTRATION_PROTOCOL_VERSION}`;
      let native: NodeWS.WebSocket | undefined;
      const socket = yield* Socket.makeWebSocket(`ws://127.0.0.1:${address.port}/ws?${query}`).pipe(
        Effect.provideService(Socket.WebSocketConstructor, (url) => {
          native = new NodeWS.WebSocket(url, { headers: { Cookie: "ambient=ignored" } });
          return native;
        }),
      );
      const reader = yield* socket.reader;
      const writer = yield* socket.writer;
      if (mode === "hello") {
        expect(String((yield* reader.pull)[0])).toContain('"Hello"');
        yield* writer.write(
          JSON.stringify({ _tag: "BearerAuth", handshakeVersion: 1, token: "frame-credential" }),
        );
        expect(String((yield* reader.pull)[0])).toContain('"Authenticated"');
      }
      const payload = "p".repeat(70_000);
      yield* writer.write(payload);
      expect(String((yield* reader.pull)[0])).toBe(payload);
      expect(methods).toEqual([mode === "hello" ? "frame" : "upgrade"]);
      expect(native?.extensions).toBe("permessage-deflate");
    }),
);
