// @effect-diagnostics nodeBuiltinImport:off anyUnknownInErrorContext:off - Node HTTP adapters erase application error types, as HttpServer.make requires.
import * as NodeHttp from "node:http";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import { HelloWebSocketServer } from "./auth/HelloUpgrade.ts";
import { RECONNECT_HANDSHAKE_MODE, RECONNECT_HANDSHAKE_QUERY_PARAM } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import type * as HttpMiddleware from "effect/http/HttpMiddleware";
import * as HttpServer from "effect/http/HttpServer";

import { guardHttpResponseWriteErrors } from "./httpResponseErrorGuard.ts";

/** A different factory can supply TLS while preserving the HTTP and WebSocket settings. */
export const layerNodeHttpServer = (
  address: { readonly host: string; readonly port: number },
  createServer: () => NodeHttp.Server = NodeHttp.createServer,
) =>
  Layer.effect(
    HttpServer.HttpServer,
    Effect.gen(function* () {
      const nodeServer = guardHttpResponseWriteErrors(createServer());
      const server = yield* NodeHttpServer.make(() => nodeServer, {
        ...address,
        gracefulShutdownTimeout: 0,
        websocket: { perMessageDeflate: true },
      });
      const helloWss = yield* Effect.acquireRelease(
        Effect.sync(() => new HelloWebSocketServer()),
        (wss) =>
          Effect.sync(() => {
            wss.close();
          }),
      );
      return HttpServer.make({
        address: server.address,
        serve: Effect.fnUntraced(function* (app, middleware) {
          const previous = new Set(nodeServer.listeners("upgrade"));
          // HttpServer.make erases middleware requirements at this adapter boundary.
          const serve = server.serve as Parameters<typeof HttpServer.make>[0]["serve"];
          yield* serve(app, middleware);
          const ordinary = nodeServer
            .listeners("upgrade")
            .find((listener) => !previous.has(listener)) as (
            request: NodeHttp.IncomingMessage,
            socket: import("node:stream").Duplex,
            head: Buffer,
          ) => void;
          const scope = yield* Effect.scope;
          const hello = yield* NodeHttpServer.makeUpgradeHandler(Effect.succeed(helloWss), app, {
            scope,
            middleware: middleware as
              | HttpMiddleware.HttpMiddleware.Applied<
                  typeof app,
                  Effect.Error<typeof app>,
                  Effect.Services<typeof app>
                >
              | undefined,
          });
          const upgrade: typeof hello = (request, socket, head) => {
            const url = new URL(request.url ?? "/", "http://localhost");
            if (
              url.pathname === "/ws" &&
              url.searchParams.get(RECONNECT_HANDSHAKE_QUERY_PARAM) === RECONNECT_HANDSHAKE_MODE
            ) {
              hello(request, socket, head);
            } else {
              ordinary(request, socket, head);
            }
          };
          nodeServer.off("upgrade", ordinary);
          nodeServer.on("upgrade", upgrade);
          yield* Scope.addFinalizer(
            scope,
            Effect.sync(() => {
              nodeServer.off("upgrade", upgrade);
            }),
          );
        }),
      });
    }),
  ).pipe(Layer.merge(NodeHttpServer.layerHttpServices));
