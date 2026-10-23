// @effect-diagnostics nodeBuiltinImport:off -- Runs the pinned addon with gated DNS and no real socket.
import * as NodeAssert from "node:assert/strict";
import type * as NodeDgram from "node:dgram";
import type * as NodeDnsPromises from "node:dns/promises";
import * as NodeEvents from "node:events";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";

import * as AuthenticatedRpcSession from "../../AuthenticatedRpcSession.ts";
import * as EnvironmentAuth from "../../auth/EnvironmentAuth.ts";
import * as ServerConfig from "../../config.ts";
import * as ServerEnvironment from "../../environment/ServerEnvironment.ts";
import * as ServerRuntimeStartup from "../../serverRuntimeStartup.ts";
import * as WebTransportAvailability from "../WebTransportAvailability.ts";
import * as WebTransportBackend from "../WebTransportBackend.ts";
import * as WebTransportListener from "../WebTransportListener.ts";

const lookupStarted = Promise.withResolvers<void>();
const lookupResult = Promise.withResolvers<{ family: number; address: string }>();
const require = NodeModule.createRequire(import.meta.url);
const mutableDNS = require("node:dns/promises") as typeof NodeDnsPromises;
const mutableDgram = require("node:dgram") as typeof NodeDgram;
let sockets = 0;
let closes = 0;
mutableDNS.lookup = (() => {
  lookupStarted.resolve();
  return lookupResult.promise;
}) as unknown as typeof NodeDnsPromises.lookup;
mutableDgram.createSocket = (() => {
  sockets++;
  return new (class extends NodeEvents.EventEmitter {
    bind() {
      queueMicrotask(() => this.emit("listening"));
    }
    address() {
      return { port: 4443, address: "127.0.0.1" };
    }
    close() {
      closes++;
      this.emit("close");
    }
  })();
}) as unknown as typeof NodeDgram.createSocket;
NodeModule.syncBuiltinESMExports();

const rawConfig = JSON.stringify({
  url: "https://localhost:4443/rpc",
  host: "localhost",
  port: 4443,
  certPath: process.argv[2],
  keyPath: process.argv[3],
});

const report = await Effect.runPromise(
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    const backend = yield* WebTransportBackend.WebTransportBackend;
    const { Http3Server } = yield* backend.load;
    const created = Promise.withResolvers<ObservedServer>();
    const stopped = Promise.withResolvers<void>();
    const endpoint = yield* Ref.make(Option.none<{ url: string; wireVersion: number }>());
    let stops = 0;
    let publications = 0;
    class ObservedServer extends Http3Server {
      constructor(options: ConstructorParameters<typeof Http3Server>[0]) {
        super(options);
        created.resolve(this);
      }
      override stopServer() {
        stops++;
        super.stopServer();
        stopped.resolve();
      }
    }
    return yield* Effect.gen(function* () {
      const listener = yield* WebTransportListener.WebTransportListener;
      const fiber = yield* listener.listen.pipe(Effect.forkScoped);
      yield* Effect.promise(() => lookupStarted.promise);
      const server = yield* Effect.promise(() => created.promise);
      yield* Fiber.interrupt(fiber);
      const stopsBeforeReady = stops;
      const socketsBeforeReady = sockets;
      const advertisedBeforeReady = Option.isSome(yield* Ref.get(endpoint));

      lookupResult.resolve({ family: 4, address: "127.0.0.1" });
      yield* Effect.promise(() => server.ready);
      yield* Effect.promise(() => stopped.promise);
      yield* Effect.promise(() => server.closed);
      NodeAssert.equal(stopsBeforeReady, 0);
      NodeAssert.equal(socketsBeforeReady, 0);
      NodeAssert.equal(advertisedBeforeReady, false);
      NodeAssert.equal(stops, 1);
      NodeAssert.equal(sockets, 1);
      NodeAssert.equal(closes, 1);
      NodeAssert.equal(publications, 0);
      NodeAssert.ok(Option.isNone(yield* Ref.get(endpoint)));
      return { stops, sockets, closes, publications };
    }).pipe(
      Effect.provide(
        WebTransportListener.layer.pipe(
          Layer.provide(
            Layer.succeed(WebTransportBackend.WebTransportBackend, {
              load: Effect.succeed({ Http3Server: ObservedServer }),
            }),
          ),
          Layer.provide(
            Layer.succeed(WebTransportAvailability.WebTransportAvailability, {
              get: Ref.get(endpoint),
              set: (value) =>
                Ref.set(endpoint, value).pipe(
                  Effect.andThen(
                    Effect.sync(() => {
                      if (Option.isSome(value)) publications++;
                    }),
                  ),
                ),
            }),
          ),
          Layer.provide(Layer.mock(AuthenticatedRpcSession.AuthenticatedRpcSession)({})),
          Layer.provide(Layer.mock(EnvironmentAuth.EnvironmentAuth)({})),
          Layer.provide(
            Layer.mock(ServerEnvironment.ServerEnvironmentIdentity)({
              getEnvironmentId: Effect.succeed(EnvironmentId.make("fixture")),
            }),
          ),
          Layer.provide(
            Layer.mock(ServerRuntimeStartup.ServerRuntimeStartup)({
              awaitCommandReady: Effect.void,
            }),
          ),
          Layer.provide(
            Layer.succeed(ServerConfig.ServerConfig, {
              ...config,
              rpcWebTransport: rawConfig,
            }),
          ),
        ),
      ),
    );
  }).pipe(
    Effect.scoped,
    Effect.provide(
      Layer.merge(
        ServerConfig.layerTest(process.cwd(), NodePath.dirname(process.argv[2]!)),
        WebTransportBackend.layer,
      ).pipe(Layer.provideMerge(NodeServices.layer)),
    ),
  ),
);
process.stdout.write(`${JSON.stringify(report)}\n`);
