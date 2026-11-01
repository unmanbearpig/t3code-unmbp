import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import type { RpcClientId } from "@t3tools/contracts";
import {
  RPC_WEBTRANSPORT_LIMITS,
  createTransportByteBudget,
} from "@t3tools/shared/rpcWebTransport";
import { memorySessionPair } from "../../../packages/shared/src/rpcWebTransport.test-support.ts";
import {
  makeRpcWebTransportClientProtocol,
  prepareRpcWebTransportClient,
} from "../../../packages/client-runtime/src/rpc/webTransportProtocol.ts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { Rpc, RpcClient, RpcGroup, RpcServer } from "effect/rpc";

import * as EnvironmentAuth from "./auth/EnvironmentAuth.ts";
import * as SessionStore from "./auth/SessionStore.ts";
import * as ServerSecretStore from "./auth/ServerSecretStore.ts";
import * as BackgroundPolicy from "./background/BackgroundPolicy.ts";
import * as HostPowerMonitor from "./background/HostPowerMonitor.ts";
import * as ServerConfig from "./config.ts";
import * as ServerEnvironment from "./environment/ServerEnvironment.ts";
import * as SqlitePersistence from "./persistence/Sqlite.ts";
import * as RpcSessionLifetime from "./RpcSessionLifetime.ts";
import { makeRpcSessionProtocol } from "./RpcSessionProtocol.ts";
import * as ServerSettings from "./serverSettings.ts";
import * as AnalyticsService from "./telemetry/AnalyticsService.ts";
import { prepareRpcWebTransportServer } from "./transport/WebTransportProtocol.ts";

const authLayer = EnvironmentAuth.layer.pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
  Layer.provide(ServerSecretStore.layer),
  Layer.provide(ServerEnvironment.layerIdentity),
  Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-rpc-lifetime-" })),
);
const policyLayer = BackgroundPolicy.layer.pipe(
  Layer.provide(Layer.effect(HostPowerMonitor.HostPowerMonitor, HostPowerMonitor.make())),
  Layer.provide(ServerSettings.layerTest()),
);
const testLayer = RpcSessionLifetime.layer.pipe(
  Layer.provideMerge(authLayer),
  Layer.provideMerge(policyLayer),
  Layer.provide(
    Layer.succeed(AnalyticsService.AnalyticsService, {
      record: () => Effect.void,
      recordOwners: () => Effect.void,
      flush: Effect.void,
    }),
  ),
  Layer.provide(NodeServices.layer),
);
const Group = RpcGroup.make(Rpc.make("listen", { success: Schema.String, stream: true }));

const report = Effect.fnUntraced(function* (
  session: EnvironmentAuth.AuthenticatedSession,
  owner: RpcClientId,
) {
  const policy = yield* BackgroundPolicy.BackgroundPolicy;
  yield* policy.reportClientActivity(session.sessionId, owner, {
    clientId: "same-client",
    clientKind: "web",
    visible: true,
    focused: true,
    recentlyInteracted: true,
    scopes: [{ type: "server-config" }],
    observedAt: DateTime.toUtc(yield* DateTime.now),
  });
});

it.effect("isolates concurrent same-credential connections and a delayed old finalizer", () =>
  Effect.gen(function* () {
    const auth = yield* EnvironmentAuth.EnvironmentAuth;
    const lifetime = yield* RpcSessionLifetime.RpcSessionLifetime;
    const policy = yield* BackgroundPolicy.BackgroundPolicy;
    const sessions = yield* SessionStore.SessionStore;
    const issued = yield* auth.issueSession();
    const ticket = yield* auth.issueWebSocketTicket(issued);
    const session = yield* auth.authenticateRpcTicket(ticket.ticket);
    const start = Effect.fnUntraced(function* (late = false) {
      const arrived = yield* Deferred.make<RpcClientId>();
      const stop = yield* Deferred.make<void>();
      const cleanup = yield* Deferred.make<void>();
      const allowCleanup = yield* Deferred.make<void>();
      const fiber = yield* lifetime
        .run(session, {}, {}, (owner) =>
          report(session, owner).pipe(
            Effect.andThen(Deferred.succeed(arrived, owner)),
            Effect.andThen(Deferred.await(stop)),
            Effect.ensuring(
              late
                ? Deferred.succeed(cleanup, undefined).pipe(
                    Effect.andThen(Deferred.await(allowCleanup)),
                  )
                : Effect.void,
            ),
          ),
        )
        .pipe(Effect.forkScoped);
      return { owner: yield* Deferred.await(arrived), fiber, stop, cleanup, allowCleanup };
    });
    const old = yield* start(true);
    const second = yield* start();
    expect((yield* policy.snapshot).leases).toHaveLength(2);
    yield* Deferred.succeed(old.stop, undefined);
    yield* Deferred.await(old.cleanup);
    const replacement = yield* start();
    expect(new Set([old.owner, second.owner, replacement.owner]).size).toBe(3);
    yield* Deferred.succeed(old.allowCleanup, undefined);
    yield* Fiber.join(old.fiber);
    expect((yield* policy.snapshot).leases.map((lease) => lease.rpcClientId).sort()).toEqual(
      [second.owner, replacement.owner].sort(),
    );
    expect(
      (yield* sessions.listActive()).find((s) => s.sessionId === session.sessionId)?.connected,
    ).toBe(true);
    yield* Deferred.succeed(second.stop, undefined);
    yield* Fiber.join(second.fiber);
    expect((yield* policy.snapshot).leases).toHaveLength(1);
    yield* Deferred.succeed(replacement.stop, undefined);
    yield* Fiber.join(replacement.fiber);
    expect((yield* policy.snapshot).leases).toHaveLength(0);
    expect(
      (yield* sessions.listActive()).find((s) => s.sessionId === session.sessionId)?.connected,
    ).toBe(false);
  }).pipe(Effect.scoped, Effect.provide(testLayer)),
);

it.effect.each(["logical-close", "revocation", "expiry"] as const)(
  "interrupts a real RPC handler and releases leases on %s without native close acknowledgement",
  (termination) =>
    Effect.gen(function* () {
      const auth = yield* EnvironmentAuth.EnvironmentAuth;
      const sessions = yield* SessionStore.SessionStore;
      const lifetime = yield* RpcSessionLifetime.RpcSessionLifetime;
      const policy = yield* BackgroundPolicy.BackgroundPolicy;
      const issued = yield* auth.issueSession({ ttl: Duration.seconds(10) });
      const session = yield* auth.authenticateRpcTicket(
        (yield* auth.issueWebSocketTicket(issued)).ticket,
      );
      const pair = memorySessionPair({ neverAcknowledgeClose: true });
      let nativeAcknowledged = false;
      void pair.rawServer.closed.then(() => {
        nativeAcknowledged = true;
      });
      const expected = { environmentId: "lifetime-fixture", orchestrationProtocolVersion: 1 };
      const prepared = yield* prepareRpcWebTransportServer(pair.server, {
        ...expected,
        budgets: {
          input: createTransportByteBudget(RPC_WEBTRANSPORT_LIMITS.listenerInputBytes),
          output: createTransportByteBudget(RPC_WEBTRANSPORT_LIMITS.listenerOutputBytes),
        },
      }).pipe(Effect.forkScoped);
      const clientPreparing = yield* prepareRpcWebTransportClient(pair.client, expected).pipe(
        Effect.forkScoped,
      );
      const server = yield* Fiber.join(prepared);
      const started = yield* Deferred.make<void>();
      const interrupted = yield* Deferred.make<void>();
      const guarded = makeRpcSessionProtocol({
        ...server.protocol,
        // These operations model native acknowledgements that never arrive.
        send: (id, message, transferables) =>
          message._tag === "Exit" ? Effect.never : server.protocol.send(id, message, transferables),
        end: () => Effect.never,
      });
      const serverOwner = yield* lifetime
        .run(session, {}, {}, (owner) =>
          Effect.scoped(
            Effect.gen(function* () {
              yield* report(session, owner);
              yield* RpcServer.make(Group).pipe(
                Effect.provideService(RpcServer.Protocol, guarded.protocol),
                Effect.provide(
                  Group.toLayer({
                    listen: () =>
                      Stream.fromEffect(Deferred.succeed(started, undefined)).pipe(
                        Stream.flatMap(() => Stream.never),
                        Stream.ensuring(Deferred.succeed(interrupted, undefined)),
                      ),
                  }),
                ),
                Effect.forkScoped,
              );
              yield* server.closed;
            }).pipe(Effect.ensuring(guarded.close)),
          ),
        )
        .pipe(Effect.ensuring(server.close), Effect.forkScoped);
      const protocol = yield* makeRpcWebTransportClientProtocol(yield* Fiber.join(clientPreparing));
      const client = yield* RpcClient.make(Group).pipe(
        Effect.provideService(RpcClient.Protocol, protocol),
      );
      yield* client.listen().pipe(Stream.runDrain, Effect.ignore, Effect.forkScoped);
      yield* Deferred.await(started);
      expect((yield* policy.snapshot).leases).toHaveLength(1);
      if (termination === "logical-close") yield* server.close;
      else if (termination === "revocation") yield* sessions.revoke(session.sessionId);
      else yield* TestClock.adjust("10 seconds");
      yield* Deferred.await(interrupted);
      yield* Fiber.join(serverOwner);
      expect((yield* policy.snapshot).leases).toHaveLength(0);
      expect(nativeAcknowledged).toBe(false);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
);
