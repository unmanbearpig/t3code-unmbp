// @effect-diagnostics nodeBuiltinImport:off - Native integration fixtures stay below codex-tmp.
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { AuthStandardClientScopes, EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";

import * as ServerSecretStore from "../../../src/auth/ServerSecretStore.ts";
import * as SessionStore from "../../../src/auth/SessionStore.ts";
import * as ServerConfig from "../../../src/config.ts";
import * as ServerEnvironment from "../../../src/environment/ServerEnvironment.ts";
import * as SqlitePersistence from "../../../src/persistence/Sqlite.ts";
import { openQuicFixture } from "./quicFixture.ts";
import { ticketRequestHandler } from "./ticketHandshake.ts";

it.live(
  "rejects CONNECT without a ticket, accepts scoped tickets, closes on revocation and rejects reuse",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({
        directory: NodePath.join(NodeOS.homedir(), "codex-tmp"),
        prefix: "t3-quic-auth-",
      });
      const layer = SessionStore.layer.pipe(
        Layer.provide(SqlitePersistence.layerMemory),
        Layer.provide(ServerSecretStore.layer),
        Layer.provide(
          Layer.succeed(ServerEnvironment.ServerEnvironmentIdentity, {
            getEnvironmentId: Effect.succeed(EnvironmentId.make("quic-protocol-fixture")),
          }),
        ),
        Layer.provide(ServerConfig.layerTest(process.cwd(), directory)),
      );
      yield* Effect.gen(function* () {
        const sessions = yield* SessionStore.SessionStore;
        const fixture = yield* Effect.acquireRelease(
          Effect.promise(() => openQuicFixture({ request: ticketRequestHandler(sessions) })),
          (value) => Effect.promise(() => value.close()),
        );
        const admitted = fixture.sessions.getReader();
        expect(yield* Effect.tryPromise(() => fixture.connect()).pipe(Effect.isFailure)).toBe(true);
        const issued = yield* sessions.issue({ subject: "quic-test" });
        const ticket = yield* sessions.issueWebSocketToken(issued.sessionId);
        expect((yield* sessions.verifyWebSocketToken(ticket.token)).scopes).toEqual(
          AuthStandardClientScopes,
        );
        const client = yield* Effect.promise(() => fixture.connect(`?wsTicket=${ticket.token}`));
        const accepted = yield* Effect.promise(() => admitted.read());
        if (accepted.done) throw new Error("Authenticated QUIC session was not admitted");
        yield* Effect.promise(() => accepted.value.ready);
        yield* sessions.markConnected(issued.sessionId);
        expect(
          (yield* sessions.listActive()).find((entry) => entry.sessionId === issued.sessionId)
            ?.connected,
        ).toBe(true);
        const invalidation = yield* sessions
          .awaitInvalidation(issued.sessionId)
          .pipe(
            Effect.andThen(Effect.sync(() => accepted.value.close())),
            Effect.forkScoped({ startImmediately: true }),
          );
        yield* sessions.revoke(issued.sessionId);
        yield* Fiber.join(invalidation);
        yield* Effect.promise(() => client.closed);
        yield* sessions.markDisconnected(issued.sessionId);
        expect(
          yield* Effect.tryPromise(() => fixture.connect(`?wsTicket=${ticket.token}`)).pipe(
            Effect.isFailure,
          ),
        ).toBe(true);
      }).pipe(Effect.provide(layer));
    }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
);
