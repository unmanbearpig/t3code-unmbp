import { expect, it } from "@effect/vitest";
import { EnvironmentId, type ReconnectLoopSample } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as Socket from "effect/socket/Socket";
import * as FetchHttpClient from "effect/http/FetchHttpClient";

import * as Persistence from "../platform/persistence.ts";
import { ReconnectLoopOutbox } from "../rpc/reconnectLoopSamples.ts";
import type { ConnectionCatalogEntry } from "./catalog.ts";
import { ConnectionTransientError } from "./model.ts";
import { runReconnectLoop } from "./reconnectLoop.ts";
import { EnvironmentRegistry } from "./registry.ts";
import { ConnectionResolver } from "./resolver.ts";

it.effect.each(["failed", "interrupted"] as const)(
  "durably records a %s preparation with run and network metadata before notifying the screen",
  (outcome) =>
    Effect.gen(function* () {
      const environmentId = EnvironmentId.make("test");
      const preparing = yield* Deferred.make<void>();
      const saved = yield* Queue.unbounded<ReconnectLoopSample>();
      const rows: ReconnectLoopSample[] = [];
      const entries = yield* SubscriptionRef.make(
        new Map([[environmentId, {} as ConnectionCatalogEntry]]),
      );
      const program = runReconnectLoop({
        environmentId,
        rounds: 1,
        runId: "run",
        platform: "android",
        appVersion: "test",
        networkType: Effect.succeed("cellular"),
        options: {},
        readTraffic: () => null,
        onPending: () => {},
        onRow: (row) => {
          expect(rows).toContain(row);
        },
      }).pipe(
        Effect.provideService(EnvironmentRegistry, {
          entries,
          run: () => Effect.succeed(0),
        } as unknown as EnvironmentRegistry["Service"]),
        Effect.provideService(ConnectionResolver, {
          prepare: () =>
            Deferred.succeed(preparing, undefined).pipe(
              Effect.andThen(
                outcome === "interrupted"
                  ? Effect.never
                  : Effect.fail(
                      new ConnectionTransientError({
                        reason: "remote-unavailable",
                        detail: "offline",
                      }),
                    ),
              ),
            ),
        } as unknown as ConnectionResolver["Service"]),
        Effect.provideService(Persistence.EnvironmentCacheStore, {
          loadServerConfig: () => Effect.succeedNone,
        } as unknown as Persistence.EnvironmentCacheStore["Service"]),
        Effect.provideService(Socket.WebSocketConstructor, () => {
          throw new Error("No socket should open");
        }),
        Effect.provideService(ReconnectLoopOutbox, {
          save: (_environmentId, row) =>
            Effect.sync(() => {
              rows.push(row);
            }).pipe(Effect.andThen(Queue.offer(saved, row)), Effect.asVoid),
          pending: () => Effect.succeed(rows),
          flush: () => Effect.succeed(0),
        }),
        Effect.provide(FetchHttpClient.layer),
      );
      const fiber = yield* program.pipe(Effect.forkScoped);
      yield* Deferred.await(preparing);
      if (outcome === "interrupted") yield* Fiber.interrupt(fiber);
      const row = yield* Queue.take(saved);
      expect(row).toMatchObject({
        id: "run:0",
        runId: "run",
        round: -1,
        arm: "warmup",
        networkType: "cellular",
        ok: false,
        serverVersion: null,
        transport: "unknown",
        error: outcome === "interrupted" ? "Interrupted" : "Connection attempt failed",
      });
      if (outcome === "failed") yield* Fiber.interrupt(fiber);
      expect(rows).toHaveLength(1);
    }),
);
