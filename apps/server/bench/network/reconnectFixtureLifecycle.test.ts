import { expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as ServerLifecycleEvents from "../../src/serverLifecycleEvents.ts";
import { operationServerConfig } from "./reconnectFixture.ts";
import * as FixtureLifecycle from "./reconnectFixtureLifecycle.ts";

it.effect(
  "replays one stable ready proof across sessions and changes it for independent fixtures",
  () =>
    Effect.gen(function* () {
      const fixture = yield* Layer.build(FixtureLifecycle.layer(operationServerConfig.environment));
      const read = FixtureLifecycle.subscribe.pipe(
        Effect.flatMap(Stream.runHead),
        Effect.provide(fixture),
      );
      const first = Option.getOrThrow(yield* read);
      expect(Option.getOrThrow(yield* read)).toEqual(first);
      const restarted = yield* Layer.build(
        FixtureLifecycle.layer(operationServerConfig.environment),
      );
      const independent = Option.getOrThrow(
        yield* FixtureLifecycle.subscribe.pipe(
          Effect.flatMap(Stream.runHead),
          Effect.provide(restarted),
        ),
      );
      expect(first.type).toBe("ready");
      if (first.type !== "ready" || independent.type !== "ready") throw new Error("Missing ready");
      expect(independent.payload.at).not.toBe(first.payload.at);
      expect(Option.isSome(DateTime.make(independent.payload.at))).toBe(true);
    }).pipe(Effect.scoped),
);

it.effect(
  "delivers later lifecycle events and cancels a subscription without closing the fixture",
  () =>
    Effect.gen(function* () {
      const fixture = yield* Layer.build(FixtureLifecycle.layer(operationServerConfig.environment));
      const service = yield* ServerLifecycleEvents.ServerLifecycleEvents.pipe(
        Effect.provide(fixture),
      );
      const stream = yield* FixtureLifecycle.subscribe.pipe(Effect.provide(fixture));
      const scope = yield* Scope.make();
      const received =
        yield* Queue.unbounded<import("@t3tools/contracts").ServerLifecycleStreamEvent>();
      const fiber = yield* stream.pipe(
        Stream.runForEach((event) => Queue.offer(received, event)),
        Effect.forkIn(scope),
      );
      const ready = yield* Queue.take(received);
      const published = yield* service.publish({
        version: 1,
        type: "ready",
        payload: { at: "2026-10-15T00:00:00.000Z", environment: operationServerConfig.environment },
      });
      expect(yield* Queue.take(received)).toEqual(published);
      expect(published.sequence).toBeGreaterThan(ready.sequence);
      yield* Fiber.interrupt(fiber);
      yield* Scope.close(scope, yield* Effect.exit(Effect.void));
      const replay = Option.getOrThrow(yield* stream.pipe(Stream.runHead));
      expect(replay).toEqual(published);
    }).pipe(Effect.scoped),
);
