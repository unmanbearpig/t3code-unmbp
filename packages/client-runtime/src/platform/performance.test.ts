import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";

import { measure, type PerformanceMeasurement } from "./performance.ts";

describe("optional local performance observation", () => {
  it.effect("measures elapsed work and includes only numeric result details", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const finish = yield* Deferred.make<string>();
      const measurements: PerformanceMeasurement[] = [];
      const work = Deferred.succeed(started, undefined).pipe(
        Effect.andThen(Deferred.await(finish)),
      );
      const fiber = yield* measure(
        work,
        { record: (value) => measurements.push(value) },
        { operation: "cache.encode", category: "thread" },
        (value) => ({ payloadChars: value.length }),
      ).pipe(Effect.forkChild);
      yield* Deferred.await(started);
      yield* TestClock.adjust("25 millis");
      yield* Deferred.succeed(finish, "private content");
      expect(yield* Fiber.join(fiber)).toBe("private content");
      expect(measurements).toEqual([
        {
          operation: "cache.encode",
          category: "thread",
          durationMs: 25,
          outcome: "success",
          payloadChars: 15,
        },
      ]);
    }),
  );

  it.effect("preserves failures and records interruption without swallowing it", () =>
    Effect.gen(function* () {
      const measurements: PerformanceMeasurement[] = [];
      const observer = { record: (value: PerformanceMeasurement) => measurements.push(value) };
      const failure = yield* measure(Effect.fail("original failure"), observer, {
        operation: "rpc.request",
      }).pipe(Effect.exit);
      expect(failure).toEqual(Exit.fail("original failure"));
      const started = yield* Deferred.make<void>();
      const work = Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never));
      const fiber = yield* measure(work, observer, { operation: "rpc.request" }).pipe(
        Effect.forkChild,
      );
      yield* Deferred.await(started);
      yield* Fiber.interrupt(fiber);
      expect(measurements.map((measurement) => measurement.outcome)).toEqual([
        "failure",
        "interrupted",
      ]);
    }),
  );

  it.effect("works without an observer and ignores a broken optional observer", () =>
    Effect.gen(function* () {
      expect(yield* measure(Effect.succeed(42), null, { operation: "rpc.request" })).toBe(42);
      expect(
        yield* measure(
          Effect.succeed(42),
          {
            record: () => {
              throw new Error("storage unavailable");
            },
          },
          { operation: "rpc.request" },
        ),
      ).toBe(42);
    }),
  );
});
