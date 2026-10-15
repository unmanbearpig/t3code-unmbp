import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import { observedScope, observedStream } from "./reconnectLifecycle.ts";
import {
  assertIsolation,
  makeResourceTracker,
  type IsolationEvidence,
} from "./reconnectIsolation.ts";
import { scheduleLanes } from "./reconnectSchedule.ts";
import { makeLifecycleManifest, makeManifest } from "./reconnectManifest.ts";
import { workloadPlan } from "./reconnectWorkload.ts";

const evidence: IsolationEvidence = {
  epoch: "one",
  environmentId: "one",
  initialMessages: 100,
  initialSequence: 410,
  httpPoolFresh: true,
  shellCacheEmpty: true,
  helloSeeded: false,
  socketsBefore: 0,
  socketsAfter: 0,
  serverConnectionsAfter: 0,
};
it("refuses reused identities, caches, pools, fixture mutations, and leaked sockets", () => {
  expect(() =>
    assertIsolation({ ...evidence, epoch: "two", environmentId: "two" }, evidence),
  ).not.toThrow();
  expect(() => assertIsolation(evidence, evidence)).toThrow("identity");
  for (const patch of [
    { httpPoolFresh: false },
    { shellCacheEmpty: false },
    { initialMessages: 120 },
    { initialSequence: 460 },
    { socketsBefore: 1 },
    { socketsAfter: 1 },
    { serverConnectionsAfter: 1 },
  ])
    expect(() => assertIsolation({ ...evidence, ...patch })).toThrow("reset");
  // Support is an explicit seed, not a persisted hint from the preceding case.
  expect(() => assertIsolation({ ...evidence, helloSeeded: true })).not.toThrow();
});
it.effect("brackets a pending finalizer until cleanup actually completes", () =>
  Effect.gen(function* () {
    const events: string[] = [];
    const acquired = yield* Deferred.make<void>();
    const releasing = yield* Deferred.make<void>();
    const released = yield* Deferred.make<void>();
    const operation = Effect.acquireRelease(Deferred.succeed(acquired, undefined), () =>
      Deferred.succeed(releasing, undefined).pipe(
        Effect.andThen(Deferred.await(released)),
        Effect.andThen(Effect.sync(() => events.push("cleanup"))),
      ),
    );
    const fiber = yield* Effect.forkChild(
      Effect.scoped(observedScope(operation, (name) => events.push(name), "release")),
    );
    yield* Deferred.await(acquired);
    yield* Deferred.await(releasing);
    expect(events).toEqual(["release-start"]);
    yield* Deferred.succeed(released, undefined);
    yield* Fiber.join(fiber);
    expect(events).toEqual(["release-start", "cleanup", "release-end"]);
  }),
);
it.effect("records stream cancellation around its actual resource finalizer", () =>
  Effect.gen(function* () {
    const events: string[] = [];
    const subscribed = yield* Deferred.make<void>();
    const source = Stream.unwrap(
      Effect.acquireRelease(Deferred.succeed(subscribed, undefined), () =>
        Effect.sync(() => events.push("cleanup")),
      ).pipe(Effect.as(Stream.never)),
    );
    const fiber = yield* Effect.forkChild(
      Stream.runDrain(observedStream(source, (name) => events.push(name), "cancel")),
    );
    yield* Deferred.await(subscribed);
    yield* Fiber.interrupt(fiber);
    expect(events).toEqual(["cancel-start", "cleanup", "cancel-end"]);
  }),
);
describe("lane scheduling", () => {
  it("runs every variant exactly once per case on the same lane, balances seeds and variant order", () => {
    const cases = makeManifest().cases.filter(
      (c) =>
        c.profile.name === "loss-1" &&
        c.routes === 2 &&
        c.scheme === "https" &&
        c.workload === "idle",
    );
    const lanes = scheduleLanes(cases, ["a", "b", "c", "d"], 4);
    expect(lanes.map((lane) => lane.length)).toEqual([5, 5, 5, 5]);
    expect(
      lanes
        .flat()
        .map((c) => c.entry.id)
        .sort(),
    ).toEqual(cases.map((c) => c.id).sort());
    for (const task of lanes.flat())
      expect([...task.variants].sort()).toEqual(["a", "b", "c", "d"]);
    expect(lanes).toEqual(scheduleLanes(cases, ["a", "b", "c", "d"], 4));
    expect(
      ["a", "b", "c", "d"].map((v) => lanes.flat().filter((t) => t.variants[0] === v).length),
    ).toEqual([5, 5, 5, 5]);
    expect(() => scheduleLanes(cases, ["a"], 0)).toThrow("Lanes");
  });
});
it("distinguishes warm state, pre-active ordering, healthy probes, and cold delayed discovery", () => {
  const cases = makeLifecycleManifest().cases;
  const get = (workload: string) => workloadPlan(cases.find((c) => c.workload === workload)!);
  expect(get("warm-closed")).toMatchObject({
    establishOld: true,
    oldSocket: "closed",
    simulatedBackgroundMs: 180000,
    addedMessages: 20,
  });
  expect(get("warm-half-open")).toMatchObject({
    oldSocket: "half-open",
    wakeup: "application-active-reconnect",
  });
  expect(get("warm-healthy")).toMatchObject({ oldSocket: "healthy", addedMessages: 0 });
  expect(get("pre-active-retry")).toMatchObject({ preActive: true, establishOld: false });
  expect(get("healthy-slow")).toMatchObject({
    keep: true,
    establishOld: true,
    wakeup: "application-active-probe",
  });
  expect(get("refocus")).toMatchObject({ simulatedBackgroundMs: 0, keep: true });
  expect(get("loaded-host")).toMatchObject({ establishOld: false });
  expect(cases.filter((c) => c.workload === "loaded-host").map((c) => c.configDelayMs)).toEqual([
    300, 1500, 300, 1500,
  ]);
});

it("detects real resource reuse across case boundaries", () => {
  const register = makeResourceTracker();
  const pool = {};
  expect(register("pool", pool)).toBe("pool/1");
  expect(() => register("pool", pool)).toThrow("carried across cases");
  expect(register("pool", {})).toBe("pool/2");
});
