// @effect-diagnostics nodeBuiltinImport:off - Per-fixture identity must survive worker reuse and differ after server restarts.
import * as NodeCrypto from "node:crypto";
import type { ServerLifecycleStreamEvent } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as ServerLifecycleEvents from "../../src/serverLifecycleEvents.ts";

/** Publish once per fixture, never once per subscribing session. */
export const layer = (
  environment: Extract<ServerLifecycleStreamEvent, { type: "ready" }>["payload"]["environment"],
) =>
  Layer.effectDiscard(
    Effect.gen(function* () {
      const lifecycle = yield* ServerLifecycleEvents.ServerLifecycleEvents;
      // A randomized millisecond within a valid ISO date prevents equal incarnation
      // proofs when independent fixtures start in the same wall-clock millisecond.
      const at = DateTime.formatIso(
        DateTime.makeUnsafe(
          1767225600000 + (NodeCrypto.randomBytes(6).readUIntBE(0, 6) % 3153600000000),
        ),
      );
      yield* lifecycle.publish({ version: 1, type: "ready", payload: { at, environment } });
    }),
  ).pipe(Layer.provideMerge(ServerLifecycleEvents.layer));

/** Match the production handler's subscribe-before-snapshot race and scoped cancellation. */
export const subscribe = Effect.gen(function* () {
  const lifecycle = yield* ServerLifecycleEvents.ServerLifecycleEvents;
  return Stream.unwrap(
    Effect.gen(function* () {
      const liveBuffer = yield* Queue.unbounded<ServerLifecycleStreamEvent>();
      yield* Effect.forkScoped(
        lifecycle.stream.pipe(Stream.runForEach((event) => Queue.offer(liveBuffer, event))),
        { startImmediately: true },
      );
      const snapshot = yield* lifecycle.snapshot;
      return Stream.concat(
        Stream.fromIterable([...snapshot.events].sort((a, b) => a.sequence - b.sequence)),
        Stream.fromQueue(liveBuffer).pipe(
          Stream.filter((event) => event.sequence > snapshot.sequence),
        ),
      );
    }),
  );
});
