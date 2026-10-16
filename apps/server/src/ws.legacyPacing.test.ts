import { describe, expect, it } from "@effect/vitest";
import { ThreadId, type OrchestrationV2ThreadStreamItem } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { idleThreadProjection, liveThreadShell } from "./mcp/McpToolAccess.testkit.ts";
import { subscribeOrchestrationV2Thread } from "./AuthenticatedRpcSession.ts";
import * as Threads from "./orchestration-v2/ThreadManagementService.ts";
import * as Events from "./persistence/OrchestrationEventStore.ts";

const threadId = ThreadId.make("legacy-pacing");
const projection = idleThreadProjection(liveThreadShell(threadId, { activeRunId: null }));

// This is the wire contract used by servers before the optional pacing flag.
const oldInput = Schema.Struct({
  threadId: ThreadId,
  afterSequence: Schema.optionalKey(Schema.Number),
  requestCompletionMarker: Schema.optionalKey(Schema.Boolean),
});

it("older decoders strip the pacing flag and keep the marker request", () => {
  expect(
    Schema.decodeUnknownSync(oldInput)({
      threadId,
      afterSequence: 1,
      requestCompletionMarker: true,
      legacyCatchUpPacing: true,
    }),
  ).toEqual({ threadId, afterSequence: 1, requestCompletionMarker: true });
});

describe("thread catch-up pacing", () => {
  const cases = (["snapshot", "fallback", "idle", "replay"] as const).flatMap((mode) =>
    [true, false, undefined].flatMap((legacyCatchUpPacing) =>
      [true, false].map((requestCompletionMarker) => ({
        mode,
        legacyCatchUpPacing,
        requestCompletionMarker,
      })),
    ),
  );
  it.effect.each(cases)(
    "$mode, legacy $legacyCatchUpPacing, marker $requestCompletionMarker",
    ({ mode, legacyCatchUpPacing, requestCompletionMarker }) =>
      Effect.gen(function* () {
        const replayCount = mode === "replay" ? 2 : 0;
        const dependencies = Layer.mergeAll(
          Layer.mock(Threads.ThreadManagementService)({
            ensureLegacyTranscript: () => Effect.void,
            getThreadSnapshot: () =>
              Effect.succeed({ schemaVersion: 1, snapshotSequence: 2, projection }),
            streamStoredEventsFrom: () => Stream.never,
          }),
          Layer.mock(Events.OrchestrationEventStore)({
            latestAgentSequence: () => Effect.succeed(2),
            getAgentReplayStats: () =>
              Effect.succeed({
                eventCount: replayCount,
                rawPayloadBytes: 0,
                hasCreateEvent: false,
              }),
            readAgentEvents: () =>
              Stream.fromIterable(
                Array.from(
                  { length: replayCount },
                  (_, i) =>
                    ({
                      sequence: i + 1,
                      event: { type: "message.updated", payload: {} },
                    }) as never,
                ),
              ),
          }),
        );
        const chunks: Array<ReadonlyArray<string>> = [];
        const count =
          (mode === "snapshot" || mode === "fallback" ? 1 : replayCount) +
          (requestCompletionMarker ? 1 : 0);
        yield* Stream.unwrap(
          subscribeOrchestrationV2Thread({
            threadId,
            ...(mode === "snapshot" ? {} : { afterSequence: mode === "fallback" ? 3 : 0 }),
            requestCompletionMarker,
            ...(legacyCatchUpPacing === undefined ? {} : { legacyCatchUpPacing }),
          }),
        ).pipe(
          Stream.take(count),
          Stream.runForEachArray((items: ReadonlyArray<OrchestrationV2ThreadStreamItem>) =>
            Effect.sync(() => chunks.push(items.map((item) => item.kind))),
          ),
          Effect.provide(dependencies),
        );
        const data =
          mode === "snapshot" || mode === "fallback"
            ? ["snapshot"]
            : Array.from({ length: replayCount }, () => "event");
        const marker = requestCompletionMarker ? ["synchronized"] : [];
        // Before M: one replay/snapshot chunk followed by a separate marker.
        expect(chunks).toEqual(
          legacyCatchUpPacing && requestCompletionMarker
            ? [...(data.length ? [data] : []), marker]
            : count
              ? [[...data, ...marker]]
              : [],
        );
      }),
  );
});
