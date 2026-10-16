import { describe, expect, it } from "@effect/vitest";
import { EventId, ProjectId, type OrchestrationV2ShellStreamItem } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";

import * as ProjectStore from "./orchestration-v2/ProjectStore.ts";
import { ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION } from "./orchestration-v2/ProjectionStore.ts";
import * as ThreadManagementService from "./orchestration-v2/ThreadManagementService.ts";
import * as OrchestrationEventStore from "./persistence/OrchestrationEventStore.ts";
import * as ProjectEnrichmentService from "./project/ProjectEnrichmentService.ts";
import * as ProjectService from "./project/ProjectService.ts";
import { subscribeOrchestrationV2Shell } from "./ws.ts";

const project = {
  id: ProjectId.make("project-a"),
  title: "A project",
  workspaceRoot: "/workspace/a",
  repositoryIdentity: null,
  defaultModelSelection: null,
  scripts: [],
  createdAt: "2026-09-01T00:00:00Z",
  updatedAt: "2026-09-01T00:00:00Z",
};

const modes = [
  "snapshot",
  "fallback",
  "idle",
  "replay",
  "paged-replay",
  "cold-snapshot",
  "cold-idle",
  "cold-replay",
] as const;
const cases = modes.flatMap((mode) =>
  [true, false, undefined].map((requestCompletionMarker) => ({ mode, requestCompletionMarker })),
);

describe("shell subscription initial RPC chunks", () => {
  it.effect.each(cases)(
    "$mode, completion marker $requestCompletionMarker",
    ({ mode, requestCompletionMarker }) =>
      Effect.gen(function* () {
        const changes = yield* Effect.acquireRelease(
          PubSub.unbounded<ProjectEnrichmentService.ProjectEnrichmentChange>(),
          PubSub.shutdown,
        );
        const snapshot = mode === "snapshot" || mode === "cold-snapshot" || mode === "fallback";
        const resolved = !mode.startsWith("cold-");
        const replayCount =
          mode === "paged-replay" ? 513 : mode === "replay" || mode === "cold-replay" ? 2 : 0;
        const highWater = mode === "fallback" ? 2_000 : replayCount;
        const removed = (sequence: number) => ({
          sequence,
          aggregateKind: "project" as const,
          aggregateId: ProjectId.make(`removed-${sequence}`),
          eventId: EventId.make(`event-${sequence}`),
          type: "project.deleted" as const,
          payload: {
            projectId: ProjectId.make(`removed-${sequence}`),
            deletedAt: "2026-09-01T00:00:00Z",
          },
          occurredAt: "2026-09-01T00:00:00Z",
          commandId: null,
          causationEventId: null,
          correlationId: null,
          metadata: {},
        });
        const liveEvents = [removed(highWater + 1), removed(highWater + 2)];
        const dependencies = Layer.mergeAll(
          NodeSqliteClient.layer({ filename: ":memory:" }),
          Layer.mock(ThreadManagementService.ThreadManagementService)({
            readShellSnapshot: () =>
              Effect.succeed(
                Effect.succeed({
                  schemaVersion: ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION,
                  snapshotSequence: highWater,
                  threads: [],
                  archivedThreads: [],
                }),
              ),
          }),
          Layer.mock(ProjectStore.ProjectStoreV2)({ listShells: () => Effect.succeed([project]) }),
          Layer.mock(ProjectService.ProjectService)({}),
          Layer.mock(ProjectEnrichmentService.ProjectEnrichmentService)({
            subscribeChanges: PubSub.subscribe(changes),
            getAvailable: () =>
              Effect.succeed({
                repositoryIdentity: null,
                faviconPath: null,
                repositoryIdentityResolved: resolved,
              }),
          }),
          Layer.mock(OrchestrationEventStore.OrchestrationEventStore)({
            latestApplicationSequence: Effect.succeed(highWater),
            getReplayStats: () => Effect.succeed({ eventCount: replayCount, rawPayloadBytes: 0 }),
            readApplicationEvents: (input) => {
              expect(input).toEqual({
                afterSequence: 0,
                throughSequence: highWater,
                skipUnknownEventTypes: true,
              });
              return Stream.fromIterable(
                Array.from({ length: replayCount }, (_, i) => removed(i + 1)),
              );
            },
            streamProjectedApplicationEvents: (input) => {
              expect(input.afterSequence).toBe(highWater);
              return Stream.fromIterable(liveEvents.map(input.project));
            },
          }),
        );
        const snapshotItem = {
          kind: "snapshot" as const,
          snapshot: {
            schemaVersion: ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION,
            snapshotSequence: highWater,
            projects: [project],
            threads: [],
            archivedThreads: [],
          },
        };
        const prefix = [
          ...(snapshot ? [snapshotItem] : []),
          ...(resolved
            ? [{ ...snapshotItem, resolvedRepositoryIdentityRoots: [project.workspaceRoot] }]
            : []),
        ];
        const chunks: Array<ReadonlyArray<OrchestrationV2ShellStreamItem>> = [];
        yield* Stream.unwrap(
          subscribeOrchestrationV2Shell({
            ...(mode === "snapshot" || mode === "cold-snapshot" ? {} : { afterSequence: 0 }),
            ...(requestCompletionMarker === undefined ? {} : { requestCompletionMarker }),
          }),
        ).pipe(
          Stream.take(prefix.length + replayCount + (requestCompletionMarker ? 1 : 0) + 2),
          Stream.runForEachArray((items) => Effect.sync(() => chunks.push([...items]))),
          Effect.provide(dependencies),
        );

        const delta = (sequence: number) => ({
          kind: "project.removed",
          sequence,
          projectId: ProjectId.make(`removed-${sequence}`),
        });
        const replay = Array.from({ length: replayCount }, (_, i) => delta(i + 1));
        const live = liveEvents.map((event) => delta(event.sequence));
        const completion = requestCompletionMarker ? [{ kind: "synchronized" }] : [];
        expect(chunks.flat()).toEqual([...prefix, ...replay, ...completion, ...live]);
        if (requestCompletionMarker) {
          expect(chunks).toEqual([[...prefix, ...replay, ...completion], live]);
        } else {
          expect(chunks).toEqual([
            ...(prefix.length === 0 ? [] : [prefix]),
            ...Array.from({ length: Math.ceil(replayCount / 512) }, (_, i) =>
              replay.slice(i * 512, (i + 1) * 512),
            ),
            live,
          ]);
        }
      }),
  );
});
