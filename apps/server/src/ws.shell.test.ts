import { describe, expect, it } from "@effect/vitest";
import { EventId, ProjectId, type OrchestrationV2ShellStreamItem } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as SqlClient from "effect/sql/SqlClient";
import * as Stream from "effect/Stream";

import * as ProjectStore from "./orchestration-v2/ProjectStore.ts";
import { ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION } from "./orchestration-v2/ProjectionStore.ts";
import * as ThreadManagementService from "./orchestration-v2/ThreadManagementService.ts";
import * as OrchestrationEventStore from "./persistence/OrchestrationEventStore.ts";
import * as SqlitePersistence from "./persistence/Sqlite.ts";
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

const ReplayTestLayer = Layer.mergeAll(OrchestrationEventStore.layer, ProjectStore.layer).pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
);

// Keep append, projection storage, raw paging, and replay-to-live recovery real.
// Only the project-shell read is parked, like a busy projection service.
const makeParkedReplay = Effect.fnUntraced(function* (unknownRows = 0) {
  const store = yield* OrchestrationEventStore.OrchestrationEventStore;
  const projects = yield* ProjectStore.ProjectStoreV2;
  const sql = yield* SqlClient.SqlClient;
  const changes = yield* Effect.acquireRelease(
    PubSub.unbounded<ProjectEnrichmentService.ProjectEnrichmentChange>(),
    PubSub.shutdown,
  );
  const firstProjected = yield* Deferred.make<void>();
  const projectionParked = yield* Deferred.make<void>();
  const releaseProjection = yield* Deferred.make<void, ProjectService.ProjectOperationError>();
  const projectionClosed = yield* Deferred.make<void>();
  const replayClosed = yield* Deferred.make<void>();
  const enrichmentClosed = yield* Deferred.make<void>();
  const laterProject = {
    ...project,
    id: ProjectId.make("project-b"),
    workspaceRoot: "/workspace/b",
  };
  const commit = Effect.fnUntraced(function* (
    event: OrchestrationEventStore.UnsequencedProjectEvent,
  ) {
    const stored = yield* sql.withTransaction(
      Effect.gen(function* () {
        const stored = yield* store.appendProjectEvent(event);
        yield* projects.apply(stored);
        return stored;
      }),
    );
    yield* store.publishCommitted([stored]);
    return stored;
  });
  const eventFields = {
    aggregateKind: "project" as const,
    occurredAt: project.updatedAt,
    commandId: null,
    causationEventId: null,
    correlationId: null,
    metadata: {},
  };
  for (const shell of [project, laterProject]) {
    yield* commit({
      ...eventFields,
      eventId: EventId.make(`created:${shell.id}`),
      aggregateId: shell.id,
      type: "project.created",
      payload: { ...shell, projectId: shell.id },
    });
  }
  const afterSequence = yield* store.latestApplicationSequence;
  // Newer-build rows cannot go through append. An entire unknown-only raw page
  // must not end replay before the known rows on the next page.
  if (unknownRows > 0) {
    yield* sql`
      WITH RECURSIVE history(n) AS (
        SELECT 1 UNION ALL SELECT n + 1 FROM history WHERE n < ${unknownRows}
      )
      INSERT INTO orchestration_events (
        event_id, aggregate_kind, stream_id, stream_version, event_type,
        occurred_at, actor_kind, payload_json, metadata_json, application_event_version
      )
      SELECT 'unknown:' || n, 'project', 'future-project', n, 'future.event',
        ${project.updatedAt}, 'server', 'not JSON', '{}', 2 FROM history
    `;
  }
  const replayEvents = [];
  for (const shell of [project, laterProject]) {
    replayEvents.push(
      yield* commit({
        ...eventFields,
        eventId: EventId.make(`updated:${shell.id}`),
        aggregateId: shell.id,
        type: "project.meta-updated",
        payload: { projectId: shell.id, title: `Updated ${shell.id}`, updatedAt: shell.updatedAt },
      }),
    );
  }
  yield* sql`
    INSERT INTO orchestration_events (
      event_id, aggregate_kind, stream_id, stream_version, event_type,
      occurred_at, actor_kind, payload_json, metadata_json, application_event_version
    ) VALUES (
      'unknown:tail', 'project', 'future-project', ${unknownRows + 1}, 'future.trailing',
      ${project.updatedAt}, 'server', 'not JSON', '{}', 2
    )
  `;
  const highWater = yield* store.latestApplicationSequence;
  expect(highWater - afterSequence).toBe(unknownRows + 3);
  const shells = yield* projects.listShells();
  const liveAfterSequences: Array<number | undefined> = [];
  const dependencies = Layer.mergeAll(
    Layer.succeed(OrchestrationEventStore.OrchestrationEventStore, {
      ...store,
      readApplicationEvents: (input) =>
        store
          .readApplicationEvents(input)
          .pipe(Stream.ensuring(Deferred.succeed(replayClosed, undefined))),
      streamProjectedApplicationEvents: (input) =>
        Stream.suspend(() => {
          liveAfterSequences.push(input.afterSequence);
          return store.streamProjectedApplicationEvents(input);
        }),
    }),
    Layer.mock(ThreadManagementService.ThreadManagementService)({}),
    Layer.mock(ProjectService.ProjectService)({
      getShell: (projectId) =>
        Effect.gen(function* () {
          if (projectId === laterProject.id) {
            yield* Deferred.await(firstProjected);
            yield* Deferred.succeed(projectionParked, undefined);
            yield* Deferred.await(releaseProjection).pipe(
              Effect.ensuring(Deferred.succeed(projectionClosed, undefined)),
            );
          }
          const shell = yield* projects.getShell(projectId).pipe(
            Effect.mapError(
              (cause) =>
                new ProjectService.ProjectOperationError({
                  operation: "read-project",
                  projectId,
                  cause,
                }),
            ),
          );
          if (projectId === project.id) yield* Deferred.succeed(firstProjected, undefined);
          return shell;
        }),
    }),
    Layer.mock(ProjectEnrichmentService.ProjectEnrichmentService)({
      subscribeChanges: Effect.gen(function* () {
        yield* Effect.addFinalizer(() => Deferred.succeed(enrichmentClosed, undefined));
        return yield* PubSub.subscribe(changes);
      }),
      getAvailable: () =>
        Effect.succeed({
          repositoryIdentity: null,
          faviconPath: null,
          repositoryIdentityResolved: true,
        }),
    }),
  );
  const remove = (shell: typeof project) =>
    commit({
      ...eventFields,
      eventId: EventId.make(`deleted:${shell.id}`),
      aggregateId: shell.id,
      type: "project.deleted",
      payload: { projectId: shell.id, deletedAt: shell.updatedAt },
    });
  return {
    stream: Stream.unwrap(
      subscribeOrchestrationV2Shell({ afterSequence, requestCompletionMarker: true }),
    ).pipe(Stream.provide(dependencies)),
    highWater,
    shells,
    replayEvents,
    changes,
    laterProject,
    remove,
    projectionParked,
    releaseProjection,
    projectionClosed,
    replayClosed,
    enrichmentClosed,
    liveAfterSequences,
  };
});

describe("shell eager replay lifecycle", () => {
  it.live.each([0, 500])(
    "keeps racing live and enrichment updates after replay with %s unknown rows",
    (unknownRows) =>
      Effect.gen(function* () {
        const fixture = yield* makeParkedReplay(unknownRows);
        const initialSent = yield* Deferred.make<void>();
        const liveSent = yield* Deferred.make<void>();
        const enrichmentSent = yield* Deferred.make<void>();
        const sentinelSent = yield* Deferred.make<void>();
        const chunks: Array<ReadonlyArray<OrchestrationV2ShellStreamItem>> = [];
        const consumer = yield* fixture.stream.pipe(
          Stream.runForEachArray((items) =>
            Effect.gen(function* () {
              chunks.push([...items]);
              for (const item of items) {
                if (item.kind === "synchronized") yield* Deferred.succeed(initialSent, undefined);
                if (item.kind === "project.removed") {
                  yield* Deferred.succeed(
                    item.projectId === project.id ? liveSent : sentinelSent,
                    undefined,
                  );
                }
                if (
                  item.kind === "snapshot" &&
                  item.snapshot.projects.some((shell) => shell.repositoryIdentity !== null)
                ) {
                  yield* Deferred.succeed(enrichmentSent, undefined);
                }
              }
            }),
          ),
          Effect.scoped,
          Effect.forkChild,
        );
        yield* Deferred.await(fixture.projectionParked);
        const newer = yield* fixture.remove(project);
        const identity = {
          canonicalKey: "github.com/test/replay",
          locator: {
            source: "git-remote" as const,
            remoteName: "origin",
            remoteUrl: "https://github.com/test/replay.git",
          },
        };
        yield* PubSub.publish(fixture.changes, {
          workspaceRoot: fixture.laterProject.workspaceRoot,
          repositoryIdentityResolved: true,
          enrichment: {
            repositoryIdentity: identity,
            faviconPath: null,
            repositoryIdentityResolved: true,
          },
        });
        expect(chunks).toEqual([]);
        expect(fixture.liveAfterSequences).toEqual([]);
        yield* Deferred.succeed(fixture.releaseProjection, undefined);
        yield* Deferred.await(initialSent);
        yield* Deferred.await(liveSent);
        yield* Deferred.await(enrichmentSent);
        // A later committed live event fences delivery before checking exact-once
        // recovery of the event published before the live subscription existed.
        const sentinel = yield* fixture.remove(fixture.laterProject);
        yield* Deferred.await(sentinelSent);
        yield* Fiber.interrupt(consumer);
        yield* Deferred.await(fixture.projectionClosed);
        yield* Deferred.await(fixture.replayClosed);
        yield* Deferred.await(fixture.enrichmentClosed);

        expect(fixture.liveAfterSequences).toEqual([fixture.highWater]);
        expect(chunks[0]).toEqual([
          {
            kind: "snapshot",
            snapshot: {
              schemaVersion: ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION,
              snapshotSequence: fixture.highWater,
              projects: fixture.shells,
              threads: [],
              archivedThreads: [],
            },
            resolvedRepositoryIdentityRoots: fixture.shells.map((shell) => shell.workspaceRoot),
          },
          ...fixture.replayEvents.map((event, index) => ({
            kind: "project.updated",
            sequence: event.sequence,
            project: fixture.shells[index],
          })),
          { kind: "synchronized" },
        ]);
        const tail = chunks.slice(1).flat();
        expect(tail.filter((item) => item.kind !== "snapshot")).toEqual([
          { kind: "project.removed", sequence: newer.sequence, projectId: project.id },
          {
            kind: "project.removed",
            sequence: sentinel.sequence,
            projectId: fixture.laterProject.id,
          },
        ]);
        expect(tail.filter((item) => item.kind === "snapshot")).toEqual([
          {
            kind: "snapshot",
            snapshot: {
              schemaVersion: ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION,
              snapshotSequence: newer.sequence,
              projects: [{ ...fixture.shells[1], repositoryIdentity: identity }],
              threads: [],
              archivedThreads: [],
            },
            resolvedRepositoryIdentityRoots: [fixture.laterProject.workspaceRoot],
          },
        ]);
      }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
  );

  it.effect.each(["failure", "interruption"] as const)(
    "cleans up parked replay on %s without completion or live acquisition",
    (mode) =>
      Effect.gen(function* () {
        const fixture = yield* makeParkedReplay();
        const items: Array<OrchestrationV2ShellStreamItem> = [];
        const consumer = yield* fixture.stream.pipe(
          Stream.runForEach((item) => Effect.sync(() => items.push(item))),
          Effect.scoped,
          Effect.forkChild,
        );
        yield* Deferred.await(fixture.projectionParked);
        expect(items).toEqual([]);
        expect(fixture.liveAfterSequences).toEqual([]);
        const failure = new ProjectService.ProjectOperationError({
          operation: "read-project",
          projectId: fixture.laterProject.id,
          cause: new Error("projection unavailable"),
        });
        if (mode === "failure") {
          yield* Deferred.fail(fixture.releaseProjection, failure);
        } else {
          yield* Fiber.interrupt(consumer);
        }
        const exit = yield* Fiber.await(consumer);
        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isFailure(exit)) {
          if (mode === "failure") {
            expect(Option.getOrNull(Cause.findErrorOption(exit.cause))).toMatchObject({
              _tag: "OrchestrationV2GetShellSnapshotError",
              message: "Failed to prepare the application shell stream",
              cause: failure,
            });
          } else {
            expect(Cause.hasInterruptsOnly(exit.cause)).toBe(true);
          }
        }
        yield* Deferred.await(fixture.projectionClosed);
        yield* Deferred.await(fixture.replayClosed);
        yield* Deferred.await(fixture.enrichmentClosed);
        expect(items).toEqual([]);
        expect(fixture.liveAfterSequences).toEqual([]);
      }).pipe(Effect.scoped, Effect.provide(ReplayTestLayer)),
  );
});
