import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as EventSink from "../../src/orchestration-v2/EventSink.ts";
import * as EventStore from "../../src/orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../../src/orchestration-v2/ProjectionStore.ts";
import * as SqlitePersistence from "../../src/persistence/Sqlite.ts";
import * as OrchestrationEventStore from "../../src/persistence/OrchestrationEventStore.ts";
import { THREAD_RESUME_MAX_REPLAY_EVENTS } from "../../src/orchestration-v2/ThreadStream.ts";
import {
  buildBoundedThreadProjection,
  THREAD_HISTORY_PAGE_POLICY,
  THREAD_HISTORY_SNAPSHOT_ROW_LIMIT,
} from "../../src/orchestration-v2/threadHistoryPaging.ts";
import { EMPTY_ENVIRONMENT_THREAD_STATE } from "../../../../packages/client-runtime/src/state/threadState.ts";
import { operationStateError } from "./operationsWorkloads.ts";
import {
  operationMessageEvents,
  operationMessageText,
  operationNames,
  operationSettings,
  operationThreadCreated,
  operationThreadId,
  type OperationControlResponse,
} from "./operationsFixture.ts";
import { compareSuites, type SuiteResults, validateSuiteResults } from "./comparison.ts";
import { caseId, makeOperationSuitePlan, makeSuitePlan } from "./suite.ts";

const persistence = EventSink.layer.pipe(
  Layer.provideMerge(EventStore.layerFromOrchestrationEventStore),
  Layer.provideMerge(ProjectionStore.layer),
  Layer.provideMerge(OrchestrationEventStore.layer),
  Layer.provideMerge(SqlitePersistence.layerMemory),
);

it.effect(
  "verifies real persisted conversation state and rejects missing, duplicate, or corrupted data",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sink = yield* EventSink.EventSinkV2;
        const projections = yield* ProjectionStore.ProjectionStoreV2;
        yield* sink.write({ events: [operationThreadCreated, ...operationMessageEvents(0, 100)] });
        const snapshot = yield* projections.getThreadSnapshot(operationThreadId);
        const expected: OperationControlResponse = {
          startedAt: 0,
          sequence: snapshot.snapshotSequence,
          messages: 100,
          windowMessageIds: [],
          windowRunIds: [],
          hasMoreHistory: false,
          latestLocalTurnOrdinal: null,
        };
        const state = {
          ...EMPTY_ENVIRONMENT_THREAD_STATE,
          status: "live" as const,
          data: Option.some(snapshot.projection),
        };
        assert.isUndefined(operationStateError(state, expected, false));
        for (const messages of [
          snapshot.projection.messages.slice(1),
          [...snapshot.projection.messages.slice(0, -1), snapshot.projection.messages[0]!],
          snapshot.projection.messages.map((message, i) =>
            i === 99 ? { ...message, text: "corrupted" } : message,
          ),
        ]) {
          assert.isString(
            operationStateError(
              { ...state, data: Option.some({ ...snapshot.projection, messages }) },
              expected,
              false,
            ),
          );
        }
        assert.isString(
          operationStateError({ ...state, status: "synchronizing" }, expected, false),
        );
        const loaded = yield* projections.getThreadSnapshotWindow(operationThreadId, {
          rowLimit: THREAD_HISTORY_SNAPSHOT_ROW_LIMIT,
          userTurnLimit: THREAD_HISTORY_PAGE_POLICY.maxUserTurns,
        });
        const bounded = buildBoundedThreadProjection(loaded);
        assert.isAbove(bounded.projection.messages.length, 0);
        assert.isBelow(bounded.projection.messages.length, 100);
        assert.isTrue(bounded.hasMoreHistory);
        const boundedExpected = {
          ...expected,
          windowMessageIds: bounded.projection.messages.map((message) => message.id),
          windowRunIds: bounded.projection.runs.map((run) => run.id),
          hasMoreHistory: true,
          latestLocalTurnOrdinal: bounded.latestLocalTurnOrdinal,
        };
        assert.isUndefined(
          operationStateError(
            {
              ...state,
              data: Option.some(bounded.projection),
              history: {
                ...state.history,
                hasMoreHistory: true,
                historyCursor: bounded.historyCursor,
                latestLocalTurnOrdinal: bounded.latestLocalTurnOrdinal,
              },
            },
            boundedExpected,
            true,
          ),
        );
      }),
    ).pipe(Effect.provide(persistence)),
);

it("keeps the busy reconnect on replay and exercises snapshot fallback separately", () => {
  assert.isAtMost(
    operationMessageEvents(100, operationSettings.replayMessages).length,
    THREAD_RESUME_MAX_REPLAY_EVENTS,
  );
  assert.isAbove(
    operationMessageEvents(100, operationSettings.snapshotMessages).length,
    THREAD_RESUME_MAX_REPLAY_EVENTS,
  );
  assert.notEqual(operationMessageText(0), operationMessageText(1));
});

function operationResults(value = 100): SuiteResults {
  const plan = makeOperationSuitePlan();
  return {
    suite: { hash: plan.hash },
    complete: true,
    metadata: { revision: "test", environmentHash: "same-host" },
    rows: plan.cases.map((entry) => ({
      ...entry,
      scenario: entry.scenario.name,
      seed: entry.seed!,
      metrics: { operationMs: value, linkTxBytes: 10000, linkTxPackets: 100 },
    })),
  };
}

it("fixes every operation, network profile, transport, and repetition independently of the old suite", () => {
  const plan = makeOperationSuitePlan();
  assert.deepEqual(plan, makeOperationSuitePlan());
  assert.deepEqual(plan.workloads, operationNames);
  assert.equal(plan.cases.length, 648);
  assert.equal(new Set(plan.cases.map(caseId)).size, 648);
  assert.equal(
    makeSuitePlan().hash,
    "624d269925dfbfb4b098791d6c46dfe91abedf451f01d3852e61f50cf5fd16f6",
  );
  validateSuiteResults(operationResults());
  assert.throws(
    () => validateSuiteResults({ ...operationResults(), rows: operationResults().rows.slice(1) }),
    /incomplete/,
  );
  assert.throws(
    () =>
      validateSuiteResults({
        ...operationResults(),
        rows: [operationResults().rows[1]!, ...operationResults().rows.slice(1)],
      }),
    /duplicate/,
  );
  assert.throws(
    () =>
      compareSuites(operationResults(), {
        ...operationResults(),
        suite: { hash: makeSuitePlan().hash },
        rows: makeSuitePlan().cases.map((entry) => ({
          ...entry,
          scenario: entry.scenario.name,
          seed: entry.seed!,
          metrics: {},
        })),
      }),
    /configuration differs/,
  );
});

it("flags slower completion and extra transfer cost without letting an incomplete attempt win", () => {
  const candidate = operationResults(130);
  const comparisons = compareSuites(operationResults(), {
    ...candidate,
    rows: candidate.rows.map((row) => ({
      ...row,
      metrics: { ...row.metrics, linkTxBytes: 15000 },
    })),
  });
  assert.deepEqual(
    comparisons[0]!.metrics.map((metric) => metric.signal),
    ["regressed", "regressed", "inconclusive"],
  );
  const failures = compareSuites(operationResults(), {
    ...candidate,
    rows: candidate.rows.map((row) =>
      row.repetition === 1 ? { ...row, metrics: { ...row.metrics, outcome: "timeout" } } : row,
    ),
  });
  assert.equal(failures[0]!.completion, "regressed");
  assert.deepEqual(failures[0]!.metrics, []);
});
