import * as Semaphore from "effect/Semaphore";
import { recordReconnectComparison } from "@t3tools/client-runtime/rpc";
import Constants from "expo-constants";
import * as DateTime from "effect/DateTime";
import {
  EnvironmentId,
  ReconnectComparisonRecord,
  type ReconnectComparisonMode,
  type ReconnectComparisonPlan,
} from "@t3tools/contracts";
import {
  ReconnectComparison as Comparison,
  EnvironmentRegistry,
  EnvironmentSupervisor,
} from "@t3tools/client-runtime/connection";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Cause from "effect/Cause";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { randomUUID } from "expo-crypto";
import * as Queue from "effect/Queue";
import { makeObservation, network } from "./reconnect-observation";
import * as MobileDatabase from "../../persistence/mobile-database";

export const BUILD_ID = "upstream-ade473-snapshot-v3-r4";
export const PROTOCOL_ID = "snapshot-reuse-v3-90-cellular-pairs";
export const NATURAL_PROTOCOL_ID = "snapshot-reuse-v3-natural-reconnects";
export const UPSTREAM_COMMIT = "ade473018d32003658b7d38693e083ee2e314bd8";
const sourceCommit = process.env.EXPO_PUBLIC_RECONNECT_SOURCE ?? "development";
const appPackage =
  Constants.expoConfig?.android?.package ??
  Constants.expoConfig?.ios?.bundleIdentifier ??
  "unknown";
export const ROUNDS = 30;
const TIMEOUT_MS = 60_000;
const storageEnvironment = EnvironmentId.make("reconnect-comparison-device");
const decodeRecord = Schema.decodeUnknownEffect(Schema.fromJsonString(ReconnectComparisonRecord));

interface Status {
  readonly mode: ReconnectComparisonMode;
  readonly randomized: boolean;
  readonly running: boolean;
  readonly completed: number;
  readonly total: number;
  readonly pending: number;
  readonly recordingError: string | null;
  readonly runId: string | null;
}

class ComparisonUnavailable extends Schema.TaggedError<ComparisonUnavailable>()(
  "ComparisonUnavailable",
  { message: Schema.String },
) {}

export const make = Effect.gen(function* () {
  const database = yield* MobileDatabase.MobileDatabase;
  const uploadLock = yield* Semaphore.make(1);
  const savedMode = yield* database
    .loadCache(storageEnvironment, "reconnect-comparison", "mode")
    .pipe(Effect.orElseSucceed(() => Option.none<string>()));
  let randomized = yield* database
    .loadCache(storageEnvironment, "reconnect-comparison", "randomized")
    .pipe(
      Effect.map((saved) => Option.getOrElse(saved, () => "true") === "true"),
      Effect.orElseSucceed(() => true),
    );
  let preferred: ReconnectComparisonMode =
    Option.getOrNull(savedMode) === "proposed" ? "proposed" : "baseline";
  let status: Status = {
    mode: preferred,
    randomized,
    running: false,
    completed: 0,
    total: 0,
    pending: 0,
    recordingError: null,
    runId: null,
  };
  const listeners = new Set<() => void>();
  const update = (patch: Partial<Status>) => {
    status = { ...status, ...patch };
    for (const listener of listeners) listener();
  };
  let schedulingPaired = false;
  let preparing = 0;
  let uploadCancellation: Deferred.Deferred<void> | null = null;
  let active: {
    environmentId: EnvironmentId;
    mode: ReconnectComparisonMode;
    observe: (origin: string) => Comparison.SessionObservation;
    start: () => void;
  } | null = null;
  const scope = yield* Effect.scope;
  const recordsSaved = yield* Queue.unbounded<EnvironmentId>();
  type Capture = Effect.Success<ReturnType<typeof makeObservation>>;
  const events = new Map<
    EnvironmentId,
    {
      id: string;
      mode: ReconnectComparisonMode;
      ready: boolean;
      attempts: number;
    }
  >();
  const ordinary = new Map<
    EnvironmentId,
    {
      capture: Capture;
      ended: Deferred.Deferred<"failed" | "interrupted">;
      saved: Deferred.Deferred<void>;
    }
  >();
  const beginSession = (environmentId: EnvironmentId) => {
    if (!randomized || schedulingPaired || status.running) return;
    if (events.get(environmentId)?.ready === false) return;
    const id = randomUUID();
    events.set(environmentId, {
      id,
      mode: Number.parseInt(id.slice(0, 2), 16) < 128 ? "proposed" : "baseline",
      ready: false,
      attempts: 0,
    });
  };
  const recordingFailed = () =>
    update({ recordingError: "Could not save reconnect measurements." });
  const control = {
    beginSession,
    beginAttempt: Effect.fn("MobileReconnectComparison.beginAttempt")(
      function* (environmentId: EnvironmentId) {
        if (active?.environmentId === environmentId) {
          active.start();
          return;
        }
        if (!randomized || schedulingPaired || status.running) return;
        const event = events.get(environmentId);
        if (event === undefined) return;
        const attempt = event.attempts++;
        const runId = `${event.id}.${attempt}`;
        const assignment = {
          index: 0,
          round: attempt,
          position: 0 as const,
          mode: event.mode,
          warmup: false,
        };
        const plan: ReconnectComparisonPlan = {
          version: 2,
          kind: "plan",
          id: `${runId}:plan`,
          runId,
          protocolId: NATURAL_PROTOCOL_ID,
          installId: yield* getInstallId(),
          buildId: BUILD_ID,
          upstreamCommit: UPSTREAM_COMMIT,
          sourceCommit,
          appPackage,
          environmentId,
          at: DateTime.formatIso(yield* DateTime.now),
          timeoutMs: TIMEOUT_MS,
          network: yield* network,
          assignments: [assignment],
        };
        yield* save(plan);
        const capture = yield* makeObservation(plan, assignment);
        const ended = yield* Deferred.make<"failed" | "interrupted">();
        const saved = yield* Deferred.make<void>();
        const current = { capture, ended, saved };
        ordinary.set(environmentId, current);
        capture.start();
        yield* Effect.raceFirst(
          capture.ready.pipe(Effect.as("ready" as const)),
          Deferred.await(ended),
        ).pipe(
          Effect.timeoutOrElse({
            duration: `${TIMEOUT_MS} millis`,
            orElse: () => Effect.succeed("timeout" as const),
          }),
          Effect.flatMap((outcome) => capture.finish(outcome)),
          Effect.flatMap(save),
          Effect.catchCause(() => Effect.sync(recordingFailed)),
          Effect.ensuring(
            Effect.gen(function* () {
              if (ordinary.get(environmentId) === current) ordinary.delete(environmentId);
              yield* Deferred.succeed(saved, undefined);
              yield* Queue.offer(recordsSaved, environmentId);
            }),
          ),
          Effect.forkIn(scope),
        );
      },
      Effect.catchCause(() => Effect.sync(recordingFailed)),
      (effect) =>
        Effect.acquireUseRelease(
          Effect.sync(() => {
            preparing++;
          }),
          () =>
            Effect.gen(function* () {
              // A reconnect cancels diagnostic traffic before taking the measurement lock.
              if (uploadCancellation !== null)
                yield* Deferred.succeed(uploadCancellation, undefined);
              yield* effect.pipe(uploadLock.withPermit);
            }),
          () =>
            Effect.sync(() => {
              preparing--;
            }),
        ),
    ),
    finishAttempt: Effect.fn("MobileReconnectComparison.finishAttempt")(function* (
      environmentId: EnvironmentId,
      outcome: "failed" | "interrupted",
    ) {
      const current = ordinary.get(environmentId);
      if (!current) return;
      yield* Deferred.succeed(current.ended, outcome);
      yield* Deferred.await(current.saved);
    }),
    reset: (environmentId: EnvironmentId) => {
      events.delete(environmentId);
    },
    mode: (environmentId: EnvironmentId) =>
      active?.environmentId === environmentId
        ? active.mode
        : randomized
          ? (events.get(environmentId)?.mode ?? "baseline")
          : preferred,
    observe: (
      environmentId: EnvironmentId,
      origin: string,
    ): Comparison.SessionObservation | undefined => {
      if (active?.environmentId === environmentId) return active.observe(origin);
      const event = events.get(environmentId);
      if (!randomized || !event) return undefined;
      const capture = ordinary.get(environmentId)?.capture;
      const attempt = event.attempts;
      const observer = capture?.observe(origin);
      return {
        socket: observer?.socket ?? (() => {}),
        catalogRequest: observer?.catalogRequest ?? (() => {}),
        shellRequest: observer?.shellRequest ?? (() => {}),
        config: observer?.config ?? (() => {}),
        synchronized: (snapshot) =>
          Effect.suspend(() => {
            if (events.get(environmentId) !== event || event.attempts !== attempt)
              return Effect.void;
            event.ready = true;
            if (capture?.finished()) return Effect.void;
            return observer?.synchronized(snapshot) ?? Effect.void;
          }),
      };
    },
  };
  const save = (record: ReconnectComparisonRecord) =>
    database
      .saveCache(record.environmentId, "reconnect-comparison", record.id, 2, JSON.stringify(record))
      .pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            if (status.recordingError !== null) update({ recordingError: null });
          }),
        ),
      );
  const installLock = yield* Semaphore.make(1);
  let knownInstallId: string | null = null;
  const getInstallId = Effect.fn("MobileReconnectComparison.installId")(function* () {
    if (knownInstallId !== null) return knownInstallId;
    const stored = yield* database.loadCache(storageEnvironment, "reconnect-comparison", "install");
    const id = Option.getOrElse(stored, randomUUID);
    yield* database.saveCache(storageEnvironment, "reconnect-comparison", "install", 2, id);
    knownInstallId = id;
    return id;
  }, installLock.withPermit);
  const pending = Effect.fn("MobileReconnectComparison.pending")(function* (
    environmentId: EnvironmentId,
  ) {
    const stored = yield* database.listCache("reconnect-comparison");
    const records: Array<ReconnectComparisonRecord> = [];
    for (const json of stored) {
      // Device preferences share this cache kind but contain no record envelope.
      if (!json.startsWith("{")) continue;
      const record = yield* decodeRecord(json);
      records.push(record);
    }
    update({ pending: records.length });
    return records.filter((record) => record.environmentId === environmentId);
  });
  const upload = Effect.fn("MobileReconnectComparison.upload")(function* (
    environmentId: EnvironmentId,
    registry: EnvironmentRegistry.EnvironmentRegistry["Service"],
  ) {
    if (preparing > 0 || active !== null || ordinary.size > 0) return;
    const cancellation = yield* Deferred.make<void>();
    uploadCancellation = cancellation;
    yield* Effect.raceFirst(
      Effect.gen(function* () {
        const records = yield* pending(environmentId);
        yield* registry.run(
          environmentId,
          Effect.gen(function* () {
            const supervisor = yield* EnvironmentSupervisor.EnvironmentSupervisor;
            const session = yield* SubscriptionRef.get(supervisor.session);
            if (Option.isNone(session)) return;
            for (let i = 0; i < records.length; i += 20) {
              const batch = records.slice(i, i + 20);
              yield* recordReconnectComparison(session.value, batch);
              for (const record of batch)
                yield* database.removeCache(environmentId, "reconnect-comparison", record.id);
              update({ pending: Math.max(0, status.pending - batch.length) });
            }
          }),
        );
      }),
      Deferred.await(cancellation),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          uploadCancellation = null;
        }),
      ),
    );
  }, uploadLock.withPermit);
  const waitDisconnected = (supervisor: EnvironmentSupervisor.EnvironmentSupervisor["Service"]) =>
    SubscriptionRef.changes(supervisor.state).pipe(
      Stream.filter((state) => state.phase === "available"),
      Stream.runHead,
      Effect.asVoid,
    );
  const connectedSession = Effect.fn("MobileReconnectComparison.connectedSession")(function* (
    supervisor: EnvironmentSupervisor.EnvironmentSupervisor["Service"],
  ) {
    const current = yield* SubscriptionRef.get(supervisor.session);
    if (Option.isSome(current)) return current.value;
    yield* supervisor.connect;
    const available = yield* SubscriptionRef.changes(supervisor.session).pipe(
      Stream.filter(Option.isSome),
      Stream.map((session) => session.value),
      Stream.runHead,
      Effect.timeoutOrElse({
        duration: "60 seconds",
        orElse: () =>
          Effect.fail(
            new ComparisonUnavailable({
              message: `Could not connect to ${supervisor.target.label}. Check the connection and try again.`,
            }),
          ),
      }),
    );
    if (Option.isNone(available))
      return yield* new ComparisonUnavailable({ message: "The environment connection closed." });
    return available.value;
  });
  const choose = Effect.fn("MobileReconnectComparison.choose")(function* (
    input: {
      environmentId: EnvironmentId;
      mode: ReconnectComparisonMode;
    },
    registry: EnvironmentRegistry.EnvironmentRegistry["Service"],
  ) {
    if (status.running || schedulingPaired) return;
    yield* registry.run(
      input.environmentId,
      Effect.gen(function* () {
        const supervisor = yield* EnvironmentSupervisor.EnvironmentSupervisor;
        yield* supervisor.disconnect;
        yield* waitDisconnected(supervisor);
        yield* Effect.gen(function* () {
          yield* database.saveCache(
            storageEnvironment,
            "reconnect-comparison",
            "mode",
            2,
            input.mode,
          );
          preferred = input.mode;
          update({ mode: preferred });
        }).pipe(Effect.ensuring(supervisor.connect));
      }),
    );
  });
  const setRandomized = Effect.fn("MobileReconnectComparison.setRandomized")(function* (
    input: { environmentId: EnvironmentId; enabled: boolean },
    registry: EnvironmentRegistry.EnvironmentRegistry["Service"],
  ) {
    if (status.running || schedulingPaired) return;
    yield* registry.run(
      input.environmentId,
      Effect.gen(function* () {
        const supervisor = yield* EnvironmentSupervisor.EnvironmentSupervisor;
        yield* supervisor.disconnect;
        yield* waitDisconnected(supervisor);
        yield* database
          .saveCache(
            storageEnvironment,
            "reconnect-comparison",
            "randomized",
            2,
            String(input.enabled),
          )
          .pipe(
            Effect.tap(() =>
              Effect.sync(() => {
                randomized = input.enabled;
                update({ randomized });
              }),
            ),
            Effect.ensuring(supervisor.connect),
          );
      }),
    );
  });
  const run = Effect.fn("MobileReconnectComparison.run")(function* (
    environmentId: EnvironmentId,
    registry: EnvironmentRegistry.EnvironmentRegistry["Service"],
  ) {
    if (status.running || schedulingPaired) return;
    schedulingPaired = true;
    yield* Effect.gen(function* () {
      yield* registry.run(
        environmentId,
        Effect.flatMap(EnvironmentSupervisor.EnvironmentSupervisor, connectedSession),
      );
      yield* Effect.forEach([...ordinary.values()], (current) => Deferred.await(current.saved), {
        discard: true,
      });
      yield* upload(environmentId, registry);
      const installId = yield* getInstallId();
      const plan: ReconnectComparisonPlan = {
        kind: "plan",
        version: 2,
        protocolId: PROTOCOL_ID,
        id: "",
        runId: randomUUID(),
        installId,
        buildId: BUILD_ID,
        upstreamCommit: UPSTREAM_COMMIT,
        sourceCommit,
        appPackage,
        environmentId,
        at: DateTime.formatIso(yield* DateTime.now),
        timeoutMs: TIMEOUT_MS,
        network: yield* network,
        assignments: Comparison.makeComparisonAssignments(
          ROUNDS,
          () => Number.parseInt(randomUUID().slice(0, 2), 16) < 128,
        ),
      };
      const savedPlan = { ...plan, id: `${plan.runId}:plan` };
      update({ running: true, completed: 0, total: plan.assignments.length, runId: plan.runId });
      yield* Effect.gen(function* () {
        yield* save(savedPlan);
        // Require the destination to accept the plan before any timed attempts begin.
        yield* registry.run(
          environmentId,
          Effect.gen(function* () {
            const supervisor = yield* EnvironmentSupervisor.EnvironmentSupervisor;
            const session = yield* connectedSession(supervisor);
            yield* recordReconnectComparison(session, [savedPlan]);
          }),
        );
        yield* upload(environmentId, registry);
        for (const assignment of plan.assignments) {
          yield* registry.run(
            environmentId,
            Effect.uninterruptibleMask((restore) =>
              Effect.gen(function* () {
                const supervisor = yield* EnvironmentSupervisor.EnvironmentSupervisor;
                yield* supervisor.disconnect;
                yield* waitDisconnected(supervisor);
                const capture = yield* makeObservation(plan, assignment);
                active = {
                  environmentId,
                  mode: assignment.mode,
                  observe: capture.observe,
                  start: capture.start,
                };
                const result = yield* restore(
                  supervisor.connect.pipe(
                    Effect.andThen(capture.ready),
                    Effect.timeoutOption(`${TIMEOUT_MS} millis`),
                  ),
                ).pipe(Effect.exit);
                active = null;
                const outcome = Exit.isSuccess(result)
                  ? Option.isSome(result.value)
                    ? "ready"
                    : "timeout"
                  : Cause.hasInterruptsOnly(result.cause)
                    ? "interrupted"
                    : "failed";
                const row = yield* capture.finish(outcome);
                yield* save(row).pipe(Effect.uninterruptible);
                update({ completed: assignment.index + 1 });
                if (Exit.isFailure(result) && Cause.hasInterruptsOnly(result.cause))
                  return yield* Effect.interrupt;
              }),
            ),
          );
          if (assignment.position === 1) yield* upload(environmentId, registry).pipe(Effect.ignore);
        }
      }).pipe(
        Effect.ensuring(
          Effect.gen(function* () {
            active = null;
            schedulingPaired = false;
            update({ running: false, mode: preferred });
            yield* registry
              .run(
                environmentId,
                Effect.gen(function* () {
                  const supervisor = yield* EnvironmentSupervisor.EnvironmentSupervisor;
                  yield* supervisor.disconnect;
                  yield* waitDisconnected(supervisor);
                  yield* supervisor.connect;
                }),
              )
              .pipe(Effect.ignore);
            yield* upload(environmentId, registry).pipe(Effect.ignore);
          }),
        ),
      );
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          schedulingPaired = false;
        }),
      ),
    );
  });
  return {
    control,
    recordsSaved,
    isMeasuring: () => preparing > 0 || schedulingPaired || status.running || ordinary.size > 0,
    choose,
    setRandomized,
    run,
    upload,
    read: () => status,
    subscribe: (listener: () => void) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
  };
});
export class MobileReconnectComparison extends Context.Service<
  MobileReconnectComparison,
  Effect.Success<typeof make>
>()("@t3tools/mobile/features/diagnostics/reconnect-comparison/MobileReconnectComparison") {}
export const layer = Layer.effect(MobileReconnectComparison, make).pipe(Layer.orDie);
