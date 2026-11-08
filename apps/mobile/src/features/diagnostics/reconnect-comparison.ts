import * as Semaphore from "effect/Semaphore";
import { recordReconnectComparison } from "@t3tools/client-runtime/rpc";
import Constants from "expo-constants";
import * as DateTime from "effect/DateTime";
import {
  EnvironmentId,
  ReconnectComparisonRecord,
  type ReconnectComparisonAttempt,
  type ReconnectComparisonMode,
  type ReconnectComparisonPlan,
  OrchestrationV2ShellSnapshot,
  type ServerConfig,
} from "@t3tools/contracts";
import {
  ReconnectComparison as Comparison,
  EnvironmentRegistry,
  EnvironmentSupervisor,
} from "@t3tools/client-runtime/connection";
import { providerCatalogRevision } from "@t3tools/shared/reconnectSnapshots";
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
import * as Network from "expo-network";
import { randomUUID, digestStringAsync, CryptoDigestAlgorithm } from "expo-crypto";
import { AppState } from "react-native";
import * as MobileDatabase from "../../persistence/mobile-database";

export const BUILD_ID = "upstream-ade473-snapshot-v2-r1";
export const PROTOCOL_ID = "snapshot-reuse-v2-90-cellular-pairs";
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
const networkName = (state: Network.NetworkState): ReconnectComparisonAttempt["networkStart"] =>
  state.type === Network.NetworkStateType.CELLULAR
    ? "cellular"
    : state.type === Network.NetworkStateType.WIFI
      ? "wifi"
      : "other";
const activity = (): ReconnectComparisonAttempt["activityStart"] =>
  AppState.currentState === "active"
    ? "active"
    : AppState.currentState === "background"
      ? "background"
      : AppState.currentState === "inactive"
        ? "inactive"
        : "unknown";
const network = Effect.promise(() => Network.getNetworkStateAsync()).pipe(
  Effect.map(networkName),
  Effect.orElseSucceed(() => "other" as const),
);

interface Status {
  readonly mode: ReconnectComparisonMode;
  readonly running: boolean;
  readonly completed: number;
  readonly total: number;
  readonly pending: number;
  readonly runId: string | null;
}

class ComparisonUnavailable extends Schema.TaggedError<ComparisonUnavailable>()(
  "ComparisonUnavailable",
  { message: Schema.String },
) {}
const fingerprintConfig = (config: ServerConfig) =>
  digestStringAsync(
    CryptoDigestAlgorithm.SHA256,
    JSON.stringify(
      config.providers.map((provider) => [provider.instanceId, providerCatalogRevision(provider)]),
    ),
  );
const encodeShell = Schema.encodeSync(OrchestrationV2ShellSnapshot);
const fingerprintShell = (snapshot: OrchestrationV2ShellSnapshot) =>
  digestStringAsync(CryptoDigestAlgorithm.SHA256, JSON.stringify(encodeShell(snapshot)));

export const make = Effect.gen(function* () {
  const database = yield* MobileDatabase.MobileDatabase;
  const uploadLock = yield* Semaphore.make(1);
  const savedMode = yield* database
    .loadCache(storageEnvironment, "reconnect-comparison", "mode")
    .pipe(Effect.orElseSucceed(() => Option.none<string>()));
  let preferred: ReconnectComparisonMode =
    Option.getOrNull(savedMode) === "proposed" ? "proposed" : "baseline";
  let status: Status = {
    mode: preferred,
    running: false,
    completed: 0,
    total: 0,
    pending: 0,
    runId: null,
  };
  const listeners = new Set<() => void>();
  const update = (patch: Partial<Status>) => {
    status = { ...status, ...patch };
    for (const listener of listeners) listener();
  };
  let active: {
    environmentId: EnvironmentId;
    mode: ReconnectComparisonMode;
    observe: (origin: string) => Comparison.SessionObservation;
  } | null = null;
  const control = {
    mode: (environmentId: EnvironmentId) =>
      active?.environmentId === environmentId ? active.mode : preferred,
    observe: (environmentId: EnvironmentId, origin: string) =>
      active?.environmentId === environmentId ? active.observe(origin) : undefined,
  };
  const save = (record: ReconnectComparisonRecord) =>
    database.saveCache(
      record.environmentId,
      "reconnect-comparison",
      record.id,
      2,
      JSON.stringify(record),
    );
  const pending = Effect.fn("MobileReconnectComparison.pending")(function* (
    environmentId: EnvironmentId,
  ) {
    const stored = yield* database.listCache("reconnect-comparison");
    const records: Array<ReconnectComparisonRecord> = [];
    for (const json of stored) {
      // Device preferences share this cache kind but contain no record envelope.
      if (!json.startsWith("{")) continue;
      const record = yield* decodeRecord(json);
      if (record.environmentId === environmentId) records.push(record);
    }
    return records;
  });
  const upload = Effect.fn("MobileReconnectComparison.upload")(function* (
    environmentId: EnvironmentId,
    registry: EnvironmentRegistry.EnvironmentRegistry["Service"],
  ) {
    yield* registry.run(
      environmentId,
      Effect.gen(function* () {
        const supervisor = yield* EnvironmentSupervisor.EnvironmentSupervisor;
        const session = yield* SubscriptionRef.get(supervisor.session);
        if (Option.isNone(session)) return;
        const records = yield* pending(environmentId);
        update({ pending: records.length });
        for (let i = 0; i < records.length; i += 20) {
          const batch = records.slice(i, i + 20);
          yield* recordReconnectComparison(session.value, batch);
          for (const record of batch)
            yield* database.removeCache(environmentId, "reconnect-comparison", record.id);
          update({ pending: records.length - Math.min(i + 20, records.length) });
        }
      }),
    );
  }, uploadLock.withPermit);
  const waitDisconnected = (supervisor: EnvironmentSupervisor.EnvironmentSupervisor["Service"]) =>
    SubscriptionRef.changes(supervisor.state).pipe(
      Stream.filter((state) => state.phase === "available"),
      Stream.runHead,
      Effect.asVoid,
    );
  const choose = Effect.fn("MobileReconnectComparison.choose")(function* (
    input: {
      environmentId: EnvironmentId;
      mode: ReconnectComparisonMode;
    },
    registry: EnvironmentRegistry.EnvironmentRegistry["Service"],
  ) {
    if (status.running) return;
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
  const run = Effect.fn("MobileReconnectComparison.run")(function* (
    environmentId: EnvironmentId,
    registry: EnvironmentRegistry.EnvironmentRegistry["Service"],
  ) {
    if (status.running) return;
    const storedInstall = yield* database.loadCache(
      storageEnvironment,
      "reconnect-comparison",
      "install",
    );
    const installId = Option.getOrElse(storedInstall, randomUUID);
    yield* database.saveCache(storageEnvironment, "reconnect-comparison", "install", 2, installId);
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
          const session = yield* SubscriptionRef.get(supervisor.session);
          if (Option.isNone(session))
            return yield* new ComparisonUnavailable({
              message: "Connect to this environment before starting the comparison.",
            });
          yield* recordReconnectComparison(session.value, [savedPlan]);
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
              const networkStart = yield* network;
              const activityStart = activity();
              let backgrounded = activityStart !== "active";
              let networkChanged = false;
              const appListener = AppState.addEventListener("change", (state) => {
                if (state !== "active") backgrounded = true;
              });
              const networkListener = Network.addNetworkStateListener((state) => {
                if (networkName(state) !== networkStart) networkChanged = true;
              });
              const ready = yield* Deferred.make<{
                readonly at: number;
                readonly snapshot: OrchestrationV2ShellSnapshot;
              }>();
              const started = performance.now();
              let valid = true;
              let socketCount = 0;
              let configMs: number | null = null;
              let lastConfig: ServerConfig | null = null;
              let catalogRevisionsRequested = 0,
                catalogsReused = 0,
                projectRevisionsRequested = 0;
              const origins = new Set<string>(),
                serverVersions = new Set<string>();
              active = {
                environmentId,
                mode: assignment.mode,
                observe: (origin) => {
                  origins.add(origin);
                  return {
                    socket: () => {
                      if (valid) socketCount++;
                    },
                    catalogRequest: (count) => {
                      if (valid) catalogRevisionsRequested += count;
                    },
                    shellRequest: (hasRevision) => {
                      if (valid && hasRevision) projectRevisionsRequested++;
                    },
                    config: (config, reused) => {
                      if (!valid) return;
                      configMs ??= performance.now() - started;
                      lastConfig = config;
                      catalogsReused += reused;
                      serverVersions.add(config.environment.serverVersion);
                    },
                    synchronized: (snapshot) =>
                      Deferred.succeed(ready, { at: performance.now(), snapshot }).pipe(
                        Effect.asVoid,
                      ),
                  };
                },
              };
              const result = yield* restore(
                Effect.gen(function* () {
                  yield* supervisor.connect;
                  return yield* Deferred.await(ready).pipe(
                    Effect.timeoutOption(`${TIMEOUT_MS} millis`),
                  );
                }),
              ).pipe(Effect.exit);
              const ended = performance.now();
              valid = false;
              active = null;
              appListener.remove();
              networkListener.remove();
              const networkEnd = yield* network;
              const activityEnd = activity();
              const success =
                Exit.isSuccess(result) && Option.isSome(result.value) ? result.value.value : null;
              const config: ServerConfig | null = lastConfig;
              const catalogState =
                config === null ? null : yield* Effect.promise(() => fingerprintConfig(config));
              const shellState =
                success === null
                  ? null
                  : yield* Effect.promise(() => fingerprintShell(success.snapshot));
              const row: ReconnectComparisonAttempt = {
                version: 2,
                kind: "attempt",
                id: `${plan.runId}:${assignment.index}`,
                runId: plan.runId,
                protocolId: PROTOCOL_ID,
                installId,
                buildId: BUILD_ID,
                upstreamCommit: UPSTREAM_COMMIT,
                sourceCommit,
                appPackage,
                environmentId,
                ...assignment,
                at: DateTime.formatIso(yield* DateTime.now),
                outcome:
                  success !== null
                    ? "ready"
                    : Exit.isFailure(result)
                      ? Cause.hasInterruptsOnly(result.cause)
                        ? "interrupted"
                        : "failed"
                      : "timeout",
                elapsedMs: success === null ? ended - started : success.at - started,
                cappedReadyMs:
                  success === null ? TIMEOUT_MS : Math.min(TIMEOUT_MS, success.at - started),
                configMs,
                jsonDown: null,
                jsonUp: null,
                instrumentationMs: null,
                socketCount,
                origins: [...origins],
                serverVersions: [...serverVersions],
                catalogRevisionsRequested,
                catalogsReused,
                projectRevisionsRequested,
                catalogState,
                shellState,
                networkStart,
                networkEnd,
                networkChanged: networkChanged || networkStart !== networkEnd,
                activityStart,
                activityEnd,
                backgrounded: backgrounded || activityEnd !== "active",
              };
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
  });
  return {
    control,
    choose,
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
