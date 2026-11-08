import type { ReconnectComparison as Comparison } from "@t3tools/client-runtime/connection";
import {
  OrchestrationV2ShellSnapshot,
  type ReconnectComparisonAssignment,
  type ReconnectComparisonAttempt,
  type ReconnectComparisonPlan,
  type ServerConfig,
} from "@t3tools/contracts";
import { providerCatalogRevision } from "@t3tools/shared/reconnectSnapshots";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { CryptoDigestAlgorithm, digestStringAsync } from "expo-crypto";
import * as Network from "expo-network";
import { AppState } from "react-native";

const networkName = (state: Network.NetworkState): ReconnectComparisonAttempt["networkStart"] =>
  state.type === Network.NetworkStateType.CELLULAR
    ? "cellular"
    : state.type === Network.NetworkStateType.WIFI
      ? "wifi"
      : "other";
export const network = Effect.promise(() => Network.getNetworkStateAsync()).pipe(
  Effect.map(networkName),
  Effect.orElseSucceed(() => "other" as const),
);
const activity = (): ReconnectComparisonAttempt["activityStart"] =>
  AppState.currentState === "active"
    ? "active"
    : AppState.currentState === "background"
      ? "background"
      : AppState.currentState === "inactive"
        ? "inactive"
        : "unknown";
const digest = (value: unknown) =>
  digestStringAsync(CryptoDigestAlgorithm.SHA256, JSON.stringify(value));
const encodeShell = Schema.encodeSync(OrchestrationV2ShellSnapshot);

export const makeObservation = Effect.fn("ReconnectObservation.make")(function* (
  plan: ReconnectComparisonPlan,
  assignment: ReconnectComparisonAssignment,
) {
  const networkStart = yield* network;
  const activityStart = activity();
  let backgrounded = activityStart !== "active",
    networkChanged = false;
  const appListener = AppState.addEventListener("change", (state) => {
    if (state !== "active") backgrounded = true;
  });
  const networkListener = Network.addNetworkStateListener((state) => {
    if (networkName(state) !== networkStart) networkChanged = true;
  });
  const ready = yield* Deferred.make<void>();
  let started = performance.now(),
    startRecorded = false,
    finished = false;
  let success: { at: number; snapshot: OrchestrationV2ShellSnapshot } | null = null;
  let lastConfig: ServerConfig | null = null,
    configMs: number | null = null;
  let socketCount = 0,
    catalogRevisionsRequested = 0,
    catalogsReused = 0,
    projectRevisionsRequested = 0;
  const origins = new Set<string>(),
    serverVersions = new Set<string>();
  const start = () => {
    if (startRecorded) return;
    startRecorded = true;
    started = performance.now();
  };
  const observe = (origin: string): Comparison.SessionObservation => {
    origins.add(origin);
    return {
      socket: () => {
        if (!finished) socketCount++;
      },
      catalogRequest: (count) => {
        if (!finished) catalogRevisionsRequested += count;
      },
      shellRequest: (hasRevision) => {
        if (!finished && hasRevision) projectRevisionsRequested++;
      },
      config: (config, reused) => {
        if (finished) return;
        configMs ??= performance.now() - started;
        lastConfig = config;
        catalogsReused += reused;
        serverVersions.add(config.environment.serverVersion);
      },
      synchronized: (snapshot) =>
        Effect.suspend(() => {
          if (finished || success !== null) return Effect.void;
          success = { at: performance.now(), snapshot };
          return Deferred.succeed(ready, undefined).pipe(Effect.asVoid);
        }),
    };
  };
  const finish = Effect.fn("ReconnectObservation.finish")(function* (
    outcome: ReconnectComparisonAttempt["outcome"],
  ) {
    const ended = performance.now();
    finished = true;
    appListener.remove();
    networkListener.remove();
    const completed = success;
    const config = lastConfig;
    const catalogState =
      config === null
        ? null
        : yield* Effect.promise(() =>
            digest(
              config.providers.map((provider) => [
                provider.instanceId,
                providerCatalogRevision(provider),
              ]),
            ),
          );
    const shellState =
      completed === null
        ? null
        : yield* Effect.promise(() => digest(encodeShell(completed.snapshot)));
    const elapsedMs = (completed?.at ?? ended) - started;
    const networkEnd = yield* network;
    const activityEnd = activity();
    const row: ReconnectComparisonAttempt = {
      version: 2,
      kind: "attempt",
      id: `${plan.runId}:${assignment.index}`,
      runId: plan.runId,
      protocolId: plan.protocolId,
      installId: plan.installId,
      buildId: plan.buildId,
      upstreamCommit: plan.upstreamCommit,
      sourceCommit: plan.sourceCommit,
      appPackage: plan.appPackage,
      environmentId: plan.environmentId,
      ...assignment,
      at: DateTime.formatIso(yield* DateTime.now),
      outcome: completed === null ? outcome : "ready",
      elapsedMs,
      cappedReadyMs: completed === null ? plan.timeoutMs : Math.min(plan.timeoutMs, elapsedMs),
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
    return row;
  });
  return { start, observe, finish, ready: Deferred.await(ready), finished: () => finished };
});
