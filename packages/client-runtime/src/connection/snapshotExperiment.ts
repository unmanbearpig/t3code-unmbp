import type { EnvironmentId, ReconnectSample, ReconnectTraffic } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";

export interface SnapshotTrial {
  readonly id: string;
  readonly arm: "old" | "new";
  readonly platform: ReconnectSample["platform"];
  readonly appVersion: string;
  readonly networkType: ReconnectSample["networkType"];
  readonly readTraffic: () => ReconnectTraffic | null;
}

export class SnapshotExperiment extends Context.Reference<{
  readonly begin: (environmentId: EnvironmentId) => SnapshotTrial | undefined;
  readonly save: (environmentId: EnvironmentId, sample: ReconnectSample) => Effect.Effect<void>;
  readonly pending: (environmentId: EnvironmentId) => Effect.Effect<ReadonlyArray<ReconnectSample>>;
  readonly acknowledge: (
    environmentId: EnvironmentId,
    ids: ReadonlyArray<string>,
  ) => Effect.Effect<void>;
} | null>("@t3tools/client-runtime/connection/SnapshotExperiment", { defaultValue: () => null }) {}

export class AttemptSnapshotTrial extends Context.Reference<{
  readonly trial: SnapshotTrial;
  readonly started: bigint;
  readonly trafficBefore: ReconnectTraffic | null;
  claimed: boolean;
} | null>("@t3tools/client-runtime/connection/AttemptSnapshotTrial", {
  defaultValue: () => null,
}) {}

export const beginSnapshotAttempt = Effect.fn("SnapshotExperiment.beginAttempt")(function* (
  environmentId: EnvironmentId,
) {
  const experiment = yield* SnapshotExperiment;
  const trial = experiment?.begin(environmentId);
  const started = yield* Clock.monotonicTimeNanos;
  const value =
    trial === undefined
      ? null
      : { trial, started, trafficBefore: trial.readTraffic(), claimed: false };
  const failed = Effect.fn("SnapshotExperiment.unopened")(function* (
    pipelineArm: "old" | "new",
    outcome: "failed" | "interrupted",
  ) {
    if (value === null || value.claimed || experiment === null) return;
    const now = yield* DateTime.now;
    yield* experiment.save(environmentId, {
      version: 1,
      id: value.trial.id,
      arm: value.trial.arm,
      pipelineArm,
      platform: value.trial.platform,
      appVersion: value.trial.appVersion,
      networkType: value.trial.networkType,
      recordedAt: DateTime.formatIso(now),
      serverVersion: null,
      transport: "unknown",
      outcome,
      cachedProviders: 0,
      reusedCatalogs: 0,
      configReadyMs: null,
      shellReadyMs: null,
      rpcRttMs: null,
      receivedJsonBytes: null,
      sentJsonBytes: null,
      receivedFrames: null,
      sentFrames: null,
      rpcAcksSent: null,
      appTraffic: trafficDelta(value.trafficBefore, value.trial.readTraffic()),
    });
  });
  return { value, failed };
});

export function trafficDelta(
  before: ReconnectTraffic | null,
  after: ReconnectTraffic | null,
): ReconnectTraffic | null {
  if (before === null || after === null) return null;
  const delta = {
    receivedBytes: after.receivedBytes - before.receivedBytes,
    sentBytes: after.sentBytes - before.sentBytes,
    receivedPackets: after.receivedPackets - before.receivedPackets,
    sentPackets: after.sentPackets - before.sentPackets,
  };
  return Object.values(delta).every((value) => Number.isSafeInteger(value) && value >= 0)
    ? delta
    : null;
}
