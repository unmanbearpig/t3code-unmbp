import {
  WS_METHODS,
  type EnvironmentId,
  type ReconnectSample,
  type ServerConfigStreamEvent,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Cause from "effect/Cause";
import * as Option from "effect/Option";
import {
  AttemptSnapshotTrial,
  SnapshotExperiment,
  trafficDelta,
} from "../connection/snapshotExperiment.ts";
import { AttemptReconnectArm } from "../connection/reconnectExperiment.ts";
import type { WsRpcProtocolClient } from "./protocol.ts";

export const makeReconnectMeasurement = Effect.fn("ReconnectMeasurement.make")(function* (
  environmentId: EnvironmentId,
) {
  const experiment = yield* SnapshotExperiment;
  const attempt = yield* AttemptSnapshotTrial;
  const trial = attempt?.trial ?? experiment?.begin(environmentId);
  if (attempt !== null) attempt.claimed = true;
  if (trial === undefined)
    return {
      arm: "new" as const,
      enabled: false,
      track: (_direction: "in" | "out", _chunk: unknown) => {},
      snapshot: (_event: ServerConfigStreamEvent) => Effect.void,
      cached: (_count: number) => {},
      selectedTransport: (_transport: "websocket" | "webtransport") => {},
      ready: (_client: WsRpcProtocolClient, _transport: "websocket" | "webtransport") =>
        Effect.void,
    };
  const scope = yield* Effect.scope;
  const started = attempt?.started ?? (yield* Clock.monotonicTimeNanos);
  const pipelineArm = yield* AttemptReconnectArm;
  const trafficBefore = attempt?.trafficBefore ?? trial?.readTraffic() ?? null;
  const encoder = new TextEncoder();
  let captured = false;
  let configReadyMs: number | null = null;
  let cachedProviders = 0;
  let reusedCatalogs = 0;
  let serverVersion: string | null = null;
  let measurementSupported = false;
  let probeSupported = false;
  let transport: ReconnectSample["transport"] = "unknown";
  const counters = {
    receivedJsonBytes: 0,
    sentJsonBytes: 0,
    receivedFrames: 0,
    sentFrames: 0,
    rpcAcksSent: 0,
  };
  const elapsed = Clock.monotonicTimeNanos.pipe(
    Effect.map((now) => Number(now - started) / 1_000_000),
  );
  const track = (direction: "in" | "out", chunk: unknown) => {
    if (trial === undefined || captured) return;
    const bytes =
      typeof chunk === "string"
        ? encoder.encode(chunk)
        : chunk instanceof Uint8Array
          ? chunk
          : undefined;
    if (bytes === undefined) return;
    if (direction === "in") {
      counters.receivedJsonBytes += bytes.length;
      counters.receivedFrames += 1;
    } else {
      counters.sentJsonBytes += bytes.length;
      counters.sentFrames += 1;
      if (bytes.length < 4096) {
        const text = typeof chunk === "string" ? chunk : new TextDecoder().decode(bytes);
        counters.rpcAcksSent += [...text.matchAll(/"_tag"\s*:\s*"Ack"/g)].length;
      }
    }
  };
  const snapshot = Effect.fn("ReconnectMeasurement.snapshot")(function* (
    event: ServerConfigStreamEvent,
  ) {
    if (trial === undefined || event.type !== "snapshot") return;
    if (configReadyMs === null) {
      configReadyMs = yield* elapsed;
      reusedCatalogs = Object.keys(event.cachedProviderCatalogs ?? {}).length;
    }
    serverVersion = event.config.environment.serverVersion;
    measurementSupported = event.config.reconnectMeasurements === true;
    probeSupported = event.config.environment.capabilities.connectionProbe === true;
  });
  const finish = Effect.fn("ReconnectMeasurement.finish")(function* (
    outcome: ReconnectSample["outcome"],
    client?: WsRpcProtocolClient,
  ) {
    if (captured || trial === undefined || experiment === null) return;
    captured = true;
    const duration = yield* elapsed;
    const now = yield* DateTime.now;
    const sample: ReconnectSample = {
      version: 1,
      id: trial.id,
      recordedAt: DateTime.formatIso(now),
      arm: trial.arm,
      pipelineArm,
      platform: trial.platform,
      appVersion: trial.appVersion,
      networkType: trial.networkType,
      serverVersion,
      transport,
      outcome,
      cachedProviders,
      reusedCatalogs,
      configReadyMs,
      shellReadyMs: outcome === "ready" ? duration : null,
      rpcRttMs: null,
      ...counters,
      ...(transport === "websocket"
        ? {}
        : {
            receivedJsonBytes: null,
            sentJsonBytes: null,
            receivedFrames: null,
            sentFrames: null,
            rpcAcksSent: null,
          }),
      appTraffic: trafficDelta(trafficBefore, trial.readTraffic()),
    };
    yield* experiment.save(environmentId, sample);
    if (client === undefined || outcome !== "ready" || !measurementSupported) return;
    yield* Effect.gen(function* () {
      let report = sample;
      if (probeSupported) {
        const probeStart = yield* Clock.monotonicTimeNanos;
        const result = yield* client[WS_METHODS.serverProbe]({}).pipe(
          Effect.timeoutOption("3 seconds"),
          Effect.option,
        );
        if (Option.isSome(result) && Option.isSome(result.value)) {
          const ended = yield* Clock.monotonicTimeNanos;
          report = { ...sample, rpcRttMs: Number(ended - probeStart) / 1_000_000 };
          yield* experiment.save(environmentId, report);
        }
      }
      const pending = yield* experiment.pending(environmentId);
      if (pending.length === 0) return;
      const delivered = yield* client[WS_METHODS.serverRecordReconnectSamples]({
        samples: pending,
      }).pipe(Effect.timeoutOption("5 seconds"), Effect.option);
      if (Option.isSome(delivered) && Option.isSome(delivered.value)) {
        yield* experiment.acknowledge(
          environmentId,
          pending.map((value) => value.id),
        );
      }
    }).pipe(Effect.interruptible, Effect.forkIn(scope));
  }, Effect.uninterruptible);
  yield* Effect.addFinalizer((exit) =>
    finish(Exit.isFailure(exit) && !Cause.hasInterruptsOnly(exit.cause) ? "failed" : "interrupted"),
  );
  return {
    enabled: true,
    arm: trial?.arm ?? "new",
    track,
    snapshot,
    cached: (count: number) => {
      cachedProviders = count;
    },
    selectedTransport: (value: "websocket" | "webtransport") => {
      transport = value;
    },
    ready: (client: WsRpcProtocolClient, selectedTransport: "websocket" | "webtransport") =>
      Effect.sync(() => {
        transport = selectedTransport;
      }).pipe(Effect.andThen(finish("ready", client))),
  };
});
