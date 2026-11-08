import * as Schema from "effect/Schema";
import { IsoDateTime, NonNegativeInt, TrimmedNonEmptyString } from "./baseSchemas.ts";

const Counter = NonNegativeInt.check(Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER));
const DurationMs = Schema.Number.check(
  Schema.isGreaterThanOrEqualTo(0),
  Schema.isLessThanOrEqualTo(86_400_000),
);
const Label = TrimmedNonEmptyString.check(Schema.isMaxLength(128));

export const ReconnectTraffic = Schema.Struct({
  receivedBytes: Counter,
  sentBytes: Counter,
  receivedPackets: Counter,
  sentPackets: Counter,
});
export type ReconnectTraffic = typeof ReconnectTraffic.Type;

export const ReconnectSample = Schema.Struct({
  version: Schema.Literal(1),
  id: Label,
  recordedAt: IsoDateTime,
  arm: Schema.Literals(["old", "new"]),
  pipelineArm: Schema.Literals(["old", "new"]),
  platform: Schema.Literals(["android", "ios", "web"]),
  appVersion: Label,
  serverVersion: Schema.NullOr(Label),
  networkType: Schema.Literals(["wifi", "cellular", "other"]),
  transport: Schema.Literals(["websocket", "webtransport", "unknown"]),
  outcome: Schema.Literals(["ready", "failed", "interrupted"]),
  cachedProviders: Counter,
  reusedCatalogs: Counter,
  configReadyMs: Schema.NullOr(DurationMs),
  shellReadyMs: Schema.NullOr(DurationMs),
  rpcRttMs: Schema.NullOr(DurationMs),
  receivedJsonBytes: Schema.NullOr(Counter),
  sentJsonBytes: Schema.NullOr(Counter),
  receivedFrames: Schema.NullOr(Counter),
  sentFrames: Schema.NullOr(Counter),
  rpcAcksSent: Schema.NullOr(Counter),
  /** Android UID counters cover every environment in this app, before probe and upload. */
  appTraffic: Schema.NullOr(ReconnectTraffic),
});
export type ReconnectSample = typeof ReconnectSample.Type;

export const ReconnectSamplesInput = Schema.Struct({
  samples: Schema.Array(ReconnectSample).check(Schema.isMaxLength(20)),
});

export const ReconnectLoopSample = Schema.Struct({
  version: Schema.Literal(1),
  id: Label,
  runId: Label,
  at: IsoDateTime,
  round: Schema.Int.check(Schema.isGreaterThanOrEqualTo(-1)),
  arm: Schema.Literals(["warmup", "full", "cached", "restart"]),
  pipelineArm: Schema.Literals(["old", "new"]),
  platform: Schema.Literals(["android", "ios", "web"]),
  appVersion: Label,
  serverVersion: Schema.NullOr(Label),
  networkType: Schema.Literals(["wifi", "cellular", "other"]),
  transport: Schema.Literals(["websocket", "webtransport", "unknown"]),
  ok: Schema.Boolean,
  error: Schema.optionalKey(TrimmedNonEmptyString.check(Schema.isMaxLength(300))),
  prepareMs: DurationMs,
  configMs: DurationMs,
  syncedMs: DurationMs,
  jsonDown: Counter,
  jsonUp: Counter,
  sentRevisions: Counter,
  reusedCatalogs: Counter,
  projectFrames: Counter,
  appTraffic: Schema.NullOr(ReconnectTraffic),
});
export type ReconnectLoopSample = typeof ReconnectLoopSample.Type;

export const ReconnectLoopSamplesInput = Schema.Struct({
  samples: Schema.Array(ReconnectLoopSample).check(Schema.isMaxLength(20)),
});

export class ReconnectMetricsError extends Schema.TaggedError<ReconnectMetricsError>()(
  "ReconnectMetricsError",
  {
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return "Could not store reconnect measurements.";
  }
}
