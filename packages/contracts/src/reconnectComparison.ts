import * as Schema from "effect/Schema";
import {
  EnvironmentId,
  IsoDateTime,
  NonNegativeInt,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";

const Label = TrimmedNonEmptyString.check(Schema.isMaxLength(160));
const Digest = Schema.NullOr(Label);
const DurationMs = Schema.Number.check(Schema.isGreaterThanOrEqualTo(0));
export const ReconnectComparisonMode = Schema.Literals(["baseline", "proposed"]);
export type ReconnectComparisonMode = typeof ReconnectComparisonMode.Type;
const Network = Schema.Literals(["wifi", "cellular", "other"]);
const Activity = Schema.Literals(["active", "background", "inactive", "unknown"]);
export const ReconnectComparisonAssignment = Schema.Struct({
  index: NonNegativeInt,
  round: Schema.Int,
  position: Schema.Literals([0, 1]),
  mode: ReconnectComparisonMode,
  warmup: Schema.Boolean,
});
export type ReconnectComparisonAssignment = typeof ReconnectComparisonAssignment.Type;
const Identity = {
  version: Schema.Literal(2),
  protocolId: Label,
  runId: Label,
  installId: Label,
  buildId: Label,
  upstreamCommit: Label,
  sourceCommit: Label,
  appPackage: Label,
  environmentId: EnvironmentId,
  at: IsoDateTime,
};
export const ReconnectComparisonPlan = Schema.Struct({
  ...Identity,
  kind: Schema.Literal("plan"),
  id: Label,
  timeoutMs: NonNegativeInt,
  network: Network,
  assignments: Schema.Array(ReconnectComparisonAssignment).check(Schema.isMaxLength(1000)),
});
export type ReconnectComparisonPlan = typeof ReconnectComparisonPlan.Type;
export const ReconnectComparisonAttempt = Schema.Struct({
  ...Identity,
  ...ReconnectComparisonAssignment.fields,
  kind: Schema.Literal("attempt"),
  id: Label,
  outcome: Schema.Literals(["ready", "timeout", "failed", "interrupted"]),
  elapsedMs: DurationMs,
  cappedReadyMs: DurationMs,
  configMs: Schema.NullOr(DurationMs),
  jsonDown: NonNegativeInt,
  jsonUp: NonNegativeInt,
  instrumentationMs: DurationMs,
  socketCount: NonNegativeInt,
  origins: Schema.Array(Label),
  serverVersions: Schema.Array(Label),
  catalogRevisionsSent: NonNegativeInt,
  catalogsReused: NonNegativeInt,
  projectRevisionsSent: NonNegativeInt,
  catalogState: Digest,
  shellState: Digest,
  networkStart: Network,
  networkEnd: Network,
  networkChanged: Schema.Boolean,
  activityStart: Activity,
  activityEnd: Activity,
  backgrounded: Schema.Boolean,
});
export type ReconnectComparisonAttempt = typeof ReconnectComparisonAttempt.Type;
export const ReconnectComparisonRecord = Schema.Union([
  ReconnectComparisonPlan,
  ReconnectComparisonAttempt,
]);
export type ReconnectComparisonRecord = typeof ReconnectComparisonRecord.Type;
export const ReconnectComparisonInput = Schema.Struct({
  records: Schema.Array(ReconnectComparisonRecord).check(Schema.isMaxLength(20)),
});
export class ReconnectComparisonError extends Schema.TaggedError<ReconnectComparisonError>()(
  "ReconnectComparisonError",
  { cause: Schema.Defect() },
) {
  override get message() {
    return "Could not store reconnect comparison.";
  }
}
