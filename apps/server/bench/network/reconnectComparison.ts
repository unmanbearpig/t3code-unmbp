import * as Schema from "effect/Schema";
import type { ReconnectCase } from "./reconnectManifest.ts";

const Stage = Schema.Struct({
  name: Schema.String,
  atMs: Schema.Number,
  endMs: Schema.optionalKey(Schema.Number),
  bytes: Schema.optionalKey(Schema.Number),
  outcome: Schema.optionalKey(Schema.Literals(["succeeded", "failed"])),
});
export const TerminalProtocol = Schema.Struct({
  requests: Schema.Number,
  chunks: Schema.Number,
  wireAcks: Schema.Number,
  interrupts: Schema.Number,
  maxPendingChunks: Schema.Number,
});
export const ReconnectResult = Schema.Struct({
  terminalProtocol: Schema.optionalKey(TerminalProtocol),
  totalMs: Schema.Number,
  probeSamples: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({ startedMs: Schema.Number, completedMs: Schema.Number, rttMs: Schema.Number }),
    ),
  ),
  operationMetrics: Schema.optionalKey(
    Schema.Record(Schema.String, Schema.Union([Schema.Number, Schema.String])),
  ),
  shellLiveMs: Schema.Number,
  selectedTransport: Schema.Literals(["websocket", "webtransport"]),
  selections: Schema.Array(
    Schema.Struct({
      transport: Schema.optionalKey(Schema.Literals(["websocket", "webtransport"])),
      atMs: Schema.Number,
    }),
  ),
  nativeEvidence: Schema.NullOr(
    Schema.Struct({
      constructed: Schema.Number,
      connectReady: Schema.Number,
      controlStreamsOpened: Schema.Number,
      requestStreamsOpened: Schema.Number,
      closedByOwner: Schema.Number,
      aborted: Schema.Number,
      admissionPath: Schema.String,
      ticketPresent: Schema.Boolean,
      reliability: Schema.String,
    }),
  ),
  serverRequests: Schema.Array(
    Schema.Struct({
      path: Schema.String,
      connection: Schema.Number,
      atMs: Schema.Number,
      bytes: Schema.Number,
    }),
  ),
  stages: Schema.Array(Stage),
  requests: Schema.Record(Schema.String, Schema.Number),
  bytes: Schema.Number,
  packets: Schema.Number,
  events: Schema.Number,
  sockets: Schema.Number,
  correct: Schema.Boolean,
  resources: Schema.Struct({
    window: Schema.String,
    client: Schema.Record(Schema.String, Schema.Number),
    server: Schema.Record(Schema.String, Schema.Number),
  }),
});
export type ReconnectResult = typeof ReconnectResult.Type;
export interface ReconnectRow {
  readonly variant: string;
  readonly revision: string;
  readonly dirty: boolean;
  readonly manifestHash: string;
  readonly case: ReconnectCase;
  readonly result?: ReconnectResult;
  readonly error?: string;
  readonly controller?: unknown;
  readonly diagnostics?: unknown;
}
export function distribution(values: readonly number[]) {
  if (!values.length) return null;
  const ordered = values.toSorted((a, b) => a - b);
  const middle = Math.floor(ordered.length / 2);
  return {
    n: ordered.length,
    min: ordered[0]!,
    median: ordered.length % 2 ? ordered[middle]! : (ordered[middle - 1]! + ordered[middle]!) / 2,
    max: ordered.at(-1)!,
    observedP90: ordered.length >= 20 ? ordered[Math.ceil(ordered.length * 0.9) - 1]! : null,
  };
}
const cell = (row: ReconnectRow) =>
  [
    row.case.profile.name,
    row.case.workload,
    row.case.scheme,
    row.case.routes,
    row.case.phaseMs,
  ].join("/");
const successful = (row: ReconnectRow) => row.error === undefined && row.result?.correct === true;
function numericDistributions(records: readonly Readonly<Record<string, number | string>>[]) {
  const values = new Map<string, number[]>();
  for (const record of records)
    for (const [key, value] of Object.entries(record)) {
      if (typeof value === "number" && Number.isFinite(value))
        values.set(key, [...(values.get(key) ?? []), value]);
    }
  return Object.fromEntries([...values].map(([key, samples]) => [key, distribution(samples)]));
}

/** Only successful, identical case IDs enter paired deltas. Failures remain visible separately. */
export function summarize(rows: readonly ReconnectRow[], baselineVariant: string) {
  const indexed = new Map<string, ReconnectRow>();
  const revisions = new Map<string, string>();
  const groups = new Map<string, ReconnectRow[]>();
  const hash = rows[0]?.manifestHash;
  for (const row of rows) {
    if (row.dirty || row.manifestHash !== hash) throw new Error("Changed benchmark provenance");
    const revision = revisions.get(row.variant);
    if (revision !== undefined && revision !== row.revision)
      throw new Error("Variant revision changed");
    revisions.set(row.variant, row.revision);
    const key = `${row.variant}/${row.case.id}`;
    if (indexed.has(key)) throw new Error(`Duplicate case ${key}`);
    indexed.set(key, row);
    const groupKey = `${row.variant}/${cell(row)}`;
    groups.set(groupKey, [...(groups.get(groupKey) ?? []), row]);
    if (successful(row)) {
      for (const value of [
        row.result!.totalMs,
        row.result!.shellLiveMs,
        row.result!.bytes,
        row.result!.packets,
      ])
        if (!Number.isFinite(value) || value < 0) throw new Error("Invalid successful measurement");
    }
  }
  return [...groups].map(([key, group]) => {
    const completed = group.filter(successful);
    const paired = completed.flatMap((row) => {
      const baseline = indexed.get(`${baselineVariant}/${row.case.id}`);
      if (!baseline || !successful(baseline)) return [];
      if (JSON.stringify(row.case) !== JSON.stringify(baseline.case))
        throw new Error("Paired inputs differ");
      return [{ row, baseline }];
    });
    return {
      key,
      attempted: group.length,
      completed: completed.length,
      failures: group
        .filter((row) => !successful(row))
        .map((row) => ({ caseId: row.case.id, error: row.error ?? "incorrect state" })),
      selectedTransports: [...new Set(completed.map((row) => row.result!.selectedTransport))],
      selectedTransportCounts: Object.fromEntries(
        ["websocket", "webtransport"].map((transport) => [
          transport,
          completed.filter((row) => row.result!.selectedTransport === transport).length,
        ]),
      ),
      operationMetrics: numericDistributions(
        completed.map((row) => row.result!.operationMetrics ?? {}),
      ),
      resources: {
        windows: [...new Set(completed.map((row) => row.result!.resources.window))],
        client: numericDistributions(completed.map((row) => row.result!.resources.client)),
        server: numericDistributions(completed.map((row) => row.result!.resources.server)),
      },
      completionMs: distribution(completed.map((row) => row.result!.totalMs)),
      shellLiveMs: distribution(completed.map((row) => row.result!.shellLiveMs)),
      bytes: distribution(completed.map((row) => row.result!.bytes)),
      packets: distribution(completed.map((row) => row.result!.packets)),
      pairedAgainst: baselineVariant,
      pairedN: paired.length,
      pairedDeltaMs: distribution(
        paired.map(({ row, baseline }) => row.result!.totalMs - baseline.result!.totalMs),
      ),
      pairedDeltaPercent: distribution(
        paired.flatMap(({ row, baseline }) =>
          baseline.result!.totalMs > 0
            ? [100 * (row.result!.totalMs / baseline.result!.totalMs - 1)]
            : [],
        ),
      ),
    };
  });
}
