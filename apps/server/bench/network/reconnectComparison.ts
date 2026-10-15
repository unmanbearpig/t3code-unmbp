import type { ReconnectCase } from "./reconnectManifest.ts";
import type { VariantOptions } from "./reconnectOptions.ts";
import type { Stage } from "./reconnectClient.ts";
export interface ReconnectRow {
  readonly variant: string;
  readonly revision: string;
  readonly dirty: boolean;
  readonly manifestHash: string;
  readonly case: ReconnectCase;
  readonly result?: {
    readonly totalMs: number;
    readonly shellLiveMs: number;
    readonly negotiatedExtensions?: string;
    readonly switches: VariantOptions;
    readonly serverRequests: readonly {
      path: string;
      connection: number;
      atMs: number;
      bytes: number;
    }[];
    readonly stages: readonly Stage[];
    readonly requests: Readonly<Record<string, number>>;
    readonly bytes: number;
    readonly packets: number;
    readonly events: number;
    readonly sockets: number;
    readonly correct: boolean;
  };
  readonly error?: string;
  readonly controller?: unknown;
}
export const median = (values: readonly number[]) => {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid]! : (sorted[mid - 1]! + sorted[mid]!) / 2;
};
/** Nearest-rank observed quantile. Never interpolate a p90 from fewer than twenty observations. */
export const observedP90 = (values: readonly number[]) =>
  values.length < 20
    ? null
    : [...values].sort((a, b) => a - b)[Math.ceil(values.length * 0.9) - 1]!;
export function summarize(rows: readonly ReconnectRow[]) {
  const groups = new Map<string, ReconnectRow[]>();
  for (const row of rows) {
    const key = [
      row.variant,
      row.case.profile.name,
      row.case.workload,
      row.case.scheme,
      row.case.routes,
    ].join("/");
    groups.set(key, [...(groups.get(key) ?? []), row]);
  }
  return [...groups].map(([key, group]) => {
    const results = group.flatMap((row) => (row.result?.correct ? [row.result] : []));
    const stages = new Map<string, number[]>();
    const durations = new Map<string, number[]>();
    for (const result of results)
      for (const stage of result.stages) {
        stages.set(stage.name, [...(stages.get(stage.name) ?? []), stage.endMs ?? stage.atMs]);
        if (stage.endMs !== undefined)
          durations.set(stage.name, [
            ...(durations.get(stage.name) ?? []),
            stage.endMs - stage.atMs,
          ]);
      }
    return {
      key,
      count: group.length,
      successes: results.length,
      failures: group.length - results.length,
      medianMs: median(results.map((r) => r.totalMs)),
      shellMedianMs: median(results.map((r) => r.shellLiveMs)),
      p90Ms: observedP90(results.map((r) => r.totalMs)),
      stageMediansMs: Object.fromEntries([...stages].map(([key, values]) => [key, median(values)])),
      stageDurationMediansMs: Object.fromEntries(
        [...durations].map(([key, values]) => [key, median(values)]),
      ),
      medianBytes: median(results.map((r) => r.bytes)),
      medianPackets: median(results.map((r) => r.packets)),
      requests: results.map((r) => r.requests),
    };
  });
}
