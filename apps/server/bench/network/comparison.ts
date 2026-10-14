import * as Schema from "effect/Schema";

import { caseId, makeOperationSuitePlan, makeSuitePlan } from "./suite.ts";

export const BenchRow = Schema.Struct({
  scenario: Schema.String,
  layer: Schema.Literals(["raw", "rpc", "operations"]),
  transport: Schema.String,
  workload: Schema.String,
  repetition: Schema.Number,
  seed: Schema.Number,
  timeoutMs: Schema.Number,
  metrics: Schema.Record(Schema.String, Schema.Union([Schema.Number, Schema.String])),
});
export type BenchRow = typeof BenchRow.Type;

export const SuiteResults = Schema.Struct({
  suite: Schema.Struct({ hash: Schema.String }),
  complete: Schema.Boolean,
  metadata: Schema.Struct({ revision: Schema.String, environmentHash: Schema.String }),
  rows: Schema.Array(BenchRow),
});
export type SuiteResults = typeof SuiteResults.Type;

/** Rejects results from different experiments or incomplete files, rather than scoring partial work. */
export function validateSuiteResults(result: SuiteResults) {
  const plan = [makeSuitePlan(), makeOperationSuitePlan()].find(
    (plan) => plan.hash === result.suite.hash,
  );
  if (!plan)
    throw new Error("Suite configuration differs; rerun both revisions with the same suite");
  if (!result.complete || result.rows.length !== plan.cases.length)
    throw new Error("Suite is incomplete");
  const rows = new Map(result.rows.map((row) => [caseId(row), row]));
  if (rows.size !== result.rows.length) throw new Error("Suite contains duplicate cases");
  for (const entry of plan.cases) {
    const row = rows.get(caseId(entry));
    if (!row || row.seed !== entry.seed || row.timeoutMs !== entry.timeoutMs)
      throw new Error(`Missing or changed suite case ${caseId(entry)}`);
  }
}

const summarize = (values: readonly number[]) => {
  const sorted = values.toSorted((a, b) => a - b);
  return {
    min: sorted[0]!,
    median: sorted[Math.floor(sorted.length / 2)]!,
    max: sorted[sorted.length - 1]!,
  };
};

const metricNames: Record<string, readonly string[]> = {
  connect: ["connectAndPing.p50"],
  rpc: ["rtt.p50", "rtt.p99"],
  stream: ["latency.p50", "latency.p99", "stall.p99"],
  fanout: ["latency.p50", "latency.p99", "stall.p99"],
  hol: ["bulkMs", "snapshotMs", "pingDuringBulk.p99", "pingDuringSnapshot.p99"],
};

/** Per-case comparisons keep tail latency, bulk time, and completion visible separately. */
export function compareSuites(baseline: SuiteResults, candidate: SuiteResults) {
  validateSuiteResults(baseline);
  validateSuiteResults(candidate);
  if (baseline.suite.hash !== candidate.suite.hash)
    throw new Error("Suite configuration differs; compare the same operation and transport suite");
  if (baseline.metadata.environmentHash !== candidate.metadata.environmentHash)
    throw new Error(
      "Benchmark environments differ; use the same host, Node, kernel, and network tools",
    );
  const groups = new Map<string, { baseline: BenchRow[]; candidate: BenchRow[] }>();
  for (const [name, result] of [
    ["baseline", baseline],
    ["candidate", candidate],
  ] as const) {
    for (const row of result.rows) {
      const key = [row.layer, row.scenario, row.workload, row.transport].join("/");
      let group = groups.get(key);
      if (!group) groups.set(key, (group = { baseline: [], candidate: [] }));
      group[name].push(row);
    }
  }
  return Array.from(groups, ([key, group]) => {
    const completed = (rows: readonly BenchRow[]) =>
      rows.filter((row) => row.metrics.outcome === undefined).length;
    const beforeCompleted = completed(group.baseline);
    const afterCompleted = completed(group.candidate);
    const completion =
      afterCompleted < beforeCompleted
        ? "regressed"
        : afterCompleted > beforeCompleted
          ? "improved"
          : "unchanged";
    const metrics = [];
    // No timing win for a workload that no longer finishes. Partial percentiles remain in raw results.
    if (beforeCompleted === group.baseline.length && afterCompleted === group.candidate.length) {
      for (const metric of group.baseline[0]!.layer === "operations"
        ? ["operationMs", "linkTxBytes", "linkTxPackets"]
        : (metricNames[group.baseline[0]!.workload] ?? [])) {
        const before = group.baseline.map((row) => row.metrics[metric]);
        const after = group.candidate.map((row) => row.metrics[metric]);
        if (
          !before.every(
            (value): value is number => typeof value === "number" && Number.isFinite(value),
          ) ||
          !after.every(
            (value): value is number => typeof value === "number" && Number.isFinite(value),
          )
        )
          continue;
        const beforeStats = summarize(before);
        const afterStats = summarize(after);
        const percent =
          beforeStats.median === 0 ? null : 100 * (afterStats.median / beforeStats.median - 1);
        const series = metric.replace(/\.p\d+$/, "");
        const enoughSamples =
          !metric.endsWith(".p99") ||
          [...group.baseline, ...group.candidate].every(
            (row) =>
              typeof row.metrics[`${series}.n`] === "number" &&
              Number(row.metrics[`${series}.n`]) >= 100,
          );
        const enoughRepeats = Math.min(before.length, after.length) >= 3;
        const material =
          (percent === null ? afterStats.median > 0 : Math.abs(percent) >= 10) &&
          Math.abs(afterStats.median - beforeStats.median) >= 1;
        const signal =
          enoughRepeats && enoughSamples && material
            ? afterStats.min > beforeStats.max
              ? "regressed"
              : afterStats.max < beforeStats.min
                ? "improved"
                : "inconclusive"
            : "inconclusive";
        metrics.push({
          metric,
          baseline: beforeStats,
          candidate: afterStats,
          percent,
          signal,
          enoughSamples,
          enoughRepeats,
        });
      }
    }
    return {
      case: key,
      completion,
      baselineCompleted: beforeCompleted,
      candidateCompleted: afterCompleted,
      repetitions: group.baseline.length,
      metrics,
    };
  });
}
