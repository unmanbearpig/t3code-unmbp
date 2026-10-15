import { median, type ReconnectRow } from "./reconnectComparison.ts";

export const aaTolerance = {
  deterministicRelative: 0.02,
  deterministicAbsoluteMs: 5,
  stochasticSamples: 20,
  bootstrapSamples: 2000,
  confidence: 0.95,
  ksFactor: 1.36,
} as const;
const cellKey = (row: ReconnectRow) =>
  [
    row.variant,
    row.case.profile.name,
    row.case.workload,
    row.case.scheme,
    row.case.routes,
    row.case.phaseMs,
    row.case.configDelayMs ?? 0,
    row.case.slowSocketDelayMs ?? 0,
  ].join("/");
function interval(values: readonly number[]) {
  let seed = 0x5eed;
  const medians = Array.from({ length: aaTolerance.bootstrapSamples }, () =>
    median(
      values.map(() => {
        seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
        return values[seed % values.length]!;
      }),
    )!,
  );
  medians.sort((a, b) => a - b);
  return [
    medians[Math.floor(medians.length * 0.025)]!,
    medians[Math.ceil(medians.length * 0.975) - 1]!,
  ] as const;
}
export function ksDistance(a: readonly number[], b: readonly number[]) {
  return Math.max(
    ...[...a, ...b].map((value) =>
      Math.abs(
        a.filter((v) => v <= value).length / a.length -
          b.filter((v) => v <= value).length / b.length,
      ),
    ),
  );
}
/** Failures remain failures. Thread and shell must each satisfy the predeclared tolerance. */
export function compareAA(serial: readonly ReconnectRow[], parallel: readonly ReconnectRow[]) {
  const groups = (rows: readonly ReconnectRow[]) => {
    const result = new Map<string, ReconnectRow[]>();
    for (const row of rows) {
      const key = cellKey(row);
      result.set(key, [...(result.get(key) ?? []), row]);
    }
    return result;
  };
  const left = groups(serial),
    right = groups(parallel);
  const cells = [...new Set([...left.keys(), ...right.keys()])].sort().map((key) => {
    const a = left.get(key) ?? [],
      b = right.get(key) ?? [];
    const inputMatch =
      a.length === b.length &&
      a.length > 0 &&
      a
        .map((r) => r.case.id)
        .sort()
        .join() ===
        b
          .map((r) => r.case.id)
          .sort()
          .join();
    const complete = inputMatch && [...a, ...b].every((r) => r.result?.correct);
    const stochastic = a[0]?.case.profile.stochastic === true;
    const metrics = (["totalMs", "shellLiveMs"] as const).map((metric) => {
      const x = a.flatMap((r) => (r.result?.correct ? [r.result[metric]] : [])),
        y = b.flatMap((r) => (r.result?.correct ? [r.result[metric]] : []));
      if (!complete || !x.length || !y.length) return { metric, pass: false };
      const serialMedian = median(x)!,
        parallelMedian = median(y)!;
      if (!stochastic) {
        const toleranceMs = Math.max(
          aaTolerance.deterministicAbsoluteMs,
          serialMedian * aaTolerance.deterministicRelative,
        );
        return {
          metric,
          serialMedian,
          parallelMedian,
          toleranceMs,
          pass: Math.abs(serialMedian - parallelMedian) <= toleranceMs,
        };
      }
      const serialInterval = interval(x),
        parallelInterval = interval(y),
        distance = ksDistance(x, y);
      const limit = aaTolerance.ksFactor * Math.sqrt((x.length + y.length) / (x.length * y.length));
      return {
        metric,
        serialMedian,
        parallelMedian,
        serialInterval,
        parallelInterval,
        distance,
        limit,
        pass:
          x.length >= 20 &&
          y.length >= 20 &&
          serialInterval[0] <= parallelInterval[1] &&
          parallelInterval[0] <= serialInterval[1] &&
          distance <= limit,
      };
    });
    const falseReplacementsAgree =
      stochastic ||
      a.reduce((sum, r) => sum + (r.result?.falseReplacements ?? 0), 0) ===
        b.reduce((sum, r) => sum + (r.result?.falseReplacements ?? 0), 0);
    return {
      key,
      falseReplacementsAgree,
      stochastic,
      serialCount: a.length,
      parallelCount: b.length,
      pass: falseReplacementsAgree && metrics.every((m) => m.pass),
      metrics,
    };
  });
  return { pass: cells.length > 0 && cells.every((c) => c.pass), tolerance: aaTolerance, cells };
}
