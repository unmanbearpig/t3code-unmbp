import type { PerformanceTelemetry } from "@t3tools/client-runtime/platform";

type Measurement = PerformanceTelemetry.PerformanceMeasurement;
type Operation = PerformanceTelemetry.PerformanceOperation;

export const SLOW_THRESHOLDS_MS: Readonly<Record<Operation, number>> = {
  "startup.navigation-ready": 1_000,
  "startup.splash-hide-requested": 1_000,
  "lifecycle.resume-js": 100,
  "runtime.js-delay": 100,
  "runtime.memory-warning": 0,
  "cache.read": 50,
  "cache.decode": 16,
  "cache.encode": 16,
  "cache.write": 50,
  "shell.apply-batch": 16,
  "thread.apply-batch": 16,
  "rpc.request": 250,
};

const RECENT_LIMIT = 128;
const SLOW_SAMPLE_INTERVAL_MS = 10_000;
type Mutable<T> = { -readonly [Key in keyof T]: T[Key] };

export interface PerformanceSample extends Measurement {
  readonly timestamp: string;
}

export interface PerformanceSummary {
  readonly operation: Operation;
  readonly category?: Measurement["category"];
  readonly count: number;
  readonly totalMs: number;
  readonly maxMs: number;
  readonly slowCount: number;
  readonly failureCount: number;
  readonly interruptedCount: number;
  readonly itemCount: number;
  readonly payloadChars: number;
  readonly recentSamples: number;
  readonly recentP50Ms: number;
  readonly recentP95Ms: number;
}

function finiteCount(value: number | undefined) {
  return value !== undefined && Number.isFinite(value) && value >= 0 ? value : 0;
}

/** Fixed operation/category cardinality; percentiles use the most recent 128 observations. */
export function createPerformanceTelemetry(
  now: () => number,
  slowSample: (sample: PerformanceSample) => void,
) {
  const summaries = new Map<
    string,
    {
      summary: Mutable<Omit<PerformanceSummary, "recentSamples" | "recentP50Ms" | "recentP95Ms">>;
      recent: number[];
      cursor: number;
      lastPublished: number | null;
    }
  >();
  return {
    record: (input: Measurement) => {
      if (!Number.isFinite(input.durationMs) || input.durationMs < 0) return;
      const threshold = SLOW_THRESHOLDS_MS[input.operation];
      if (threshold === undefined) return;
      const key = `${input.operation}:${input.category ?? ""}`;
      let current = summaries.get(key);
      if (!current) {
        current = {
          summary: {
            operation: input.operation,
            ...(input.category === undefined ? {} : { category: input.category }),
            count: 0,
            totalMs: 0,
            maxMs: 0,
            slowCount: 0,
            failureCount: 0,
            interruptedCount: 0,
            itemCount: 0,
            payloadChars: 0,
          },
          recent: [],
          cursor: 0,
          lastPublished: null,
        };
        summaries.set(key, current);
      }
      const summary = current.summary;
      summary.count++;
      summary.totalMs += input.durationMs;
      summary.maxMs = Math.max(summary.maxMs, input.durationMs);
      if (input.durationMs >= threshold) summary.slowCount++;
      if (input.outcome === "failure") summary.failureCount++;
      if (input.outcome === "interrupted") summary.interruptedCount++;
      summary.itemCount += finiteCount(input.itemCount);
      summary.payloadChars += finiteCount(input.payloadChars);
      current.recent[current.cursor] = input.durationMs;
      current.cursor = (current.cursor + 1) % RECENT_LIMIT;
      const time = now();
      const milestone =
        input.operation.startsWith("startup.") || input.operation === "runtime.memory-warning";
      if (
        (milestone || input.durationMs >= threshold || input.outcome !== "success") &&
        (current.lastPublished === null || time - current.lastPublished >= SLOW_SAMPLE_INTERVAL_MS)
      ) {
        current.lastPublished = time;
        slowSample({
          timestamp: new Date(time).toISOString(),
          operation: input.operation,
          durationMs: input.durationMs,
          outcome: input.outcome,
          ...(input.category === undefined ? {} : { category: input.category }),
          ...(input.itemCount === undefined ? {} : { itemCount: finiteCount(input.itemCount) }),
          ...(input.payloadChars === undefined
            ? {}
            : { payloadChars: finiteCount(input.payloadChars) }),
          ...(input.rpcMethod === undefined ? {} : { rpcMethod: input.rpcMethod.slice(0, 100) }),
        });
      }
    },
    snapshot: (): ReadonlyArray<PerformanceSummary> =>
      [...summaries.values()].map(({ summary, recent }) => {
        const sorted = [...recent].sort((left, right) => left - right);
        return {
          ...summary,
          recentSamples: sorted.length,
          recentP50Ms: sorted[Math.ceil(sorted.length * 0.5) - 1] ?? 0,
          recentP95Ms: sorted[Math.ceil(sorted.length * 0.95) - 1] ?? 0,
        };
      }),
    clear: () => summaries.clear(),
  };
}

/** Read only the numeric, static metadata emitted by the performance recorder. */
export function parsePerformanceSample(raw: string): PerformanceSample | null {
  try {
    const value: unknown = JSON.parse(raw);
    if (typeof value !== "object" || value === null) return null;
    const sample = value as Record<string, unknown>;
    if (
      typeof sample.operation !== "string" ||
      typeof SLOW_THRESHOLDS_MS[sample.operation as Operation] !== "number" ||
      typeof sample.timestamp !== "string" ||
      !Number.isFinite(Date.parse(sample.timestamp)) ||
      typeof sample.durationMs !== "number" ||
      !Number.isFinite(sample.durationMs) ||
      sample.durationMs < 0 ||
      (sample.outcome !== "success" &&
        sample.outcome !== "failure" &&
        sample.outcome !== "interrupted")
    )
      return null;
    const category = ["shell", "thread", "server-config", "vcs-refs"].includes(
      String(sample.category),
    )
      ? (sample.category as Measurement["category"])
      : undefined;
    return {
      operation: sample.operation as Operation,
      timestamp: sample.timestamp,
      durationMs: sample.durationMs,
      outcome: sample.outcome,
      ...(category === undefined ? {} : { category }),
      ...(typeof sample.itemCount === "number" ? { itemCount: finiteCount(sample.itemCount) } : {}),
      ...(typeof sample.payloadChars === "number"
        ? { payloadChars: finiteCount(sample.payloadChars) }
        : {}),
      ...(typeof sample.rpcMethod === "string"
        ? { rpcMethod: sample.rpcMethod.slice(0, 100) }
        : {}),
    };
  } catch {
    return null;
  }
}

/** The caller stops ticks in the background and resets the deadline on every resume. */
export function createJsDelaySampler(now: () => number, record: (delayMs: number) => void) {
  let expected: number | null = null;
  return {
    start: () => {
      expected = now() + 1_000;
    },
    stop: () => {
      expected = null;
    },
    tick: () => {
      if (expected === null) return;
      const time = now();
      record(Math.max(0, time - expected));
      expected = time + 1_000;
    },
  };
}
