import { describe, expect, it } from "vite-plus/test";

import {
  createJsDelaySampler,
  createPerformanceTelemetry,
  parsePerformanceSample,
} from "./performance-model";

describe("local performance summaries", () => {
  it("keeps counts and totals while bounding recent percentile samples", () => {
    const telemetry = createPerformanceTelemetry(
      () => 0,
      () => undefined,
    );
    for (let durationMs = 1; durationMs <= 1_000; durationMs++) {
      telemetry.record({
        operation: "cache.decode",
        category: "shell",
        durationMs,
        outcome: "success",
        itemCount: 2,
        payloadChars: 20,
      });
    }
    expect(telemetry.snapshot()).toEqual([
      expect.objectContaining({
        operation: "cache.decode",
        category: "shell",
        count: 1_000,
        totalMs: 500_500,
        maxMs: 1_000,
        slowCount: 985,
        recentSamples: 128,
        recentP50Ms: 936,
        recentP95Ms: 994,
        itemCount: 2_000,
        payloadChars: 20_000,
      }),
    ]);
  });

  it("limits persistent samples without losing failure and interruption counts", () => {
    let now = 0;
    const samples: unknown[] = [];
    const telemetry = createPerformanceTelemetry(
      () => now,
      (sample) => samples.push(sample),
    );
    telemetry.record({
      operation: "rpc.request",
      durationMs: 300,
      outcome: "success",
      rpcMethod: "server.getConfig",
    });
    telemetry.record({ operation: "rpc.request", durationMs: 1, outcome: "failure" });
    telemetry.record({ operation: "rpc.request", durationMs: 2, outcome: "interrupted" });
    expect(samples).toHaveLength(1);
    now = 10_000;
    telemetry.record({ operation: "rpc.request", durationMs: 400, outcome: "success" });
    expect(samples).toHaveLength(2);
    expect(telemetry.snapshot()[0]).toMatchObject({
      count: 4,
      failureCount: 1,
      interruptedCount: 1,
      slowCount: 2,
    });
  });

  it("retains startup milestones, omits extra data, and resets all summaries", () => {
    const samples: unknown[] = [];
    const telemetry = createPerformanceTelemetry(
      () => 1_000,
      (sample) => samples.push(sample),
    );
    const measurement = {
      operation: "startup.navigation-ready" as const,
      durationMs: 20,
      outcome: "success" as const,
      privateContent: "secret message",
    };
    telemetry.record(measurement);
    expect(samples).toHaveLength(1);
    expect(JSON.stringify(samples)).not.toContain("secret message");
    telemetry.record({ operation: "cache.read", durationMs: NaN, outcome: "success" });
    telemetry.record({ operation: "cache.read", durationMs: -1, outcome: "success" });
    expect(telemetry.snapshot()).toHaveLength(1);
    telemetry.clear();
    expect(telemetry.snapshot()).toEqual([]);
  });

  it("decodes retained measurements without accepting unknown fields or malformed values", () => {
    const sample = {
      timestamp: "2026-10-19T01:00:00.000Z",
      operation: "cache.read",
      category: "shell",
      durationMs: 51,
      outcome: "success",
      payloadChars: 100,
      privateContent: "secret",
    };
    expect(parsePerformanceSample(JSON.stringify(sample))).toEqual({
      timestamp: sample.timestamp,
      operation: "cache.read",
      category: "shell",
      durationMs: 51,
      outcome: "success",
      payloadChars: 100,
    });
    expect(parsePerformanceSample("{")).toBeNull();
    expect(parsePerformanceSample(JSON.stringify({ ...sample, operation: "secret" }))).toBeNull();
    expect(parsePerformanceSample(JSON.stringify({ ...sample, durationMs: -1 }))).toBeNull();
    expect(parsePerformanceSample(JSON.stringify({ ...sample, outcome: "secret" }))).toBeNull();
  });
});

it("measures foreground delays and excludes time spent suspended", () => {
  let now = 0;
  const measurements: number[] = [];
  const sampler = createJsDelaySampler(
    () => now,
    (delay) => measurements.push(delay),
  );
  sampler.start();
  now = 1_250;
  sampler.tick();
  expect(measurements).toEqual([250]);
  sampler.stop();
  now = 80_000;
  sampler.tick();
  expect(measurements).toHaveLength(1);
  sampler.start();
  now = 81_000;
  sampler.tick();
  expect(measurements).toEqual([250, 0]);
});
