import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

const mocks = vi.hoisted(() => ({
  listeners: new Map<string, (state: string) => void>(),
  entries: [] as Array<{ timestamp: number; level: string; message: string }>,
  read: vi.fn(),
}));
vi.mock("react-native", () => ({
  Platform: { OS: "android" },
  AppState: {
    currentState: "active",
    addEventListener: (event: string, listener: (state: string) => void) => {
      mocks.listeners.set(event, listener);
      return { remove: () => mocks.listeners.delete(event) };
    },
  },
}));
vi.mock("expo-device", () => ({
  osVersion: "16",
  modelName: "test-device",
  totalMemory: 4_000_000_000,
}));
vi.mock("./diagnostic-log", () => ({
  readDiagnosticLog: mocks.read,
  recordDiagnosticLog: (level: string, message: string) => {
    mocks.entries.unshift({ timestamp: Date.now(), level, message });
  },
}));

let stop: (() => void) | undefined;

beforeEach(() => {
  vi.resetModules();
  vi.useFakeTimers();
  mocks.listeners.clear();
  mocks.entries.length = 0;
  mocks.read.mockReset().mockImplementation(() => mocks.entries);
});
afterEach(() => {
  stop?.();
  stop = undefined;
  vi.useRealTimers();
});

describe("native performance diagnostics", () => {
  it("records a startup milestone once even if navigation remounts", async () => {
    const telemetry = await import("./performance-telemetry");
    telemetry.recordStartupMilestone("startup.navigation-ready");
    vi.advanceTimersByTime(30_000);
    telemetry.recordStartupMilestone("startup.navigation-ready");
    expect(telemetry.performanceTelemetry.snapshot()[0]).toMatchObject({ count: 1 });
    expect(mocks.entries).toHaveLength(1);
  });

  it("stops sampling in the background and disposes every listener and timer", async () => {
    const telemetry = await import("./performance-telemetry");
    stop = telemetry.startPerformanceMonitoring();
    vi.advanceTimersByTime(3_000);
    expect(
      telemetry.performanceTelemetry
        .snapshot()
        .find((metric) => metric.operation === "runtime.js-delay")?.count,
    ).toBe(3);
    mocks.listeners.get("change")?.("background");
    vi.advanceTimersByTime(60_000);
    expect(telemetry.performanceTelemetry.snapshot()[0]?.count).toBe(3);
    mocks.listeners.get("change")?.("active");
    vi.advanceTimersByTime(1_000);
    const metrics = telemetry.performanceTelemetry.snapshot();
    expect(metrics.find((metric) => metric.operation === "runtime.js-delay")).toMatchObject({
      count: 4,
      maxMs: 0,
    });
    expect(metrics.find((metric) => metric.operation === "lifecycle.resume-js")?.count).toBe(1);
    stop();
    expect(mocks.listeners.size).toBe(0);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("includes retained measurements after restart and current device context", async () => {
    const first = await import("./performance-telemetry");
    first.performanceTelemetry.record({
      operation: "cache.decode",
      category: "shell",
      durationMs: 75,
      outcome: "success",
      payloadChars: 2_000,
    });
    expect(first.readPerformanceReport().summary).toHaveLength(1);
    vi.resetModules();
    const second = await import("./performance-telemetry");
    const report = second.readPerformanceReport();
    expect(report.summary).toEqual([]);
    expect(report.recentSlowSamples).toMatchObject([
      { operation: "cache.decode", durationMs: 75, payloadChars: 2_000 },
    ]);
    expect(report.device).toEqual({
      platform: "android",
      osVersion: "16",
      model: "test-device",
      totalMemoryBytes: 4_000_000_000,
    });
    const { formatConnectionLog } = await import("./connection-log-model");
    const exported = JSON.parse(
      formatConnectionLog([], { version: "2.0.0", build: "1", platform: "android" }, report),
    );
    expect(exported.version).toBe(4);
    expect(exported.records).toEqual([]);
    expect(exported.performance.recentSlowSamples).toEqual(report.recentSlowSamples);
  });

  it("keeps summaries usable when persistence cannot be read", async () => {
    const telemetry = await import("./performance-telemetry");
    telemetry.performanceTelemetry.record({
      operation: "rpc.request",
      durationMs: 10,
      outcome: "success",
    });
    mocks.read.mockImplementation(() => {
      throw new Error("storage unavailable");
    });
    expect(telemetry.readPerformanceReport()).toMatchObject({
      summary: [{ operation: "rpc.request", count: 1 }],
      recentSlowSamples: [],
    });
  });
});
