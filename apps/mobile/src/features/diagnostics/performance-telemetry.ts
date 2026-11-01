import type { PerformanceTelemetry } from "@t3tools/client-runtime/platform";
import * as Device from "expo-device";
import { AppState, Platform } from "react-native";

import { readDiagnosticLog, recordDiagnosticLog } from "./diagnostic-log";
import { mobileJsLaunch } from "./launch-clock";
import {
  createJsDelaySampler,
  createPerformanceTelemetry,
  parsePerformanceSample,
  SLOW_THRESHOLDS_MS,
} from "./performance-model";

export const performanceTelemetry = createPerformanceTelemetry(Date.now, (sample) => {
  recordDiagnosticLog("performance", JSON.stringify(sample));
});
const startupMilestones = new Set<string>();

/** Startup milestones measure JS entry through navigation readiness or the splash hide request. */
export function recordStartupMilestone(
  operation: "startup.navigation-ready" | "startup.splash-hide-requested",
) {
  if (startupMilestones.has(operation)) return;
  startupMilestones.add(operation);
  performanceTelemetry.record({
    operation,
    durationMs: Math.max(0, performance.now() - mobileJsLaunch.monotonic),
    outcome: "success",
  });
}

export function readPerformanceReport() {
  let recentSlowSamples: ReturnType<typeof parsePerformanceSample>[] = [];
  try {
    recentSlowSamples = readDiagnosticLog()
      .filter((entry) => entry.level === "performance")
      .slice(0, 100)
      .map((entry) => parsePerformanceSample(entry.message));
  } catch {
    // Current-process aggregates remain available if persistent storage fails.
  }
  return {
    version: 1,
    slowThresholdsMs: SLOW_THRESHOLDS_MS,
    timingOrigins: {
      startup: "JavaScript entry, excludes native process startup",
      resume: "AppState active callback to next JS timer",
      jsDelay: "Foreground JS timer delay, not UI frame time",
      applyBatch: "Elapsed batch application, excludes waiting for its lock",
      percentiles: "Most recent 128 samples per operation/category in this process",
    },
    sessionStartedAt: new Date(mobileJsLaunch.wall).toISOString(),
    device: {
      platform: Platform.OS,
      osVersion: Device.osVersion,
      model: Device.modelName,
      totalMemoryBytes: Device.totalMemory,
    },
    summary: performanceTelemetry.snapshot(),
    recentSlowSamples: recentSlowSamples.filter((sample) => sample !== null),
  };
}

export type PerformanceReport = ReturnType<typeof readPerformanceReport>;

/** One foreground timer measures JS scheduling delays; no frame callbacks or background polling. */
export function startPerformanceMonitoring() {
  let timer: ReturnType<typeof setInterval> | undefined;
  let resumeTimer: ReturnType<typeof setTimeout> | undefined;
  const sampler = createJsDelaySampler(performance.now.bind(performance), (durationMs) => {
    performanceTelemetry.record({ operation: "runtime.js-delay", durationMs, outcome: "success" });
  });
  const stop = () => {
    clearInterval(timer);
    clearTimeout(resumeTimer);
    timer = undefined;
    resumeTimer = undefined;
    sampler.stop();
  };
  const start = (resume: boolean) => {
    stop();
    sampler.start();
    timer = setInterval(sampler.tick, 1_000);
    if (resume) {
      const started = performance.now();
      resumeTimer = setTimeout(() => {
        performanceTelemetry.record({
          operation: "lifecycle.resume-js",
          durationMs: performance.now() - started,
          outcome: "success",
        });
      }, 0);
    }
  };
  if (AppState.currentState === "active") start(false);
  const activity = AppState.addEventListener("change", (state) => {
    if (state === "active") start(true);
    else stop();
  });
  const memory = AppState.addEventListener("memoryWarning", () => {
    performanceTelemetry.record({
      operation: "runtime.memory-warning",
      durationMs: 0,
      outcome: "success",
    });
  });
  return () => {
    stop();
    activity.remove();
    memory.remove();
  };
}

export const performanceObserver: PerformanceTelemetry.PerformanceObserver = {
  record: performanceTelemetry.record,
};
