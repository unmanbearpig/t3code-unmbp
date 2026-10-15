import type { ReconnectCase } from "./reconnectManifest.ts";

/** State transitions are explicit so setup cannot accidentally enter the measured window. */
export function workloadPlan(entry: ReconnectCase) {
  const warm = entry.workload.startsWith("warm-") || entry.workload === "pre-active-retry";
  const keep = entry.workload === "healthy-slow" || entry.workload === "refocus";
  return {
    warm,
    keep,
    establishOld:
      warm || keep || entry.workload === "short-probe" || entry.workload === "network-changed",
    oldSocket:
      entry.workload === "warm-closed" || entry.workload === "pre-active-retry"
        ? "closed"
        : entry.workload === "warm-half-open" ||
            entry.workload === "short-probe" ||
            entry.workload === "network-changed"
          ? "half-open"
          : "healthy",
    simulatedBackgroundMs: warm ? 180_000 : entry.workload === "refocus" ? 0 : 1_000,
    addedMessages: entry.workload === "idle" || entry.workload === "warm-healthy" || keep ? 0 : 20,
    wakeup:
      entry.workload === "network-changed"
        ? ("network-changed" as const)
        : keep || entry.workload === "short-probe"
          ? ("application-active-probe" as const)
          : ("application-active-reconnect" as const),
    preActive: entry.workload === "pre-active-retry",
  };
}
