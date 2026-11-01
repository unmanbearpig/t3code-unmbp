// @effect-diagnostics nodeBuiltinImport:off globalTimers:off - Optional benchmark observers measure their own Node process.
import * as NodeOS from "node:os";
import * as NodePerfHooks from "node:perf_hooks";
import * as Schema from "effect/Schema";

export const ResourceCommand = Schema.Struct({
  _tag: Schema.Literals(["netbench-resource-start", "netbench-resource-stop"]),
});
export const ResourceResponse = Schema.TaggedStruct("netbench-resource-result", {
  metrics: Schema.Record(Schema.String, Schema.Number),
});

const isResourceCommand = Schema.is(ResourceCommand);

const hostCpu = () =>
  NodeOS.cpus().reduce(
    (total, cpu) => ({
      idle: total.idle + cpu.times.idle,
      total: total.total + Object.values(cpu.times).reduce((sum, time) => sum + time, 0),
    }),
    { idle: 0, total: 0 },
  );

/** CPU includes native threads. RSS peaks are sampled, rather than process-lifetime high water. */
export function startProcessMeasurement() {
  const startedAt = NodePerfHooks.performance.now();
  const cpu = process.cpuUsage();
  const utilization = NodePerfHooks.performance.eventLoopUtilization();
  const host = hostCpu();
  const load = NodeOS.loadavg()[0]!;
  const memory = process.memoryUsage();
  let peakRss = memory.rss;
  let peakHeap = memory.heapUsed;
  const sample = () => {
    const current = process.memoryUsage();
    peakRss = Math.max(peakRss, current.rss);
    peakHeap = Math.max(peakHeap, current.heapUsed);
    return current;
  };
  const histogram = NodePerfHooks.monitorEventLoopDelay({ resolution: 10 });
  histogram.enable();
  const timer = setInterval(sample, 50);
  timer.unref();
  let result: Record<string, number> | undefined;
  return {
    stop() {
      if (result) return result;
      clearInterval(timer);
      histogram.disable();
      const elapsed = NodePerfHooks.performance.now() - startedAt;
      const used = process.cpuUsage(cpu);
      const loop = NodePerfHooks.performance.eventLoopUtilization(utilization);
      const after = sample();
      const hostAfter = hostCpu();
      const hostTotal = hostAfter.total - host.total;
      result = {
        wallMs: elapsed,
        cpuUserMs: used.user / 1000,
        cpuSystemMs: used.system / 1000,
        rssStartBytes: memory.rss,
        rssEndBytes: after.rss,
        rssSampledPeakBytes: peakRss,
        heapUsedStartBytes: memory.heapUsed,
        heapUsedEndBytes: after.heapUsed,
        heapUsedSampledPeakBytes: peakHeap,
        memorySampleIntervalMs: 50,
        eventLoopUtilization: loop.utilization,
        eventLoopDelayResolutionMs: 10,
        eventLoopDelaySamples: histogram.count,
        hostLoad1Start: load,
        hostLoad1End: NodeOS.loadavg()[0]!,
        ...(hostTotal > 0
          ? { hostCpuBusyPercent: 100 * (1 - (hostAfter.idle - host.idle) / hostTotal) }
          : {}),
        ...(histogram.count > 0
          ? {
              eventLoopDelayMaxMs: histogram.max / 1e6,
              eventLoopDelayMedianMs: histogram.percentile(50) / 1e6,
            }
          : {}),
        ...(histogram.count >= 100
          ? { eventLoopDelayObservedP99Ms: histogram.percentile(99) / 1e6 }
          : {}),
      };
      return result;
    },
  };
}

/** Resource commands share fixture IPC without entering the application-control request queue. */
export function makeResourceControl() {
  let measurement: ReturnType<typeof startProcessMeasurement> | undefined;
  return (message: unknown) => {
    if (!isResourceCommand(message)) return false;
    const metrics = measurement?.stop() ?? {};
    measurement =
      message._tag === "netbench-resource-start" ? startProcessMeasurement() : undefined;
    process.send?.({ _tag: "netbench-resource-result", metrics });
    return true;
  };
}
