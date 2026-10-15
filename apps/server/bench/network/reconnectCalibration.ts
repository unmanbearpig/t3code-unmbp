// @effect-diagnostics nodeBuiltinImport:off - Calibration is separate from the measured Node event loop.
import * as NodeChildProcess from "node:child_process";
import * as NodeReadline from "node:readline";
import * as Schema from "effect/Schema";
import { applyProfile, directionCommands } from "./reconnectNetwork.ts";
import { profiles, type Manifest, type Profile, type ReconnectCase } from "./reconnectManifest.ts";
import { median } from "./reconnectComparison.ts";
import { startController, stopChild } from "./reconnectProcess.ts";
const EchoResult = Schema.Struct({
  type: Schema.Literal("result"),
  sent: Schema.Number,
  echoes: Schema.Array(
    Schema.Struct({
      number: Schema.Number,
      sentMs: Schema.Number,
      receivedMs: Schema.Number,
      rttMs: Schema.Number,
    }),
  ),
});
const BulkResult = Schema.Struct({
  type: Schema.Literal("result"),
  direction: Schema.String,
  bytes: Schema.Number,
  seconds: Schema.Number,
  kbitPerSecond: Schema.Number,
});
const decodeEcho = Schema.decodeUnknownSync(Schema.fromJsonString(EchoResult));
const decodeBulk = Schema.decodeUnknownSync(Schema.fromJsonString(BulkResult));
const script = new URL("./reconnectCalibration.py", import.meta.url).pathname;
function python(args: string[]) {
  const child = NodeChildProcess.spawn("python3", [script, ...args], {
    stdio: ["pipe", "pipe", "inherit"],
  });
  const lines = NodeReadline.createInterface({ input: child.stdout });
  const queue: string[] = [];
  let consume: ((line: string) => void) | undefined;
  lines.on("line", (line) => {
    if (consume) {
      const f = consume;
      consume = undefined;
      f(line);
    } else queue.push(line);
  });
  return {
    child,
    next: () =>
      new Promise<string>((resolve, reject) => {
        const queued = queue.shift();
        if (queued) resolve(queued);
        else {
          consume = resolve;
          child.once("error", reject);
          child.once("exit", (code) => {
            if (code !== 0) reject(new Error(`Calibration process exited ${code}`));
          });
        }
      }),
  };
}
export function validateEcho(profile: Profile, result: typeof EchoResult.Type) {
  const errors: string[] = [];
  const echoes = result.echoes;
  if (!echoes.length) return ["No numbered echoes received"];
  if (echoes.some((e, i) => i > 0 && e.number <= echoes[i - 1]!.number))
    errors.push("Packet reordering");
  const ordinary = echoes.filter(
    (e) => profile.schedule.length === 0 || e.sentMs > (profile.schedule.at(-1)?.atMs ?? 0) + 2200,
  );
  const expectedRtt = profile.down.delayMs + profile.up.delayMs;
  const rtt = median(ordinary.map((e) => e.rttMs));
  if (rtt === null || Math.abs(rtt - expectedRtt) > Math.max(15, expectedRtt * 0.3))
    errors.push(`Base RTT ${rtt}ms differs from ${expectedRtt}ms`);
  if (profile.name === "latency-spikes" && !echoes.some((e) => e.rttMs > 800))
    errors.push("Latency spike did not hold packets for roughly one second");
  if (profile.name.startsWith("slow-start")) {
    const outageEnd = profile.schedule.at(-1)!.atMs;
    if (echoes.some((e) => e.sentMs > 30 && e.sentMs < outageEnd - 30))
      errors.push("Outage leaked echo packets");
    if (!echoes.some((e) => e.sentMs >= outageEnd + 30)) errors.push("No recovery after outage");
  } else if (!profile.stochastic && echoes.length !== result.sent)
    errors.push("Unexpected packet loss");
  return errors;
}
export async function calibrate(manifest: Manifest, report: (message: string) => void) {
  const server = python(["server"]);
  const echoRows = [];
  const bulkRows = [];
  try {
    await server.next();
    for (const profile of profiles) {
      report(`calibrating ${profile.name}`);
      applyProfile(profile, 104729, manifest.queuePackets);
      const entry: ReconnectCase = {
        id: "calibration",
        profile,
        scheme: "http",
        routes: 1,
        workload: "idle",
        seed: 104729,
        phaseMs: 0,
        timeoutMs: 60000,
        repetition: 1,
      };
      const controller = await startController(entry, manifest.queuePackets);
      const echo = python([
        "echo",
        String(Math.max(3200, (profile.schedule.at(-1)?.atMs ?? 0) + 3000)),
      ]);
      try {
        await echo.next();
        const signal = { startNs: process.hrtime.bigint().toString() };
        controller.start();
        echo.child.stdin.end(JSON.stringify(signal) + "\n");
        const result = decodeEcho(await echo.next());
        await controller.done;
        const errors = validateEcho(profile, result);
        echoRows.push({
          profile: profile.name,
          ...result,
          errors,
          controller: controller.records,
          configuration: directionCommands(profile, entry.seed, manifest.queuePackets),
        });
      } finally {
        await stopChild(echo.child);
        await stopChild(controller.child);
      }
    }
    // Test rates in isolation from jitter, loss, and scheduled changes, once for each configured cap.
    const constrained = profiles.find((p) => p.name === "constrained")!;
    for (const direction of ["down", "up"] as const) {
      report(`calibrating constrained ${direction} throughput`);
      applyProfile(
        {
          ...constrained,
          down: { ...constrained.down, jitterMs: 0 },
          up: { ...constrained.up, jitterMs: 0 },
        },
        104729,
        manifest.queuePackets,
      );
      const bulk = python(["bulk", direction]);
      try {
        const result = decodeBulk(await bulk.next());
        const configured = constrained[direction].rateKbit;
        const ratio = result.kbitPerSecond / configured;
        bulkRows.push({
          ...result,
          configuredKbit: configured,
          ratio,
          errors:
            ratio < 0.65 || ratio > 1.08 ? ["Directional throughput outside 65-108% of cap"] : [],
        });
      } finally {
        await stopChild(bulk.child);
      }
    }
    return {
      correct: echoRows.every((r) => !r.errors.length) && bulkRows.every((r) => !r.errors.length),
      echoes: echoRows,
      throughput: bulkRows,
    };
  } finally {
    await stopChild(server.child);
  }
}
