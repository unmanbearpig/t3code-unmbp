// @effect-diagnostics nodeBuiltinImport:off - tc runs only in the guarded private namespace.
import * as NodeChildProcess from "node:child_process";
import { assertIsolated, operationPacketSettings } from "./impairments.ts";
import {
  validateSchedule,
  type Direction,
  type Profile,
  type ScheduleEvent,
} from "./reconnectManifest.ts";

const device = operationPacketSettings.device;
export const serverPort = 4410;
export function netemArgs(direction: Direction, seed: number, queuePackets: number, spike = false) {
  return [
    "limit",
    String(queuePackets),
    "delay",
    `${spike ? 1000 : direction.delayMs}ms`,
    ...(direction.jitterMs ? [`${direction.jitterMs}ms`] : []),
    ...(direction.loss ? ["loss", ...direction.loss.split(" ")] : []),
    "rate",
    `${direction.rateKbit}kbit`,
    "seed",
    String(seed),
  ];
}
/** Source port identifies downloads, including ACKs in that direction; the default band is upload. */
export function directionCommands(
  profile: Profile,
  seed: number,
  queuePackets: number,
  port = serverPort,
) {
  return [
    `qdisc add dev ${device} root handle 1: prio bands 2 priomap ${Array(16).fill(1).join(" ")}`,
    ...(
      [
        ["down", 1, 20],
        ["up", 2, 30],
      ] as const
    ).flatMap(([direction, band, handle], i) => [
      `qdisc add dev ${device} parent 1:${band} handle ${handle}: netem ${netemArgs(profile[direction], seed + i, queuePackets).join(" ")}`,
      // netem's default tfifo sorts by delivery timestamp. pfifo holds insertion order,
      // including across a delay decrease when a spike ends.
      `qdisc add dev ${device} parent ${handle}:1 handle ${handle + 1}: pfifo limit ${queuePackets}`,
    ]),
    `filter add dev ${device} parent 1: protocol ip pref 10 u32 match ip sport ${port} 0xffff flowid 1:1`,
    `filter add dev lo ingress protocol ip pref 30 matchall action mirred egress redirect dev ${device}`,
  ];
}
export function applyProfile(profile: Profile, seed: number, queuePackets: number) {
  assertIsolated();
  for (const args of [
    ["qdisc", "del", "dev", "lo", "clsact"],
    ["qdisc", "del", "dev", device, "root"],
  ]) {
    NodeChildProcess.spawnSync("tc", args, { stdio: "ignore" });
  }
  // Some kernels restrict the metrics flush even in a user-owned netns.
  // prepareLoopback sets tcp_no_metrics_save=1, so new connections do not reuse learned metrics.
  NodeChildProcess.spawnSync("ip", ["tcp_metrics", "flush", "all"], { stdio: "ignore" });
  NodeChildProcess.execFileSync("tc", ["qdisc", "add", "dev", "lo", "clsact"]);
  NodeChildProcess.execFileSync("tc", ["-batch", "-"], {
    input: directionCommands(profile, seed, queuePackets).join("\n") + "\n",
    stdio: ["pipe", "ignore", "pipe"],
  });
}
export function scheduleCommands(
  event: ScheduleEvent,
  profile: Profile,
  seed: number,
  queuePackets: number,
) {
  switch (event.action) {
    case "outage-on":
      return ["filter add dev lo ingress protocol ip pref 1 matchall action drop"];
    case "outage-off":
      return ["filter del dev lo ingress protocol ip pref 1"];
    case "spike-on":
    case "spike-off":
      return (
        [
          ["down", 20],
          ["up", 30],
        ] as const
      ).map(
        ([direction, handle], i) =>
          `qdisc change dev ${device} parent 1:${i + 1} handle ${handle}: netem ${netemArgs(profile[direction], seed + i, queuePackets, event.action === "spike-on").join(" ")}`,
      );
  }
}
export function controllerPlan(
  profile: Profile,
  seed: number,
  queuePackets: number,
  phaseMs: number,
) {
  validateSchedule(profile.schedule);
  return profile.schedule.map((event) => ({
    ...event,
    atMs: event.atMs + phaseMs,
    commands: scheduleCommands(event, profile, seed, queuePackets),
  }));
}
