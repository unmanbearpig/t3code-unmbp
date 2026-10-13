// @effect-diagnostics nodeBuiltinImport:off - Shells out to tc/iptables inside the bench netns.
// Network impairments applied to `lo` inside the bench's private network namespace. Every packet
// crosses `lo` once, so delays are one-way: `delay=25` is a 50ms round trip.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

import { linkSettings } from "./settings.ts";

export interface Scenario {
  readonly name: string;
  readonly delayMs?: number;
  /** Per-packet random variation. netem reorders packets when jitter exceeds their spacing. */
  readonly jitterMs?: number;
  /** netem loss spec: "2%" for random loss, "gemodel 1% 25%" for Gilbert-Elliott bursts. */
  readonly loss?: string;
  /** Silently drop every Nth packet (counted across both directions). */
  readonly nthDrop?: number;
  /** Add `extraMs` to every Nth packet; the others overtake it. */
  readonly nthDelay?: { readonly every: number; readonly extraMs: number };
}

export const presets: ReadonlyArray<Scenario> = [
  { name: "clean" },
  { name: "wan", delayMs: 25 },
  { name: "high-latency", delayMs: 150 },
  { name: "mobile", delayMs: 50, jitterMs: 15, loss: "1%" },
  { name: "loss2", delayMs: 25, loss: "2%" },
  { name: "loss5", delayMs: 25, loss: "5%" },
  { name: "burst", delayMs: 25, loss: "gemodel 1% 25%" },
  { name: "nth3-delay300", delayMs: 25, nthDelay: { every: 3, extraMs: 300 } },
  { name: "nth20-drop", delayMs: 25, nthDrop: 20 },
];

/** Parses a preset name or `name:delay=25,jitter=5,loss=2%,nthDrop=10,nthDelay=3/300`. */
export function parseScenario(spec: string): Scenario {
  const preset = presets.find((scenario) => scenario.name === spec);
  if (preset) return preset;
  const [name, params, extra] = spec.split(":");
  if (!name || !params || extra !== undefined) {
    throw new Error(
      `Unknown scenario "${spec}". Presets: ${presets.map((p) => p.name).join(", ")}`,
    );
  }
  const scenario: { -readonly [Key in keyof Scenario]: Scenario[Key] } = { name };
  for (const param of params.split(",")) {
    const [key, value = "", extraValue] = param.split("=");
    if (!value || extraValue !== undefined) throw new Error(`Invalid parameter "${param}"`);
    switch (key) {
      case "delay":
        scenario.delayMs = Number(value);
        break;
      case "jitter":
        scenario.jitterMs = Number(value);
        break;
      case "loss":
        scenario.loss = value.endsWith("%") ? value : `${value}%`;
        break;
      case "nthDrop":
        scenario.nthDrop = Number(value);
        break;
      case "nthDelay": {
        const parts = value.split("/");
        if (parts.length !== 2) throw new Error(`Invalid nthDelay "${value}"; use N/ms`);
        const [every, extraMs] = parts.map(Number);
        scenario.nthDelay = { every: every!, extraMs: extraMs! };
        break;
      }
      default:
        throw new Error(`Unknown scenario parameter "${key}" in "${spec}"`);
    }
  }
  for (const [key, value] of [
    ["delay", scenario.delayMs],
    ["jitter", scenario.jitterMs],
    ["extraMs", scenario.nthDelay?.extraMs],
  ] as const) {
    if (value !== undefined && (!Number.isFinite(value) || value < 0))
      throw new Error(`Invalid ${key} in "${spec}"`);
  }
  for (const value of [scenario.nthDrop, scenario.nthDelay?.every]) {
    if (value !== undefined && (!Number.isInteger(value) || value < 1))
      throw new Error(`Packet interval must be a positive integer in "${spec}"`);
  }
  if (
    scenario.loss !== undefined &&
    (!/^\d+(?:\.\d+)?%$/.test(scenario.loss) || Number(scenario.loss.slice(0, -1)) > 100)
  ) {
    throw new Error(`Loss must be between 0% and 100% in "${spec}"`);
  }
  return scenario;
}

/** Presets are comma-separated; commas before key=value stay inside a custom scenario. */
export function parseScenarios(spec: string): ReadonlyArray<Scenario> {
  const entries: string[] = [];
  for (const part of spec.split(",")) {
    if (part.includes("=") && !part.includes(":") && entries.length > 0) {
      entries[entries.length - 1] += `,${part}`;
    } else entries.push(part);
  }
  return entries.map(parseScenario);
}

export function describeScenario(scenario: Scenario) {
  const parts = [
    scenario.delayMs !== undefined && `${scenario.delayMs * 2}ms RTT`,
    scenario.jitterMs !== undefined && `±${scenario.jitterMs}ms jitter`,
    scenario.loss !== undefined && `loss ${scenario.loss}`,
    scenario.nthDrop !== undefined && `drop one packet in every ${scenario.nthDrop}`,
    scenario.nthDelay !== undefined &&
      `+${scenario.nthDelay.extraMs}ms on one packet in every ${scenario.nthDelay.every}`,
  ].filter(Boolean);
  return parts.length === 0 ? "no impairment" : parts.join(", ");
}

// iptables-legacy locks /run/xtables.lock, which an unprivileged namespace cannot open.
let lockfile: string | undefined;

function assertIsolated() {
  if (
    !process.env.NETBENCH_HOST_NETNS ||
    NodeFS.readlinkSync("/proc/self/ns/net") === process.env.NETBENCH_HOST_NETNS
  ) {
    throw new Error("Refusing to impair the host network; use run.ts to enter a private namespace");
  }
}
const sh = (
  command: string,
  args: ReadonlyArray<string>,
  options: { allowFailure?: boolean } = {},
) => {
  try {
    NodeChildProcess.execFileSync(command, args, {
      env: { ...process.env, XTABLES_LOCKFILE: lockfile },
      stdio: ["ignore", "ignore", "pipe"],
    });
  } catch (error) {
    if (!options.allowFailure) throw error;
  }
};

/** One-time namespace setup: realistic MTU and no offloads, so netem sees real-sized packets. */
export function prepareLoopback(scratchDir: string) {
  assertIsolated();
  lockfile = NodePath.join(scratchDir, "xtables.lock");
  sh("ip", ["link", "set", "lo", "up", "mtu", String(linkSettings.mtu)]);
  sh("ethtool", ["-K", "lo", "tso", "off", "gso", "off", "gro", "off"]);
  // Without this Linux reuses cwnd/ssthresh learned by the previous run against the same address.
  sh("sysctl", ["-qw", "net.ipv4.tcp_no_metrics_save=1"], { allowFailure: true });
}

export function applyScenario(scenario: Scenario, seed?: number) {
  assertIsolated();
  sh("tc", ["qdisc", "del", "dev", "lo", "root"], { allowFailure: true });
  sh("iptables", ["-F", "INPUT"]);
  sh("iptables", ["-t", "mangle", "-F", "OUTPUT"]);
  sh("ip", ["tcp_metrics", "flush", "all"], { allowFailure: true });

  const netem = (extraMs: number) => {
    const delay = (scenario.delayMs ?? 0) + extraMs;
    return [
      ...(delay > 0 || scenario.jitterMs ? ["delay", `${delay}ms`] : []),
      ...(scenario.jitterMs ? [`${scenario.jitterMs}ms`] : []),
      ...(scenario.loss ? ["loss", ...scenario.loss.split(" ")] : []),
      // netem's default 1000-packet queue overflows on a 4MB burst and adds loss nobody asked for.
      "limit",
      String(linkSettings.queuePackets),
      ...(seed !== undefined ? ["seed", String(seed + (extraMs > 0 ? 1 : 0))] : []),
    ];
  };

  if (scenario.nthDelay) {
    // Band 1:2 carries normal traffic; packets marked 7 go to 1:3 with the extra delay.
    sh("tc", [
      "qdisc",
      "add",
      "dev",
      "lo",
      "root",
      "handle",
      "1:",
      "prio",
      "bands",
      "3",
      "priomap",
      ...Array(16).fill("1"),
    ]);
    sh("tc", ["qdisc", "add", "dev", "lo", "parent", "1:2", "handle", "20:", "netem", ...netem(0)]);
    sh("tc", [
      "qdisc",
      "add",
      "dev",
      "lo",
      "parent",
      "1:3",
      "handle",
      "30:",
      "netem",
      ...netem(scenario.nthDelay.extraMs),
    ]);
    sh("tc", [
      "filter",
      "add",
      "dev",
      "lo",
      "parent",
      "1:",
      "protocol",
      "ip",
      "handle",
      "7",
      "fw",
      "flowid",
      "1:3",
    ]);
    sh("iptables", [
      "-t",
      "mangle",
      "-A",
      "OUTPUT",
      "-o",
      "lo",
      "-m",
      "statistic",
      "--mode",
      "nth",
      "--every",
      String(scenario.nthDelay.every),
      "--packet",
      String(scenario.nthDelay.every - 1),
      "-j",
      "MARK",
      "--set-mark",
      "7",
    ]);
  } else if (scenario.delayMs || scenario.jitterMs || scenario.loss) {
    sh("tc", ["qdisc", "add", "dev", "lo", "root", "handle", "1:", "netem", ...netem(0)]);
  }

  if (scenario.nthDrop) {
    // Dropped on INPUT so the sender cannot tell; an OUTPUT drop returns EPERM to the socket.
    sh("iptables", [
      "-A",
      "INPUT",
      "-i",
      "lo",
      "-m",
      "statistic",
      "--mode",
      "nth",
      "--every",
      String(scenario.nthDrop),
      "--packet",
      String(scenario.nthDrop - 1),
      "-j",
      "DROP",
    ]);
  }
}
