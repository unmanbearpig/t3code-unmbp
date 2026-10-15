// @effect-diagnostics nodeBuiltinImport:off - Networking changes are guarded by the private namespace identity.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
export function assertIsolated() {
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
      stdio: ["ignore", "ignore", "pipe"],
    });
  } catch (error) {
    if (!options.allowFailure) throw error;
  }
};

/** One-time namespace setup: realistic MTU and no offloads, so netem sees real-sized packets. */
export const operationPacketSettings = {
  point: "loopback-ingress-after-segmentation",
  device: "ifb-netbench",
  offloads: ["tso", "gso", "gro", "tx-udp-segmentation", "tx-gso-list"],
} as const;

export function prepareLoopback(mtu: number) {
  assertIsolated();
  sh("ip", ["link", "set", "lo", "up", "mtu", String(mtu)]);
  sh("ethtool", [
    "-K",
    "lo",
    "tso",
    "off",
    "gso",
    "off",
    "gro",
    "off",
    "tx-udp-segmentation",
    "off",
    "tx-gso-list",
    "off",
  ]);
  sh("ip", ["link", "add", operationPacketSettings.device, "type", "ifb"]);
  sh("ip", ["link", "set", operationPacketSettings.device, "up", "mtu", String(mtu)]);
  sh("ethtool", ["-K", operationPacketSettings.device, "tso", "off", "gso", "off", "gro", "off"]);
  sh("sysctl", ["-qw", "net.ipv4.tcp_no_metrics_save=1"]);
}
