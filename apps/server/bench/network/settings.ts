/** Shared by the workloads and suite manifest so recorded inputs are the ones actually used. */
export const workloadSettings = {
  pingBytes: 256,
  connect: { count: 3 },
  rpc: { count: 128, warmupCount: 1 },
  stream: { count: 128, bytes: 512, intervalMs: 10 },
  fanout: { subscriptions: 8, count: 64, bytes: 512, intervalMs: 10 },
  hol: {
    bulkBytes: 4 * 1024 * 1024,
    messages: 2000,
    textBytes: 2000,
    pingIntervalMs: 20,
    warmupCount: 1,
  },
} as const;

export const linkSettings = { mtu: 1500, queuePackets: 100000, offloads: false } as const;
export const rpcHeartbeatTimeout = "15 seconds";

export const rpcTextSettings = {
  vocabulary:
    "the a to of and in is it that for on with as this be are was by at or an from not can function return const value type error thread message server client stream request event update state render network latency packet change file test build agent provider session",
  multiplier: 1103515245,
  increment: 12345,
  modulus: 2147483648,
  createdAt: "2026-10-13T00:00:00.000Z",
} as const;
