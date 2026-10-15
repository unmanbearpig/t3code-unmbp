// @effect-diagnostics nodeBuiltinImport:off - The suite fingerprint is computed outside measured workloads.
import * as NodeCrypto from "node:crypto";

import { transportNames } from "./client.ts";
import { operationPacketSettings, presets, type Scenario } from "./impairments.ts";
import { quicFlowControl } from "./protocol.ts";
import { rpcTransportNames } from "./rpcClient.ts";
import { operationNames, operationSettings } from "./operationsFixture.ts";
import { http3ProxySettings } from "./http3Proxy.ts";
import {
  linkSettings,
  rpcHeartbeatTimeout,
  rpcTextSettings,
  workloadSettings,
} from "./settings.ts";

export const workloadNames = ["connect", "rpc", "stream", "fanout", "hol"] as const;
export type WorkloadName = (typeof workloadNames)[number];

/** Increment the version when changing inputs or measurement semantics. */
export const suiteDefinition = {
  version: "network-v1",
  scenarios: presets,
  layers: [
    { name: "raw", transports: transportNames },
    { name: "rpc", transports: rpcTransportNames },
  ],
  workloads: workloadNames,
  workloadSettings,
  seeds: [104729, 104729, 104729],
  timeoutMs: { connect: 15000, rpc: 50000, stream: 50000, fanout: 30000, hol: 30000 },
  linkSettings,
  quicFlowControl,
  rpcHeartbeatTimeout,
  serialization: {
    raw: "binary-v1",
    rpcWebSocket: "json",
    rpcWebTransport: "length-prefixed-json-v1",
  },
  compression: { ws: false, wss: false, "rpc-ws": true, "rpc-ws-raw": false, webTransport: false },
  fixture: {
    raw: "zero-bytes",
    rpc: "repeated-vocabulary-text-v1",
    snapshotTextRepeats: true,
    rpcTextSettings,
  },
  order: "layer,scenario,workload,seed,rotated-transport",
} as const;

export interface BenchCase {
  readonly layer: "raw" | "rpc" | "operations";
  readonly scenario: Scenario;
  readonly workload: string;
  readonly transport: string;
  readonly repetition: number;
  readonly seed?: number | undefined;
  readonly timeoutMs: number;
  readonly httpPool?: "cold" | "warm";
  readonly packetPhase?: number;
}

export const caseId = (
  entry: Omit<BenchCase, "scenario"> & { readonly scenario: string | Scenario },
) =>
  [
    entry.layer,
    typeof entry.scenario === "string" ? entry.scenario : entry.scenario.name,
    entry.workload,
    entry.transport,
    entry.repetition,
  ].join("/") +
  (entry.httpPool === "warm" ? "/warm" : "") +
  (entry.packetPhase !== undefined ? `/phase-${entry.packetPhase}` : "");

/** The complete ordered matrix is saved, so missing or duplicated cases cannot look complete. */
export function makeSuitePlan() {
  const cases: BenchCase[] = [];
  for (const layer of suiteDefinition.layers) {
    for (const scenario of suiteDefinition.scenarios) {
      for (const workload of suiteDefinition.workloads) {
        for (const [index, seed] of suiteDefinition.seeds.entries()) {
          for (let offset = 0; offset < layer.transports.length; offset++) {
            cases.push({
              layer: layer.name,
              scenario,
              workload,
              transport: layer.transports[(index + offset) % layer.transports.length]!,
              repetition: index + 1,
              seed,
              timeoutMs: suiteDefinition.timeoutMs[workload],
            });
          }
        }
      }
    }
  }
  const definition = { ...suiteDefinition, cases };
  return {
    hash: NodeCrypto.createHash("sha256").update(JSON.stringify(definition)).digest("hex"),
    ...definition,
  };
}

/** Thread operations run only over RPC transports; raw frames have no application sync semantics. */
export function makeOperationSuitePlan() {
  const cases: BenchCase[] = [];
  for (const scenario of presets) {
    for (const workload of operationNames) {
      for (let index = 0; index < 3; index++) {
        for (let offset = 0; offset < rpcTransportNames.length; offset++) {
          cases.push({
            layer: "operations",
            scenario,
            workload,
            transport: rpcTransportNames[(index + offset) % rpcTransportNames.length]!,
            repetition: index + 1,
            seed: 104729,
            timeoutMs: 60000,
          });
        }
      }
    }
  }
  const definition = {
    version: "operations-v4",
    scenarios: presets,
    layers: [{ name: "operations", transports: rpcTransportNames }],
    workloads: operationNames,
    workloadSettings: operationSettings,
    seeds: [104729, 104729, 104729],
    timeoutMs: 60000,
    linkSettings,
    quicFlowControl,
    rpcHeartbeatTimeout,
    serialization: {
      ...suiteDefinition.serialization,
      rpcWebTransport: "operation-transport-default",
    },
    compression: { ...suiteDefinition.compression, webTransport: "operation-transport-default" },
    order: "scenario,workload,seed,rotated-transport",
    timing:
      "uncached:ready-session-through-live;incremental:server-write-through-applied;reconnect:open-through-live",
    traffic: "loopback-tx-IP-bytes-both-directions-through-client-completion",
    reconnectSetup: "verified-offline-cache-with-no-prior-transport",
    packetization: operationPacketSettings,
    snapshots: {
      proxy: http3ProxySettings,
      websocket: "https-http1.1",
      webTransport: "http3-only",
      client: "node-libcurl-5.1.2",
    },
    cases,
  } as const;
  return {
    hash: NodeCrypto.createHash("sha256").update(JSON.stringify(definition)).digest("hex"),
    ...definition,
  };
}

/** Fixed independent draws; critical loss cells and periodic phases get broader coverage. */
export function makeCurrentOperationSuitePlan() {
  const { hash: _hash, cases: _cases, ...legacy } = makeOperationSuitePlan();
  const seeds = [
    104729, 130363, 155921, 181081, 206369, 231701, 257053, 282511, 308087, 333667, 359357, 385039,
    410857, 436757, 462671, 488723, 514793, 540863, 567047, 593231,
  ];
  const transports = ["rpc-ws", "rpc-wss", "rpc-wt", "rpc-wt-multi"] as const;
  const cases: BenchCase[] = [];
  const critical = new Set(["sync-five", "open-new", "reconnect-replay"]);
  for (const scenario of presets) {
    for (const workload of operationNames) {
      const extended =
        critical.has(workload) && ["loss2", "loss5", "nth20-drop"].includes(scenario.name);
      for (const [index, seed] of seeds.entries()) {
        for (let offset = 0; offset < transports.length; offset++) {
          const transport = transports[(index + offset) % transports.length]!;
          if (index >= 3 && !(extended && ["rpc-wss", "rpc-wt"].includes(transport))) continue;
          cases.push({
            layer: "operations",
            scenario,
            workload,
            transport,
            repetition: index + 1,
            seed,
            timeoutMs: 60000,
            ...(scenario.nthDrop || scenario.nthDelay ? { packetPhase: index } : {}),
          });
        }
      }
    }
  }
  for (const scenario of presets.filter((s) => ["clean", "wan", "high-latency"].includes(s.name))) {
    for (const workload of ["open-new", "open-long"]) {
      for (const [index, seed] of seeds.slice(0, 3).entries()) {
        for (const transport of index % 2 === 0 ? ["rpc-wss", "rpc-wt"] : ["rpc-wt", "rpc-wss"]) {
          cases.push({
            layer: "operations",
            scenario,
            workload,
            transport,
            repetition: index + 1,
            seed,
            timeoutMs: 60000,
            httpPool: "warm",
          });
        }
      }
    }
  }
  const definition = {
    ...legacy,
    version: "operations-v5",
    layers: [{ name: "operations", transports }],
    seeds,
    sampling: {
      general: "first-three-independent-seeds",
      critical: {
        profiles: ["loss2", "loss5", "nth20-drop"],
        workloads: [...critical],
        transports: ["rpc-wss", "rpc-wt"],
        repetitions: 20,
      },
      periodic: "prime-counter-phase-before-connection-setup",
      warm: {
        profiles: ["clean", "wan", "high-latency"],
        workloads: ["open-new", "open-long"],
        transports: ["rpc-wss", "rpc-wt"],
        repetitions: 3,
      },
    },
    websocketClient: "node-ws-common-plain-and-tls1.3-verified",
    cases,
  } as const;
  return {
    hash: NodeCrypto.createHash("sha256").update(JSON.stringify(definition)).digest("hex"),
    ...definition,
  };
}
