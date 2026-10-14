// @effect-diagnostics nodeBuiltinImport:off - The suite fingerprint is computed outside measured workloads.
import * as NodeCrypto from "node:crypto";

import { transportNames } from "./client.ts";
import { presets, type Scenario } from "./impairments.ts";
import { quicFlowControl } from "./protocol.ts";
import { rpcTransportNames } from "./rpcClient.ts";
import { operationNames, operationSettings } from "./operationsFixture.ts";
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
  ].join("/");

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
    version: "operations-v2",
    scenarios: presets,
    layers: [{ name: "operations", transports: rpcTransportNames }],
    workloads: operationNames,
    workloadSettings: operationSettings,
    seeds: [104729, 104729, 104729],
    timeoutMs: 60000,
    linkSettings,
    quicFlowControl,
    rpcHeartbeatTimeout,
    serialization: suiteDefinition.serialization,
    compression: suiteDefinition.compression,
    order: "scenario,workload,seed,rotated-transport",
    timing:
      "uncached:ready-session-through-live;incremental:server-write-through-applied;reconnect:open-through-live",
    traffic: "loopback-tx-IP-bytes-both-directions-through-client-completion",
    reconnectSetup: "verified-offline-cache-with-no-prior-transport",
    cases,
  } as const;
  return {
    hash: NodeCrypto.createHash("sha256").update(JSON.stringify(definition)).digest("hex"),
    ...definition,
  };
}
