// @effect-diagnostics nodeBuiltinImport:off - Exercises the controller parser without changing host networking.
import * as NodeChildProcess from "node:child_process";
import { describe, expect, it } from "@effect/vitest";
import {
  balancedOrder,
  lossSeeds,
  makeManifest,
  makeReducedManifest,
  manifestHash,
  phaseOffsetsMs,
  profiles,
  validateManifest,
  validateSchedule,
  variantOrder,
  type ReconnectCase,
} from "./reconnectManifest.ts";
import { controllerPlan, directionCommands, netemArgs } from "./reconnectNetwork.ts";
import { median, observedP90, summarize, type ReconnectRow } from "./reconnectComparison.ts";
import { validateEcho } from "./reconnectCalibration.ts";

const cellular = profiles.find((p) => p.name === "cellular")!;
const entry = makeManifest(true).cases[0]!;
const row = (
  totalMs: number,
  stages = [{ name: "ticket", atMs: 10, endMs: 40 }],
): ReconnectRow => ({
  variant: "baseline",
  revision: "123",
  dirty: false,
  manifestHash: "fixed",
  case: entry,
  result: {
    totalMs,
    shellLiveMs: totalMs + 20,
    switches: {
      helloHandshake: false,
      helloSupportCached: true,
      websocketHeaders: false,
      skipDescriptorOnHttpsReconnect: false,
    },
    serverRequests: [],
    stages,
    requests: { "/.well-known/t3/environment": 2 },
    bytes: 100,
    packets: 5,
    events: 0,
    sockets: 1,
    correct: true,
  },
});
describe("frozen reconnect inputs", () => {
  it("covers the full matrix, all seeds, both routes/transports/workloads, and every phase", () => {
    const manifest = makeManifest();
    expect(manifest.cases).toHaveLength(1440);
    expect(manifest.mtu).toBe(1280);
    const smoke = makeManifest(true);
    expect(smoke.cases).toHaveLength(47);
    expect(new Set(smoke.cases.map((c) => `${c.scheme}/${c.routes}`))).toEqual(
      new Set(["http/1", "http/2", "https/1", "https/2"]),
    );
    expect(new Set(manifest.cases.map((c) => c.id)).size).toBe(1440);
    expect(lossSeeds).toHaveLength(20);
    expect(
      new Set(manifest.cases.filter((c) => c.profile.name === "loss-1").map((c) => c.seed)),
    ).toEqual(new Set(lossSeeds));
    expect(
      new Set(
        manifest.cases.filter((c) => c.profile.name === "latency-spikes").map((c) => c.phaseMs),
      ),
    ).toEqual(new Set(phaseOffsetsMs));
    expect(manifestHash(manifest)).toBe(manifestHash(makeManifest()));
  });
  it("rejects duplicates, arbitrary phases, malformed directions, and unpaired schedules", () => {
    const manifest = makeManifest(true);
    expect(() => validateManifest({ ...manifest, cases: [entry, entry] })).toThrow("Duplicate");
    expect(() => validateManifest({ ...manifest, cases: [{ ...entry, phaseMs: 13 }] })).toThrow(
      "Phase",
    );
    expect(() =>
      validateManifest({
        ...manifest,
        cases: [{ ...entry, profile: { ...cellular, down: { ...cellular.down, rateKbit: 0 } } }],
      }),
    ).toThrow("directional");
    expect(() => validateSchedule([{ atMs: 0, action: "outage-on" }])).toThrow("active");
    expect(() =>
      validateSchedule([
        { atMs: 20, action: "outage-on" },
        { atMs: 10, action: "outage-off" },
      ]),
    ).toThrow("ordered");
  });
  it("balances first positions and forward/reverse order over complete cycles", () => {
    const variants = ["a", "b", "c", "d"];
    const orders = Array.from({ length: 8 }, (_, i) => balancedOrder(variants, i));
    expect(orders.every((order) => new Set(order).size === 4)).toBe(true);
    expect(variants.map((v) => orders.filter((o) => o[0] === v).length)).toEqual([2, 2, 2, 2]);
  });
});
it("balances each loss cell across twenty seeds instead of repeating global-index order", () => {
  const cells = makeManifest().cases.filter(
    (c) =>
      c.profile.name === "loss-1" &&
      c.routes === 2 &&
      c.scheme === "https" &&
      c.workload === "idle",
  );
  const orders = cells.map((c) => variantOrder(["baseline", "change"], c));
  expect(orders.filter((o) => o[0] === "baseline")).toHaveLength(10);
  expect(orders.filter((o) => o[0] === "change")).toHaveLength(10);
});
describe("directional network and controller", () => {
  it("classifies downloads by source port and uses FIFO below both directional netems", () => {
    const commands = directionCommands(cellular, 10, 100000, 1234).join("\n");
    expect(commands).toContain("prio bands 3");
    expect(commands).toContain("match ip sport 1234 0xffff flowid 1:1");
    expect(commands).toContain("parent 20:1 handle 21: pfifo");
    expect(commands).toContain("parent 30:1 handle 31: pfifo");
    expect(commands).toContain("rate 100000kbit seed 10");
    expect(commands).toContain("rate 100000kbit seed 11");
    expect(netemArgs(cellular.down, 10, 100000)).toContain("7ms");
  });
  it("offsets spikes and gives outages priority over redirect", () => {
    const spike = profiles.find((p) => p.name === "latency-spikes")!;
    const plan = controllerPlan(spike, 10, 100000, 250);
    expect(plan.map((e) => e.atMs)).toEqual([250, 750]);
    expect(plan[0]!.commands.every((c) => c.includes("delay 1000ms"))).toBe(true);
    expect(plan[1]!.commands.every((c) => c.includes("delay 35ms"))).toBe(true);
    const outage = controllerPlan(
      profiles.find((p) => p.name === "slow-start-1500")!,
      10,
      100000,
      0,
    );
    expect(outage[0]!.commands).toEqual([
      "filter add dev lo ingress protocol ip pref 1 matchall action drop",
    ]);
    expect(outage[1]!.commands).toEqual(["filter del dev lo ingress protocol ip pref 1"]);
  });
  it("the separate controller rejects unordered times and multiline commands before tc", () => {
    const controller = new URL("./reconnectController.py", import.meta.url).pathname;
    const script = `import importlib.util
s=importlib.util.spec_from_file_location('controller',${JSON.stringify(controller)})
m=importlib.util.module_from_spec(s)
s.loader.exec_module(m)
assert m.validate_plan([{'atMs':0,'commands':['qdisc show dev lo']}])
for p in [[{'atMs':-1,'commands':['x']}],[{'atMs':0,'commands':['x\\ny']}],[{'atMs':2,'commands':['x']},{'atMs':1,'commands':['x']}]]:
    try: m.validate_plan(p)
    except ValueError: pass
    else: raise AssertionError('invalid schedule accepted')
`;
    expect(NodeChildProcess.spawnSync("python3", ["-B", "-c", script]).status).toBe(0);
  });
  it("calibration rejects reordered packets even when median RTT is correct", () => {
    const echo = (number: number) => ({
      number,
      sentMs: number * 20,
      receivedMs: number * 20 + 70,
      rttMs: 70,
    });
    expect(
      validateEcho(cellular, { type: "result", sent: 3, echoes: [echo(0), echo(2), echo(1)] }),
    ).toContain("Packet reordering");
    expect(
      validateEcho(cellular, { type: "result", sent: 3, echoes: [echo(0), echo(1), echo(2)] }),
    ).toEqual([]);
  });
});
describe("comparison", () => {
  it("uses medians and reports observed p90 only with twenty successful samples", () => {
    expect(median([2, 1, 9, 3])).toBe(2.5);
    expect(median([])).toBeNull();
    expect(observedP90([1, 2, 3])).toBeNull();
    expect(observedP90(Array.from({ length: 20 }, (_, i) => i + 1))).toBe(18);
  });
  it("counts failures and separates workload, route count, and transport", () => {
    const failed: ReconnectRow = {
      variant: "baseline",
      revision: "123",
      dirty: false,
      manifestHash: "fixed",
      case: entry,
      error: "timeout",
    };
    const summary = summarize([
      row(100),
      row(200),
      failed,
      { ...row(1), case: { ...entry, routes: 1 } as ReconnectCase },
    ]);
    expect(summary).toHaveLength(2);
    expect(summary[0]).toMatchObject({
      count: 3,
      successes: 2,
      failures: 1,
      medianMs: 150,
      p90Ms: null,
      stageMediansMs: { ticket: 40 },
      stageDurationMediansMs: { ticket: 30 },
    });
  });
});

it("keeps every workload and profile in the reduced decision pass", () => {
  const manifest = makeReducedManifest();
  expect(manifest.cases).toHaveLength(312);
  expect(new Set(manifest.cases.map((c) => c.workload))).toEqual(
    new Set(["idle", "replay-50", "short-probe", "network-changed"]),
  );
  expect(new Set(manifest.cases.map((c) => c.profile.name)).size).toBe(11);
  expect(manifest.cases.every((c) => c.scheme === "https" && c.routes === 2)).toBe(true);
  expect(new Set(manifest.cases.filter((c) => c.profile.stochastic).map((c) => c.seed)).size).toBe(
    20,
  );
});
