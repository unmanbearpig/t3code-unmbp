import { describe, expect, it } from "@effect/vitest";
import { makeManifest, manifestHash, validateManifest, variantOrder } from "./reconnectManifest.ts";
import { distribution, summarize, type ReconnectRow } from "./reconnectComparison.ts";
import { directionCommands, controllerPlan } from "./reconnectNetwork.ts";

const manifest = makeManifest();
const entry = manifest.cases[0]!;
const row = (
  variant: string,
  totalMs: number,
  overrides: Partial<ReconnectRow> = {},
): ReconnectRow => ({
  variant,
  revision: "committed",
  dirty: false,
  manifestHash: "same",
  case: entry,
  result: {
    totalMs,
    shellLiveMs: totalMs + 20,
    selectedTransport: "websocket",
    selections: [{ transport: "websocket", atMs: 0 }],
    nativeEvidence: null,
    serverRequests: [],
    stages: [],
    requests: {},
    bytes: 123,
    packets: 4,
    events: 0,
    sockets: 1,
    correct: true,
    resources: { window: "case", client: {}, server: {} },
  },
  ...overrides,
});

describe("bounded reconnect plan", () => {
  it("keeps the default smoke small and gives the larger plan five independent seeds per cell", () => {
    expect(manifest.cases).toHaveLength(4);
    expect(new Set(manifest.cases.map((c) => c.workload)).size).toBe(4);
    expect(manifest.mtu).toBe(1280);
    expect(makeManifest(true).cases).toHaveLength(60);
    expect(makeManifest(true, "operations").cases).toHaveLength(45);
    expect(makeManifest(true, "fallback").cases).toHaveLength(15);
    expect(makeManifest(true, "overlap").cases).toHaveLength(30);
    expect(manifestHash(makeManifest())).toBe(manifestHash(manifest));
    expect(new Set(makeManifest(true).cases.map((c) => c.id)).size).toBe(60);
  });
  it("refuses duplicate cases and invalid link or packet inputs before network mutations", () => {
    expect(() => validateManifest({ ...manifest, cases: [entry, entry] })).toThrow("Duplicate");
    expect(() => validateManifest({ ...manifest, mtu: 1000 })).toThrow("--mtu");
    expect(() => validateManifest({ ...manifest, cases: [{ ...entry, seed: 0 }] })).toThrow("seed");
    expect(() =>
      validateManifest({
        ...manifest,
        cases: [
          {
            ...entry,
            profile: { ...entry.profile, down: { ...entry.profile.down, loss: "101%" } },
          },
        ],
      }),
    ).toThrow("directional");
  });
  it("balances first variant within a cell over twenty seeds", () => {
    const orders = Array.from({ length: 20 }, (_, index) =>
      variantOrder(["before", "after"], { ...entry, repetition: index + 1 }),
    );
    expect(orders.filter((order) => order[0] === "before")).toHaveLength(10);
    expect(orders.every((order) => new Set(order).size === 2)).toBe(true);
  });
  it("classifies download and upload packets separately and preserves FIFO below netem", () => {
    const commands = directionCommands(entry.profile, 731, 100000, 4410).join("\n");
    expect(commands).toContain("match ip sport 4410 0xffff flowid 1:1");
    expect(commands).toContain("parent 20:1 handle 21: pfifo");
    expect(commands).toContain("parent 30:1 handle 31: pfifo");
    expect(controllerPlan(entry.profile, 731, 100000, 0)).toEqual([]);
  });
});

describe("paired completion evidence", () => {
  it("keeps per-run resource and probe sample counts separate and excludes failed runs", () => {
    const first = row("a", 10);
    const second = row("a", 20, { case: { ...entry, id: "second", repetition: 2 } });
    const failed = row("a", 1, { case: { ...entry, id: "failed" }, error: "timeout" });
    const observed = [first, second, failed].map((sample, index) => ({
      ...sample,
      result: {
        ...sample.result!,
        operationMetrics: {
          operation: "bulk-overlap",
          probeSamples: index + 10,
          probeMedianMs: index + 1,
        },
        resources: { window: "whole-case", client: { cpuUserMs: (index + 1) * 10 }, server: {} },
      },
    }));
    expect(summarize(observed, "a")[0]).toMatchObject({
      attempted: 3,
      completed: 2,
      selectedTransportCounts: { websocket: 2, webtransport: 0 },
      operationMetrics: { probeSamples: { n: 2, min: 10, median: 10.5, max: 11 } },
      resources: { windows: ["whole-case"], client: { cpuUserMs: { n: 2, median: 15 } } },
    });
  });
  it("reports paired deltas for equal successful inputs while retaining failures", () => {
    const failedCase = { ...entry, id: "second", seed: 947, repetition: 2 };
    const groups = summarize(
      [
        row("before", 100),
        row("after", 60),
        row("before", 200, { case: failedCase }),
        row("after", 1, { case: failedCase, error: "timeout" }),
      ],
      "before",
    );
    expect(groups[1]).toMatchObject({
      attempted: 2,
      completed: 1,
      pairedN: 1,
      pairedDeltaMs: { n: 1, median: -40 },
      pairedDeltaPercent: { median: -40 },
      completionMs: { n: 1, observedP90: null },
    });
    expect(groups[1]!.failures).toEqual([{ caseId: "second", error: "timeout" }]);
  });
  it("refuses mixed revisions, changed inputs, duplicates and dirty measurements", () => {
    expect(() => summarize([row("a", 1), row("a", 2)], "a")).toThrow("Duplicate");
    expect(() => summarize([row("a", 1, { dirty: true })], "a")).toThrow("provenance");
    expect(() => summarize([row("a", 1), row("b", 2, { manifestHash: "other" })], "a")).toThrow(
      "provenance",
    );
    expect(() =>
      summarize([row("a", 1), row("b", 2, { case: { ...entry, seed: 947 } })], "a"),
    ).toThrow("inputs");
  });
  it("reports observed tails only with enough actual successful samples", () => {
    expect(distribution([1, 4, 8])).toEqual({ n: 3, min: 1, median: 4, max: 8, observedP90: null });
    expect(distribution(Array.from({ length: 20 }, (_, i) => i + 1))?.observedP90).toBe(18);
  });
});
