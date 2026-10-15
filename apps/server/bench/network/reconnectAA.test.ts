import { expect, it } from "@effect/vitest";
import { compareAA, ksDistance } from "./reconnectAA.ts";
import type { ReconnectRow } from "./reconnectComparison.ts";
import { defaultVariantOptions } from "./reconnectOptions.ts";
import { makeManifest } from "./reconnectManifest.ts";
const entry = makeManifest(true).cases[0]!;
const row = (ms: number, seed = 1, stochastic = false): ReconnectRow => ({
  variant: "a",
  revision: "a",
  dirty: false,
  manifestHash: "a",
  case: { ...entry, id: String(seed), seed, profile: { ...entry.profile, stochastic } },
  result: {
    totalMs: ms,
    shellLiveMs: ms,
    switches: defaultVariantOptions,
    serverRequests: [],
    stages: [],
    requests: {},
    bytes: 0,
    packets: 0,
    events: 0,
    sockets: 1,
    correct: true,
  },
});
it("applies the declared absolute or relative deterministic tolerance to both milestones", () => {
  expect(compareAA([row(100), row(104, 2)], [row(104), row(108, 2)]).pass).toBe(true);
  expect(compareAA([row(1000)], [row(1021)]).pass).toBe(false);
  expect(
    compareAA([row(100)], [{ ...row(101), result: { ...row(101).result!, shellLiveMs: 120 } }])
      .pass,
  ).toBe(false);
  const { result: _result, ...failed } = row(100);
  expect(compareAA([row(100)], [{ ...failed, error: "timeout" }]).pass).toBe(false);
});
it("requires twenty stochastic observations, overlapping median intervals, and compatible empirical distributions", () => {
  const a = Array.from({ length: 20 }, (_, i) => row(100 + i * 10, i, true));
  expect(compareAA(a, a.toReversed()).pass).toBe(true);
  expect(
    compareAA(
      a,
      a.map((r) => ({ ...r, result: { ...r.result!, totalMs: r.result!.totalMs + 1000 } })),
    ).pass,
  ).toBe(false);
  expect(compareAA(a.slice(0, 4), a.slice(0, 4)).pass).toBe(false);
  expect(ksDistance([1, 2, 3], [4, 5, 6])).toBe(1);
});
