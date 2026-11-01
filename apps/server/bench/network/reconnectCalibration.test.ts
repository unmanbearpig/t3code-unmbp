import { expect, it } from "@effect/vitest";
import { validateEcho } from "./reconnectCalibration.ts";
import { profiles } from "./reconnectManifest.ts";

const wan = profiles.find((profile) => profile.name === "wan")!;
const echoes = [0, 1, 2].map((number) => ({
  number,
  sentMs: number * 20,
  receivedMs: number * 20 + 50,
  rttMs: 50,
}));
const result = { type: "result" as const, sent: 3, echoes };

it("accepts the directional RTT sum and rejects an impairment applied to only one direction", () => {
  expect(validateEcho(wan, result)).toEqual([]);
  expect(
    validateEcho(wan, {
      ...result,
      echoes: echoes.map((echo) => ({ ...echo, rttMs: 25 })),
    }),
  ).toEqual(["Base RTT 25ms differs from 50ms"]);
});

it("rejects packet reordering, missing packets on a lossless link, and no receiver evidence", () => {
  expect(validateEcho(wan, { ...result, echoes: [echoes[1]!, echoes[0]!, echoes[2]!] })).toContain(
    "Packet reordering",
  );
  expect(validateEcho(wan, { ...result, echoes: echoes.slice(0, 2) })).toContain(
    "Unexpected packet loss",
  );
  expect(validateEcho(wan, { ...result, echoes: [] })).toEqual(["No numbered echoes received"]);
  expect(
    validateEcho({ ...wan, stochastic: true }, { ...result, echoes: echoes.slice(0, 2) }),
  ).toEqual([]);
});
