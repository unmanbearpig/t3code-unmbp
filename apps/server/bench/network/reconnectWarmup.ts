import { lossSeeds, profiles, type ReconnectCase } from "./reconnectManifest.ts";

/** Exercise every transport/route/replay path before measuring a reused worker. */
export function warmupCase(lane: number, variant: string, index: number) {
  const good = profiles.find((profile) => profile.name === "good")!;
  return {
    id: `warmup/${lane}/${variant}/${index}`,
    profile: { ...good, name: "warmup-good" },
    workload: index % 2 ? "replay-50" : "idle",
    scheme: Math.floor(index / 2) % 2 ? "http" : "https",
    routes: Math.floor(index / 4) % 2 ? 1 : 2,
    repetition: 1,
    seed: lossSeeds[0]!,
    phaseMs: 0,
    timeoutMs: 60000,
  } satisfies ReconnectCase;
}
