// @effect-diagnostics nodeBuiltinImport:off - Fingerprints benchmark inputs outside the measured path.
import * as NodeCrypto from "node:crypto";
import * as Schema from "effect/Schema";

export const phaseOffsetsMs = [0, 100, 250, 500, 750, 1000, 1500, 2000] as const;
export const lossSeeds = Array.from({ length: 20 }, (_, i) => 104729 + i * 7919);
const Direction = Schema.Struct({
  delayMs: Schema.Number,
  jitterMs: Schema.Number,
  rateKbit: Schema.Number,
  loss: Schema.optionalKey(Schema.String),
});
export type Direction = typeof Direction.Type;
export const ScheduleEvent = Schema.Struct({
  atMs: Schema.Number,
  action: Schema.Literals(["spike-on", "spike-off", "outage-on", "outage-off"]),
});
export type ScheduleEvent = typeof ScheduleEvent.Type;
const Profile = Schema.Struct({
  name: Schema.String,
  down: Direction,
  up: Direction,
  stochastic: Schema.Boolean,
  schedule: Schema.Array(ScheduleEvent),
});
export type Profile = typeof Profile.Type;
const cellular: Direction = { delayMs: 35, jitterMs: 7, rateKbit: 100000 };
const symmetric = (name: string, direction: Direction, stochastic = false): Profile => ({
  name,
  down: direction,
  up: direction,
  stochastic,
  schedule: [],
});
export const profiles: readonly Profile[] = [
  symmetric("good", { delayMs: 10, jitterMs: 0, rateKbit: 100000 }),
  symmetric("cellular", cellular),
  symmetric("high-latency", { delayMs: 150, jitterMs: 0, rateKbit: 100000 }),
  {
    ...symmetric("latency-spikes", cellular),
    schedule: [
      { atMs: 0, action: "spike-on" },
      { atMs: 500, action: "spike-off" },
    ],
  },
  symmetric("loss-1", { ...cellular, loss: "1%" }, true),
  symmetric("loss-3", { ...cellular, loss: "3%" }, true),
  symmetric("burst-loss", { ...cellular, loss: "gemodel 1% 25%" }, true),
  ...[500, 1500, 3000].map((outageMs): Profile => ({
    ...symmetric(`slow-start-${outageMs}`, cellular),
    schedule: [
      { atMs: 0, action: "outage-on" },
      { atMs: outageMs, action: "outage-off" },
    ],
  })),
  {
    ...symmetric("constrained", cellular),
    down: { ...cellular, rateKbit: 1000 },
    up: { ...cellular, rateKbit: 512 },
  },
];
export const ReconnectCase = Schema.Struct({
  id: Schema.String,
  profile: Profile,
  scheme: Schema.Literals(["http", "https"]),
  routes: Schema.Literals([1, 2]),
  workload: Schema.Literals(["idle", "replay-50"]),
  repetition: Schema.Number,
  seed: Schema.Number,
  phaseMs: Schema.Number,
  timeoutMs: Schema.Number,
});
export type ReconnectCase = typeof ReconnectCase.Type;
export const Manifest = Schema.Struct({
  version: Schema.Literal("reconnect-v1"),
  mtu: Schema.Number,
  queuePackets: Schema.Number,
  fixture: Schema.Literal("100-cached-messages-50-new-domain-events"),
  cases: Schema.Array(ReconnectCase),
});
export type Manifest = typeof Manifest.Type;
export function validateSchedule(events: readonly ScheduleEvent[]) {
  let last = -1;
  const active = new Set<string>();
  for (const event of events) {
    if (!Number.isSafeInteger(event.atMs) || event.atMs < 0 || event.atMs < last)
      throw new Error("Schedule times must be ordered nonnegative integer milliseconds");
    last = event.atMs;
    const [kind, state] = event.action.split("-");
    if (state === "on" ? active.has(kind!) : !active.has(kind!))
      throw new Error("Schedule must pair each on with one off");
    if (state === "on") active.add(kind!);
    else active.delete(kind!);
  }
  if (active.size) throw new Error("Schedule leaves an impairment active");
}
const decodeManifest = Schema.decodeUnknownSync(Manifest);
export function validateManifest(input: unknown): Manifest {
  const manifest = decodeManifest(input);
  if (
    !Number.isSafeInteger(manifest.mtu) ||
    manifest.mtu < 576 ||
    manifest.mtu > 1500 ||
    !Number.isSafeInteger(manifest.queuePackets) ||
    manifest.queuePackets < 1000
  )
    throw new Error("Invalid MTU or deep queue size");
  const ids = new Set<string>();
  for (const c of manifest.cases) {
    if (ids.has(c.id)) throw new Error(`Duplicate case ${c.id}`);
    ids.add(c.id);
    if (
      !Number.isSafeInteger(c.seed) ||
      c.seed < 1 ||
      c.seed > 0xfffffffd ||
      !Number.isSafeInteger(c.repetition) ||
      c.repetition < 1 ||
      c.timeoutMs <= 0 ||
      !Number.isFinite(c.timeoutMs)
    )
      throw new Error("Invalid seed, repetition, or timeout");
    if (
      !(phaseOffsetsMs as readonly number[]).includes(c.phaseMs) ||
      (c.profile.name !== "latency-spikes" && c.phaseMs !== 0)
    )
      throw new Error("Phase must come from the fixed spike manifest");
    validateSchedule(c.profile.schedule);
    for (const d of [c.profile.down, c.profile.up]) {
      if (
        ![d.delayMs, d.jitterMs, d.rateKbit].every(Number.isFinite) ||
        d.delayMs < 0 ||
        d.jitterMs < 0 ||
        d.jitterMs > d.delayMs ||
        d.rateKbit <= 0 ||
        (d.loss !== undefined && !/^(?:[13]%|gemodel 1% 25%)$/.test(d.loss))
      )
        throw new Error("Invalid directional impairment");
    }
  }
  return manifest;
}
export function makeManifest(smoke = false): Manifest {
  const cases: ReconnectCase[] = [];
  for (const profile of profiles) {
    const phases = profile.name === "latency-spikes" && !smoke ? phaseOffsetsMs : [0];
    const seeds = smoke ? [lossSeeds[0]!] : profile.stochastic ? lossSeeds : lossSeeds.slice(0, 4);
    for (const phaseMs of phases)
      for (const [index, seed] of seeds.entries()) {
        for (const scheme of smoke ? ["https" as const] : ["http" as const, "https" as const])
          for (const routes of smoke ? [2 as const] : [1 as const, 2 as const])
            for (const workload of smoke
              ? ["idle" as const, "replay-50" as const]
              : ["idle" as const, "replay-50" as const]) {
              cases.push({
                id: [profile.name, phaseMs, seed, scheme, routes, workload].join("/"),
                profile,
                phaseMs,
                seed,
                scheme,
                routes,
                workload,
                repetition: index + 1,
                timeoutMs: 60000,
              });
            }
      }
  }
  return validateManifest({
    version: "reconnect-v1",
    mtu: 1280,
    queuePackets: 100000,
    fixture: "100-cached-messages-50-new-domain-events",
    cases,
  });
}
export const manifestHash = (manifest: Manifest) =>
  NodeCrypto.createHash("sha256").update(JSON.stringify(manifest)).digest("hex");
/** Forward/reverse rotations balance both first position and adjacent ordering. */
export function balancedOrder<T>(variants: readonly T[], caseIndex: number): readonly T[] {
  if (!variants.length) throw new Error("At least one variant is required");
  const order = caseIndex % 2 ? variants.toReversed() : [...variants];
  const offset = Math.floor(caseIndex / 2) % variants.length;
  return [...order.slice(offset), ...order.slice(0, offset)];
}
