// @effect-diagnostics nodeBuiltinImport:off - Fingerprints fixed benchmark inputs outside the measured path.
import * as NodeCrypto from "node:crypto";
import * as Schema from "effect/Schema";
import { validateLinkMtu } from "./impairments.ts";

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
const profile = (name: string, delayMs: number, loss?: string): Profile => ({
  name,
  down: { delayMs, jitterMs: 0, rateKbit: 100000, ...(loss ? { loss } : {}) },
  up: { delayMs, jitterMs: 0, rateKbit: 100000, ...(loss ? { loss } : {}) },
  stochastic: loss !== undefined,
  schedule: [],
});
export const profiles = [profile("clean", 0), profile("wan", 25), profile("loss2", 25, "2%")];
export const workloads = ["idle", "replay-50", "short-probe", "network-changed"] as const;
export const seeds = [731, 947, 1163, 1381, 1597] as const;
export const ReconnectCase = Schema.Struct({
  id: Schema.String,
  profile: Profile,
  scheme: Schema.Literals(["http", "https"]),
  routes: Schema.Literals([1, 2]),
  workload: Schema.Literals(workloads),
  repetition: Schema.Number,
  seed: Schema.Number,
  phaseMs: Schema.Number,
  timeoutMs: Schema.Number,
});
export type ReconnectCase = typeof ReconnectCase.Type;
export const Manifest = Schema.Struct({
  version: Schema.Literal("rpc-reconnect-v1"),
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
  validateLinkMtu(manifest.mtu);
  if (!Number.isSafeInteger(manifest.queuePackets) || manifest.queuePackets < 1000)
    throw new Error("Invalid deep queue size");
  if (!manifest.cases.length) throw new Error("Manifest must contain cases");
  const ids = new Set<string>();
  for (const entry of manifest.cases) {
    if (ids.has(entry.id)) throw new Error(`Duplicate case ${entry.id}`);
    ids.add(entry.id);
    if (
      !Number.isSafeInteger(entry.seed) ||
      entry.seed < 1 ||
      entry.seed > 0xfffffffd ||
      !Number.isSafeInteger(entry.repetition) ||
      entry.repetition < 1 ||
      !Number.isFinite(entry.timeoutMs) ||
      entry.timeoutMs <= 0 ||
      !Number.isSafeInteger(entry.phaseMs) ||
      entry.phaseMs < 0
    )
      throw new Error("Invalid seed, repetition, phase or timeout");
    validateSchedule(entry.profile.schedule);
    for (const direction of [entry.profile.down, entry.profile.up]) {
      if (
        ![direction.delayMs, direction.jitterMs, direction.rateKbit].every(Number.isFinite) ||
        direction.delayMs < 0 ||
        direction.jitterMs < 0 ||
        direction.jitterMs > direction.delayMs ||
        direction.rateKbit <= 0 ||
        (direction.loss !== undefined &&
          (!/^\d+(?:\.\d+)?%$/.test(direction.loss) || Number(direction.loss.slice(0, -1)) > 100))
      )
        throw new Error("Invalid directional impairment");
    }
  }
  return manifest;
}

/** Four integration cases by default; the optional measurement plan has sixty paired inputs. */
export function makeManifest(measure = false): Manifest {
  const selectedProfiles = measure ? profiles : profiles.filter((p) => p.name === "wan");
  const selectedSeeds = measure ? seeds : seeds.slice(0, 1);
  return validateManifest({
    version: "rpc-reconnect-v1",
    mtu: 1280,
    queuePackets: 100000,
    fixture: "100-cached-messages-50-new-domain-events",
    cases: selectedProfiles.flatMap((profile) =>
      workloads.flatMap((workload) =>
        selectedSeeds.map((seed, index) => ({
          id: [profile.name, "https", 2, workload, seed].join("/"),
          profile,
          scheme: "https",
          routes: 2,
          workload,
          seed,
          repetition: index + 1,
          phaseMs: 0,
          timeoutMs: 25000,
        })),
      ),
    ),
  });
}
export const manifestHash = (manifest: Manifest) =>
  NodeCrypto.createHash("sha256").update(JSON.stringify(manifest)).digest("hex");

/** Balance first position within each seed group, then rotate across operation cells. */
export function variantOrder<T>(variants: readonly T[], entry: ReconnectCase): readonly T[] {
  if (!variants.length) throw new Error("At least one variant is required");
  const key = [entry.profile.name, entry.scheme, entry.routes, entry.workload, entry.phaseMs].join(
    "/",
  );
  const offset =
    [...key].reduce((sum, c) => (sum * 31 + c.charCodeAt(0)) >>> 0, 0) % variants.length;
  const order = [...variants.slice(offset), ...variants.slice(0, offset)];
  const rotated = entry.repetition % 2 === 0 ? order.toReversed() : order;
  const rotation = Math.floor((entry.repetition - 1) / 2) % variants.length;
  return [...rotated.slice(rotation), ...rotated.slice(0, rotation)];
}
