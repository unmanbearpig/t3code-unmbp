// @effect-diagnostics nodeBuiltinImport:off globalConsole:off globalTimers:off globalDate:off - Outer runner owns isolated benchmark processes, not app runtime state.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import * as HostProcess from "@t3tools/shared/HostProcess";
import {
  makeManifest,
  makeReducedManifest,
  validateManifest,
  manifestHash,
  makeLifecycleManifest,
} from "./reconnectManifest.ts";
import { warmupCase } from "./reconnectWarmup.ts";
import { scheduleLanes } from "./reconnectSchedule.ts";
import { mailbox, stopChild } from "./reconnectProcess.ts";
import { VariantOptions, defaultVariantOptions } from "./reconnectOptions.ts";
import { summarize, type ReconnectRow } from "./reconnectComparison.ts";

const decodeWorker = Schema.decodeUnknownSync(
  Schema.Union([
    Schema.Struct({ type: Schema.Literal("failure"), error: Schema.String }),
    Schema.Struct({
      type: Schema.Literal("result"),
      controller: Schema.Unknown,
      wallMs: Schema.Number,
      result: Schema.Struct({
        totalMs: Schema.Number,
        shellLiveMs: Schema.Number,
        switches: VariantOptions,
        usesHelloHandshake: Schema.Boolean,
        helloSupportCachedAtStart: Schema.Boolean,
        serverRequests: Schema.Array(
          Schema.Struct({
            path: Schema.String,
            connection: Schema.Number,
            connectionId: Schema.String,
            atMs: Schema.Number,
            bytes: Schema.Number,
          }),
        ),
        stages: Schema.Array(
          Schema.Struct({
            name: Schema.String,
            attempt: Schema.optionalKey(Schema.Number),
            phase: Schema.optionalKey(Schema.Literals(["measurement", "cleanup"])),
            atMs: Schema.Number,
            endMs: Schema.optionalKey(Schema.Number),
            bytes: Schema.optionalKey(Schema.Number),
            outcome: Schema.optionalKey(Schema.Literals(["succeeded", "failed"])),
          }),
        ),
        requests: Schema.Record(Schema.String, Schema.Number),
        rpcRequests: Schema.Record(Schema.String, Schema.Number),
        bytes: Schema.Number,
        packets: Schema.Number,
        events: Schema.Number,
        shellSnapshots: Schema.Number,
        sockets: Schema.Number,
        correct: Schema.Boolean,
        simulatedBackgroundMs: Schema.Number,
        backgroundHttpPoolExpired: Schema.Boolean,
        oldSocketStateAtActive: Schema.Number,
        oldSocketStateAtCompletion: Schema.Number,
        clientPeakSockets: Schema.Number,
        falseReplacements: Schema.Number,
        extraRequests: Schema.Number,
        isolation: Schema.Unknown,
        serverPeakSockets: Schema.Number,
      }),
    }),
  ]),
);

const { values } = NodeUtil.parseArgs({
  options: {
    smoke: { type: "boolean" },
    lifecycle: { type: "boolean" },
    lanes: { type: "string", default: "1" },
    cpus: { type: "string" },
    coordinator: { type: "boolean" },
    "fresh-workers": { type: "boolean" },
    warmups: { type: "string", default: "2" },
    reduced: { type: "boolean" },
    "variant-options": { type: "string" },
    "calibrate-only": { type: "boolean" },
    manifest: { type: "string" },
    "write-manifest": { type: "string" },
    variants: { type: "string" },
    out: { type: "string" },
    "dry-run": { type: "boolean" },
    help: { type: "boolean", short: "h" },
  },
});
if (values.help) {
  console.log(`node apps/server/bench/network/run.ts [--smoke] [--calibrate-only]
  --write-manifest FILE  Freeze inputs then exit; --smoke creates the small matrix
  --reduced              HTTPS/two routes, n=2 deterministic / n=20 loss, four workloads
  --lifecycle            Focused lifecycle cells, n=2, including loaded-host delay controls
  --lanes N              Independent calibrated namespaces, default 1, maximum 18
  --fresh-workers        Serial reference: restart both processes after each case
  --cpus 0,1;2,3         Optional per-lane CPU sets. Quote the argument in a shell.
  --variant-options FILE JSON mapping variant names to switches
  --manifest FILE        Use exactly these frozen inputs
  --variants A=/abs/worktree,B=/abs/worktree  Clean worktrees containing this bench
  --out DIRECTORY        Durable raw rows, manifest, calibration, and summary
  --dry-run              Show case count and duration estimate, with no packets
Every execution calibrates first, in a private network and PID namespace.
HTTP = LAN/tailnet IP. HTTPS = Node TLS 1.3 stand-in for tailscaled termination.`);
  process.exit(0);
}
const manifest = values.manifest
  ? validateManifest(JSON.parse(NodeFS.readFileSync(values.manifest, "utf8")))
  : values.lifecycle
    ? makeLifecycleManifest()
    : values.reduced
      ? makeReducedManifest()
      : makeManifest(values.smoke);
const hash = manifestHash(manifest);
if (values["write-manifest"]) {
  NodeFS.writeFileSync(values["write-manifest"], JSON.stringify(manifest, null, 2) + "\n");
  process.exit(0);
}
const root = NodePath.resolve(import.meta.dirname, "../../../..");
const optionsByVariant = values["variant-options"]
  ? Schema.decodeUnknownSync(Schema.Record(Schema.String, VariantOptions))(
      JSON.parse(NodeFS.readFileSync(values["variant-options"], "utf8")),
    )
  : {};
const variants = (values.variants ?? `baseline=${root}`).split(",").map((spec) => {
  const index = spec.indexOf("=");
  if (index < 1) throw new Error("Variants must be named absolute worktree paths");
  const name = spec.slice(0, index),
    path = spec.slice(index + 1);
  if (
    !NodePath.isAbsolute(path) ||
    !NodeFS.existsSync(NodePath.join(path, "apps/server/bench/network/reconnectWorker.ts"))
  )
    throw new Error(`Missing benchmark in ${path}`);
  return { name, path, options: optionsByVariant[name] ?? defaultVariantOptions };
});
if (
  new Set(variants.map((v) => v.name)).size !== variants.length ||
  new Set(variants.map((v) => NodeFS.realpathSync(v.path))).size !== variants.length
)
  throw new Error("Variant names and worktrees must be distinct");
const git = (path: string, args: string[]) =>
  NodeChildProcess.execFileSync("git", ["-C", path, ...args], { encoding: "utf8" }).trim();
const state = (path: string) => ({
  revision: git(path, ["rev-parse", "HEAD"]),
  dirty: git(path, ["status", "--porcelain"]).length > 0,
});
const laneCount = Number(values.lanes);
const estimatedSeconds =
  (manifest.cases.reduce(
    (sum, c) =>
      sum +
      0.15 +
      ((c.profile.down.delayMs + c.profile.up.delayMs) * 12) / 1000 +
      (c.configDelayMs ?? 0) / 1000 +
      (["short-probe", "network-changed"].includes(c.workload) ? 3 : 0) +
      (c.workload.startsWith("warm-")
        ? ((c.profile.down.delayMs + c.profile.up.delayMs) * 12) / 1000 + 1
        : 0) +
      (c.profile.stochastic ? 4 : 0) +
      (c.profile.schedule.at(-1)?.atMs ?? 0) / 1000,
    0,
  ) *
    variants.length) /
    laneCount +
  new Set(manifest.cases.map((c) => c.profile.name)).size * 6.2 +
  34;
if (values["dry-run"]) {
  console.log(
    JSON.stringify(
      {
        hash,
        casesPerVariant: manifest.cases.length,
        variants: variants.length,
        lanes: laneCount,
        totalCases: manifest.cases.length * variants.length,
        estimatedMinutes: estimatedSeconds / 60,
        deadlineBoundMinutes:
          (manifest.cases.reduce((sum, c) => sum + c.timeoutMs + 30000, 0) * variants.length) /
            60000 +
          3,
      },
      null,
      2,
    ),
  );
  process.exit(0);
}
const frozenRevisions = new Map(
  variants.map((variant) => [variant.path, state(variant.path).revision]),
);
for (const variant of variants)
  if (state(variant.path).dirty)
    throw new Error(`Refusing dirty worktree ${variant.name}: ${variant.path}`);
const assignments = scheduleLanes(manifest.cases, variants, laneCount);
const cpuSets = values.cpus?.split(";");
if (cpuSets && (cpuSets.length !== laneCount || cpuSets.some((s) => !/^\d+(?:[-,]\d+)*$/.test(s))))
  throw new Error("--cpus needs one CPU set per lane");
if (!values.coordinator) {
  const child = NodeChildProcess.spawn(
    "flock",
    [
      "-n",
      NodePath.join(NodeOS.homedir(), "codex-tmp", "reconnect-benchmark.lock"),
      process.execPath,
      ...process.execArgv,
      import.meta.filename,
      ...process.argv.slice(2),
      "--coordinator",
    ],
    { stdio: "inherit" },
  );
  process.once("SIGTERM", () => child.kill("SIGTERM"));
  process.once("SIGINT", () => child.kill("SIGTERM"));
  process.exit(
    await new Promise<number>((resolve, reject) => {
      child.once("error", reject);
      child.once("exit", (code) => resolve(code ?? 1));
    }),
  );
}
const out = NodePath.resolve(
  values.out ?? NodePath.join(NodeOS.homedir(), "codex-tmp", `reconnect-${Date.now()}`),
);
NodeFS.mkdirSync(out, { recursive: true });
const scratch = NodeFS.mkdtempSync(NodePath.join(NodeOS.homedir(), "codex-tmp", "reconnect-work-"));
const cert = NodePath.join(scratch, "cert.pem"),
  key = NodePath.join(scratch, "key.pem");
const active: NodeChildProcess.ChildProcess[] = [];
const cleanup = async () => {
  await Promise.all(active.map(stopChild));
  NodeFS.rmSync(scratch, { recursive: true, force: true });
};
process.once("SIGTERM", () => {
  void cleanup().finally(() => process.exit(143));
});
process.on("message", (message) => {
  if (message === "interrupt") void cleanup().finally(() => process.exit(143));
});
const rows: ReconnectRow[] = [];
const write = (name: string, value: unknown) => {
  const target = NodePath.join(out, name);
  NodeFS.writeFileSync(target + ".next", JSON.stringify(value, null, 2) + "\n");
  NodeFS.renameSync(target + ".next", target);
};
try {
  write("manifest.json", manifest);
  write("environment.json", {
    node: process.version,
    versions: process.versions,
    platform: Effect.runSync(HostProcess.Platform),
    arch: Effect.runSync(HostProcess.Architecture),
    kernel: NodeOS.release(),
    cpu: NodeOS.cpus()[0]?.model,
    laneCount,
    cpuSets,
    warmups: Number(values.warmups),
    freshWorkers: values["fresh-workers"] === true,
    variants: variants.map((variant) => ({ ...variant, ...state(variant.path) })),
  });
  NodeChildProcess.execFileSync(
    "openssl",
    [
      "req",
      "-x509",
      "-newkey",
      "ec",
      "-pkeyopt",
      "ec_paramgen_curve:prime256v1",
      "-nodes",
      "-keyout",
      key,
      "-out",
      cert,
      "-days",
      "10",
      "-subj",
      "/CN=localhost",
      "-addext",
      "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:127.0.0.2",
    ],
    { stdio: "ignore" },
  );
  const startedAt = Date.now();
  const lanes = active;
  const mailboxes: ReturnType<typeof mailbox>[] = [];
  const calibration = await Promise.all(
    assignments.map(async (_tasks, lane) => {
      const laneScratch = NodePath.join(scratch, `lane-${lane}`);
      NodeFS.mkdirSync(laneScratch);
      for (const name of ["cert.pem", "key.pem"])
        NodeFS.copyFileSync(NodePath.join(scratch, name), NodePath.join(laneScratch, name));
      const laneArgs = [
        "unshare",
        "--user",
        "--map-root-user",
        "--net",
        "--pid",
        "--fork",
        "--kill-child=SIGTERM",
        process.execPath,
        ...process.execArgv,
        new URL("./reconnectLane.ts", import.meta.url).pathname,
        NodePath.join(out, "manifest.json"),
        laneScratch,
        String(lane),
        values["fresh-workers"] ? "fresh" : "persistent",
      ];
      const child = NodeChildProcess.spawn(
        cpuSets ? "taskset" : laneArgs.shift()!,
        cpuSets ? ["-c", cpuSets[lane]!, ...laneArgs] : laneArgs,
        {
          stdio: ["ignore", "inherit", "inherit", "ipc"],
          env: { ...process.env, NETBENCH_HOST_NETNS: NodeFS.readlinkSync("/proc/self/ns/net") },
        },
      );
      lanes[lane] = child;
      const inbox = mailbox(child);
      mailboxes[lane] = inbox;
      const ready = await inbox.next(240000);
      return ready;
    }),
  );
  write("calibration.json", calibration);
  const Calibration = Schema.Struct({
    type: Schema.Literal("calibrated"),
    calibration: Schema.Struct({ correct: Schema.Boolean }),
  });
  for (const ready of calibration)
    if (!Schema.decodeUnknownSync(Calibration)(ready).calibration.correct)
      throw new Error("Calibration failed");
  const calibrationMs = Date.now() - startedAt;
  const caseStart = Date.now();
  const warmupRows: unknown[] = [];
  const warmupCount = Number(values.warmups);
  if (!Number.isSafeInteger(warmupCount) || warmupCount < 0 || warmupCount > 20)
    throw new Error("warmups must be 0..20");
  if (!values["calibrate-only"]) {
    await Promise.all(
      assignments.map(async (tasks, lane) => {
        const warmed = new Set<string>();
        for (const [groupIndex, task] of tasks.entries())
          for (const variant of task.variants) {
            const provenance = state(variant.path);
            if (provenance.dirty || provenance.revision !== frozenRevisions.get(variant.path))
              throw new Error(`Worktree changed during suite: ${variant.name}`);
            const epoch = `${process.pid}-${lane}-${groupIndex}-${variant.name}`;
            if (!values["fresh-workers"] && !warmed.has(variant.name)) {
              for (let warmup = 0; warmup < warmupCount; warmup++) {
                const entry = warmupCase(lane, variant.name, warmup);
                lanes[lane]!.send({
                  path: variant.path,
                  epoch: `${epoch}-warmup-${warmup}`,
                  entry,
                  options: variant.options,
                });
                const result = decodeWorker(await mailboxes[lane]!.next(125000));
                warmupRows.push({ lane, variant: variant.name, entry, ...result });
                write("warmups.json", warmupRows);
                if (result.type === "failure") throw new Error(`Warmup failed: ${result.error}`);
              }
              warmed.add(variant.name);
            }
            lanes[lane]!.send({
              path: variant.path,
              epoch,
              entry: task.entry,
              options: variant.options,
            });
            const result = decodeWorker(await mailboxes[lane]!.next(task.entry.timeoutMs + 65000));
            const row: ReconnectRow = {
              variant: variant.name,
              ...provenance,
              manifestHash: hash,
              lane,
              case: task.entry,
              ...(result.type === "result"
                ? { result: result.result, controller: result.controller, wallMs: result.wallMs }
                : { error: result.error }),
            };
            const after = state(variant.path);
            if (after.dirty || after.revision !== provenance.revision)
              throw new Error(`Variant changed: ${variant.name}`);
            rows.push(row);
            write("rows.json", rows);
            write("summary.json", summarize(rows));
            console.log(
              `${rows.length}/${manifest.cases.length * variants.length} lane=${lane} ${variant.name} ${task.entry.id}: ${row.result ? row.result.totalMs.toFixed(1) + " ms" : row.error}`,
            );
            if (result.type === "failure")
              throw new Error(`Poisoned lane ${lane}: ${result.error}`);
          }
      }),
    );
  }
  write("runtime.json", {
    startedAt: new Date(startedAt).toISOString(),
    finishedAt: new Date().toISOString(),
    lanes: laneCount,
    cases: rows.length,
    calibrationMs,
    casesMs: Date.now() - caseStart,
    totalMs: Date.now() - startedAt,
    wallMsPerCase: (Date.now() - caseStart) / rows.length,
  });
  for (const lane of lanes) lane.send({ stop: true });
} finally {
  await cleanup();
  process.disconnect?.();
}
