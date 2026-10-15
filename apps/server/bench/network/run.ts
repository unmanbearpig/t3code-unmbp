// @effect-diagnostics nodeBuiltinImport:off globalConsole:off globalTimers:off globalDate:off - Outer runner owns isolated benchmark processes, not app runtime state.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import * as Schema from "effect/Schema";
import {
  makeManifest,
  validateManifest,
  manifestHash,
  balancedOrder,
} from "./reconnectManifest.ts";
import { prepareLoopback } from "./impairments.ts";
import { applyProfile } from "./reconnectNetwork.ts";
import { calibrate } from "./reconnectCalibration.ts";
import { receive, stopChild } from "./reconnectProcess.ts";
import { summarize, type ReconnectRow } from "./reconnectComparison.ts";

const decodeWorker = Schema.decodeUnknownSync(
  Schema.Union([
    Schema.Struct({ type: Schema.Literal("failure"), error: Schema.String }),
    Schema.Struct({
      type: Schema.Literal("result"),
      controller: Schema.Unknown,
      result: Schema.Struct({
        totalMs: Schema.Number,
        stages: Schema.Array(
          Schema.Struct({
            name: Schema.String,
            atMs: Schema.Number,
            endMs: Schema.optionalKey(Schema.Number),
          }),
        ),
        requests: Schema.Record(Schema.String, Schema.Number),
        bytes: Schema.Number,
        packets: Schema.Number,
        events: Schema.Number,
        sockets: Schema.Number,
        correct: Schema.Boolean,
      }),
    }),
  ]),
);

const { values } = NodeUtil.parseArgs({
  options: {
    smoke: { type: "boolean" },
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
  : makeManifest(values.smoke);
const hash = manifestHash(manifest);
if (values["write-manifest"]) {
  NodeFS.writeFileSync(values["write-manifest"], JSON.stringify(manifest, null, 2) + "\n");
  process.exit(0);
}
const root = NodePath.resolve(import.meta.dirname, "../../../..");
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
  return { name, path };
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
const estimatedSeconds =
  manifest.cases.reduce(
    (sum, c) =>
      sum +
      2 +
      ((c.profile.down.delayMs + c.profile.up.delayMs) * 12) / 1000 +
      (c.scheme === "https" ? 0.3 : 0) +
      (c.profile.stochastic ? 4 : 0) +
      (c.profile.schedule.at(-1)?.atMs ?? 0) / 1000,
    0,
  ) *
    variants.length +
  140;
if (values["dry-run"]) {
  console.log(
    JSON.stringify(
      {
        hash,
        casesPerVariant: manifest.cases.length,
        variants: variants.length,
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
for (const variant of variants)
  if (state(variant.path).dirty)
    throw new Error(`Refusing dirty worktree ${variant.name}: ${variant.path}`);
if (process.env.NETBENCH_IN_NETNS !== "1") {
  const child = NodeChildProcess.spawn(
    "flock",
    [
      "-n",
      NodePath.join(NodeOS.homedir(), "codex-tmp", "reconnect-benchmark.lock"),
      "unshare",
      "--user",
      "--map-root-user",
      "--net",
      "--pid",
      "--fork",
      "--kill-child=SIGTERM",
      process.execPath,
      ...process.execArgv,
      import.meta.filename,
      ...process.argv.slice(2),
    ],
    {
      stdio: ["inherit", "inherit", "inherit", "ipc"],
      env: {
        ...process.env,
        NETBENCH_IN_NETNS: "1",
        NETBENCH_HOST_NETNS: NodeFS.readlinkSync("/proc/self/ns/net"),
      },
    },
  );
  const interrupt = () =>
    child.send("interrupt", (error) => {
      if (error) child.kill("SIGKILL");
    });
  process.once("SIGINT", interrupt);
  process.once("SIGTERM", interrupt);
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
let active: NodeChildProcess.ChildProcess | undefined;
const cleanup = async () => {
  await stopChild(active);
  NodeFS.rmSync(scratch, { recursive: true, force: true });
};
process.once("SIGTERM", () => {
  void cleanup().finally(() => process.exit(143));
});
process.on("message", (message) => {
  if (message === "interrupt") void cleanup().finally(() => process.exit(143));
});
const rows: ReconnectRow[] = [];
const write = (name: string, value: unknown) =>
  NodeFS.writeFileSync(NodePath.join(out, name), JSON.stringify(value, null, 2) + "\n");
try {
  prepareLoopback(scratch, true, undefined, manifest.mtu);
  write("manifest.json", manifest);
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
  const calibration = await calibrate(manifest, (message) => console.log(message));
  write("calibration.json", calibration);
  if (!calibration.correct)
    throw new Error("Calibration failed; measured matrix refused. See calibration.json");
  if (!values["calibrate-only"]) {
    for (const entry of manifest.cases)
      for (const variant of balancedOrder(variants, entry.repetition - 1)) {
        const provenance = state(variant.path);
        if (provenance.dirty) throw new Error(`Worktree became dirty: ${variant.name}`);
        const entryFile = NodePath.join(scratch, "case.json");
        write("summary.json", summarize(rows));
        NodeFS.writeFileSync(entryFile, JSON.stringify(entry));
        applyProfile(entry.profile, entry.seed, manifest.queuePackets);
        active = NodeChildProcess.fork(
          NodePath.join(variant.path, "apps/server/bench/network/reconnectWorker.ts"),
          [entryFile, cert, key, scratch, String(manifest.queuePackets)],
          {
            cwd: variant.path,
            stdio: ["ignore", "inherit", "inherit", "ipc"],
            env: { ...process.env, NODE_EXTRA_CA_CERTS: cert },
          },
        );
        let row: ReconnectRow;
        try {
          const message = await receive(active, entry.timeoutMs + 35000);
          const result = decodeWorker(message);
          row = {
            variant: variant.name,
            ...provenance,
            manifestHash: hash,
            case: entry,
            ...(result.type === "result"
              ? { result: result.result, controller: result.controller }
              : { error: result.error }),
          };
        } catch (error) {
          row = {
            variant: variant.name,
            ...provenance,
            manifestHash: hash,
            case: entry,
            error: String(error),
          };
        } finally {
          await stopChild(active);
          active = undefined;
        }
        const after = state(variant.path);
        if (after.dirty || after.revision !== provenance.revision)
          throw new Error(`Variant changed during case: ${variant.name}`);
        rows.push(row);
        write("rows.json", rows);
        write("summary.json", summarize(rows));
        console.log(
          `${rows.length}/${manifest.cases.length * variants.length} ${variant.name} ${entry.id}: ${row.result ? row.result.totalMs.toFixed(1) + " ms" : row.error}`,
        );
      }
    if (rows.some((row) => row.error)) process.exitCode = 1;
  }
} finally {
  await cleanup();
  process.disconnect?.();
}
