// @effect-diagnostics nodeBuiltinImport:off globalConsole:off globalTimers:off globalDate:off - Owns isolated fixture processes and benchmark output.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { makeManifest, manifestHash, validateManifest, variantOrder } from "./reconnectManifest.ts";
import { applyProfile } from "./reconnectNetwork.ts";
import {
  awaitBenchmarkStart,
  benchmarkLockArguments,
  controlBenchmarkCommand,
  receive,
  stopChild,
} from "./reconnectProcess.ts";
import { prepareLoopback } from "./impairments.ts";
import { calibrate } from "./reconnectCalibration.ts";
import { ReconnectResult, summarize, type ReconnectRow } from "./reconnectComparison.ts";
import { backendProvenance } from "./reconnectNativeDiagnostics.ts";

const decodeWorker = Schema.decodeUnknownSync(
  Schema.Union([
    Schema.Struct({
      type: Schema.Literal("failure"),
      error: Schema.String,
      diagnostics: Schema.optionalKey(Schema.Unknown),
    }),
    Schema.Struct({
      type: Schema.Literal("result"),
      result: ReconnectResult,
      controller: Schema.Unknown,
      diagnostics: Schema.optionalKey(Schema.Unknown),
    }),
  ]),
);
const { values } = NodeUtil.parseArgs({
  options: {
    smoke: { type: "boolean" },
    measure: { type: "boolean" },
    operations: { type: "boolean" },
    fallback: { type: "boolean" },
    overlap: { type: "boolean" },
    manifest: { type: "string" },
    "write-manifest": { type: "string" },
    variants: { type: "string" },
    transports: { type: "string" },
    out: { type: "string" },
    "dry-run": { type: "boolean" },
    "calibrate-only": { type: "boolean" },
    "wait-lock": { type: "boolean" },
    help: { type: "boolean", short: "h" },
  },
});
if (values.help) {
  console.log(`node apps/server/bench/network/reconnectRun.ts [--smoke] --out DIRECTORY
  --smoke               Four WSS integration cases, 50ms RTT, one seed (default)
  --operations          Sync-five, replay 50 messages and cold long-thread open
  --fallback            Drop fixture UDP; verify first fallback and persistent cooldown
  --overlap             Warm deliberate bulk RPC and eight-chunk terminal streaming stress
  --measure             Sixty resume or 45 operation inputs: clean/50msRTT/2% loss, four workloads, five seeds
  --manifest FILE       Exact frozen inputs, including custom subsets
  --write-manifest FILE Write inputs and exit without starting listeners
  --variants A=/worktree,B=/worktree  Pair clean committed worktrees, rotating order
  --transports websocket,webtransport Pair the same revision with/without its real connector
  --dry-run             Print manifest hash and planned case counts
  --calibrate-only      Verify packetization, profile RTT and directional rate; no application cases
  --wait-lock           Queue on the shared host lock before starting the guarded namespace
Uses production client resolver/auth/session/supervisor and thread/shell reducers.
Supply websocket or webtransport, with actual selection checked for every cold connection.
Every fixture runs in a private user/network/PID namespace with MTU 1280.`);
  process.exit(0);
}
if ([values.operations, values.fallback, values.overlap].filter(Boolean).length > 1)
  throw new Error("Choose --operations or --fallback");
if (values.smoke && values.measure) throw new Error("Choose --smoke or --measure");
const manifest = values.manifest
  ? validateManifest(JSON.parse(NodeFS.readFileSync(values.manifest, "utf8")))
  : makeManifest(
      values.measure,
      values.operations
        ? "operations"
        : values.fallback
          ? "fallback"
          : values.overlap
            ? "overlap"
            : "resume",
    );
const hash = manifestHash(manifest);
if (values["write-manifest"]) {
  NodeFS.writeFileSync(values["write-manifest"], JSON.stringify(manifest, null, 2) + "\n", {
    flag: "wx",
  });
  process.exit(0);
}
const root = NodePath.resolve(import.meta.dirname, "../../../..");
const baseVariants = (values.variants ?? `baseline=${root}`).split(",").map((spec) => {
  const index = spec.indexOf("=");
  const name = spec.slice(0, index),
    path = spec.slice(index + 1);
  if (
    index < 1 ||
    !NodePath.isAbsolute(path) ||
    !NodeFS.existsSync(NodePath.join(path, "apps/server/bench/network/reconnectWorker.ts"))
  )
    throw new Error("Variants require named absolute worktrees containing the benchmark");
  return { name, path };
});
if (
  new Set(baseVariants.map((v) => v.name)).size !== baseVariants.length ||
  new Set(baseVariants.map((v) => NodeFS.realpathSync(v.path))).size !== baseVariants.length
)
  throw new Error("Variant names and worktrees must be distinct");
const transports = (values.transports ?? "websocket").split(",");
if (
  !transports.length ||
  new Set(transports).size !== transports.length ||
  transports.some((mode) => mode !== "websocket" && mode !== "webtransport")
)
  throw new Error("--transports requires websocket and/or webtransport");
const variants = baseVariants.flatMap((variant) =>
  transports.map((transport) => ({ ...variant, name: `${variant.name}/${transport}`, transport })),
);
if (values["dry-run"]) {
  console.log(
    JSON.stringify(
      {
        hash,
        casesPerVariant: manifest.cases.length,
        totalCases: manifest.cases.length * variants.length,
        manifest,
      },
      null,
      2,
    ),
  );
  process.exit(0);
}
const git = (path: string, args: string[]) =>
  NodeChildProcess.execFileSync("git", ["-C", path, ...args], { encoding: "utf8" }).trim();
const state = (path: string) => ({
  revision: git(path, ["rev-parse", "HEAD"]),
  dirty: git(path, ["status", "--porcelain"]).length > 0,
});
const frozen = new Map(variants.map((v) => [v.path, state(v.path).revision]));
for (const variant of variants)
  if (state(variant.path).dirty) throw new Error(`Refusing dirty worktree ${variant.name}`);
const out = NodePath.resolve(
  values.out ?? NodePath.join(NodeOS.homedir(), "codex-tmp", `rpc-reconnect-${Date.now()}`),
);
if (process.env.RPC_RECONNECT_NETNS !== "1") {
  if (NodeFS.existsSync(out)) throw new Error("Output exists; choose a fresh --out directory");
  if (values["wait-lock"])
    console.log("Waiting for the shared reconnect benchmark lock; no fixture has started.");
  const child = NodeChildProcess.spawn(
    "flock",
    benchmarkLockArguments(
      values["wait-lock"] === true,
      NodePath.join(NodeOS.homedir(), "codex-tmp", "reconnect-benchmark.lock"),
      [
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
        ...(values.out ? [] : ["--out", out]),
      ],
    ),
    {
      stdio: ["inherit", "inherit", "inherit", "ipc"],
      env: {
        ...process.env,
        RPC_RECONNECT_NETNS: "1",
        NETBENCH_HOST_NETNS: NodeFS.readlinkSync("/proc/self/ns/net"),
      },
    },
  );
  const command = controlBenchmarkCommand(child);
  process.once("SIGINT", () => command.interrupt("SIGINT"));
  process.once("SIGTERM", () => command.interrupt("SIGTERM"));
  const exitCode = await command.done;
  if (exitCode === 75)
    console.error(
      "Another reconnect benchmark holds the shared host lock; no fixture was started.",
    );
  process.exit(exitCode);
}
NodeFS.mkdirSync(out, { recursive: true });
const scratch = NodeFS.mkdtempSync(
  NodePath.join(NodeOS.homedir(), "codex-tmp", "rpc-reconnect-work-"),
);
const cert = NodePath.join(scratch, "cert.pem"),
  key = NodePath.join(scratch, "key.pem");
let active: NodeChildProcess.ChildProcess | undefined;
const cleanup = async () => {
  await stopChild(active);
  NodeFS.rmSync(scratch, { recursive: true, force: true });
};
let stopping = false;
const interrupted = () => {
  if (stopping) return;
  stopping = true;
  void cleanup().finally(() => process.exit(143));
};
process.once("SIGTERM", interrupted);
process.once("SIGINT", interrupted);
process.once("disconnect", interrupted);
process.on("message", (message) => {
  if (message === "interrupt") interrupted();
});
const rows: ReconnectRow[] = [];
const write = (name: string, value: unknown) => {
  const target = NodePath.join(out, name);
  NodeFS.writeFileSync(target + ".next", JSON.stringify(value, null, 2) + "\n");
  NodeFS.renameSync(target + ".next", target);
};
let complete = false;
const save = () => {
  write("rows.json", rows);
  write("summary.json", {
    complete,
    expected: values["calibrate-only"] ? 0 : manifest.cases.length * variants.length,
    attempted: rows.length,
    groups: summarize(rows, variants[0]!.name),
  });
};
try {
  await awaitBenchmarkStart();
  prepareLoopback(scratch, true, undefined, manifest.mtu);
  write("manifest.json", manifest);
  write("environment.json", {
    node: process.version,
    versions: process.versions,
    kernel: NodeOS.release(),
    cpu: NodeOS.cpus()[0]?.model,
    cpuCount: NodeOS.cpus().length,
    loadAtStart: NodeOS.loadavg(),
    variants: variants.map((v) => ({ ...v, ...state(v.path) })),
    links: JSON.parse(
      NodeChildProcess.execFileSync("ip", ["-json", "link", "show"], { encoding: "utf8" }),
    ),
    resourceWindow: "whole-client-case-including-short-resume-warmup",
    diagnosticTrace: process.env.RPC_RECONNECT_TRACE === "1",
    lockAcquisition: values["wait-lock"] ? "blocking-queue" : "nonblocking",
    backendProvenance: await Effect.runPromise(
      Effect.gen(function* () {
        return backendProvenance(yield* HostProcess.Platform, yield* HostProcess.Architecture);
      }),
    ),
    limits:
      "Node fixture; no browser/mobile/DERP/battery claim; rate/RTT calibration and packet impairment self-test precede measurements",
  });
  const calibration = await calibrate(manifest, (message) => console.log(message));
  write("calibration.json", calibration);
  if (!calibration.correct) throw new Error("Calibration failed; no application cases started");
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
  for (const entry of values["calibrate-only"] ? [] : manifest.cases)
    for (const variant of variantOrder(variants, entry)) {
      const provenance = state(variant.path);
      if (provenance.dirty || provenance.revision !== frozen.get(variant.path))
        throw new Error(`Variant changed: ${variant.name}`);
      const entryFile = NodePath.join(scratch, "case.json");
      NodeFS.writeFileSync(entryFile, JSON.stringify(entry));
      applyProfile(entry.profile, entry.seed, manifest.queuePackets);
      active = NodeChildProcess.fork(
        NodePath.join(variant.path, "apps/server/bench/network/reconnectWorker.ts"),
        [entryFile, cert, key, scratch, String(manifest.queuePackets), variant.transport],
        {
          cwd: variant.path,
          stdio: ["ignore", "inherit", "inherit", "ipc"],
          env: { ...process.env, NODE_EXTRA_CA_CERTS: cert },
        },
      );
      let row: ReconnectRow;
      try {
        const result = decodeWorker(await receive(active, entry.timeoutMs + 35000));
        row = {
          variant: variant.name,
          ...provenance,
          manifestHash: hash,
          case: entry,
          ...(result.diagnostics === undefined ? {} : { diagnostics: result.diagnostics }),
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
      save();
      console.log(
        `${rows.length}/${manifest.cases.length * variants.length} ${variant.name} ${entry.id}: ${row.result ? `${row.result.totalMs.toFixed(1)} ms, ${row.result.selectedTransport}` : row.error}`,
      );
    }
  complete = true;
  save();
  if (rows.some((row) => row.error || !row.result?.correct)) process.exitCode = 1;
} finally {
  await cleanup();
  process.off("disconnect", interrupted);
  process.disconnect?.();
}
