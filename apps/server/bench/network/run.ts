// @effect-diagnostics nodeBuiltinImport:off globalConsole:off globalTimers:off - Bench runner owning a netns, a child server, and run deadlines; Effect scheduling would sit in the measured path.
// Run with: node apps/server/bench/network/run.ts [--layer raw|rpc] [--scenarios wan,loss2]
//   [--transports ws,wt-multi] [--workloads rpc,hol] [--timeout 60] [--out results.json]
// Compares WebSocket/TCP with WebTransport/QUIC under impaired networks. `--layer raw` measures
// the transports with minimal framing; `--layer rpc` runs the app's Effect RPC stack over them. The runner re-executes
// itself in a rootless network namespace (`unshare -rn`), so tc/iptables impairments touch nothing
// outside the bench: not the host's loopback, other dev servers, or a running T3 Code.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";

import * as HostProcess from "@t3tools/shared/HostProcess";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { RpcClientError } from "effect/rpc";
import * as Schema from "effect/Schema";

import { transportNames, type TlsMaterial } from "./client.ts";
import {
  applyScenario,
  describeScenario,
  parseScenarios,
  prepareLoopback,
  presets,
} from "./impairments.ts";
import { rpcTransportNames } from "./rpcClient.ts";
import { rpcWorkloads } from "./rpcWorkloads.ts";
import { runOperation } from "./operationsWorkloads.ts";
import {
  OperationControlResponse,
  operationNames,
  type OperationName,
} from "./operationsFixture.ts";
import { type Metrics, Recorder, workloads } from "./workloads.ts";
import { compareSuites, SuiteResults, validateSuiteResults } from "./comparison.ts";
import { type BenchCase, makeOperationSuitePlan, makeSuitePlan, workloadNames } from "./suite.ts";

const isRpcClientError = Schema.is(RpcClientError.RpcClientError);
const decodeOperationControl = Schema.decodeUnknownEffect(
  Schema.toCodecJson(OperationControlResponse),
);

const { values } = NodeUtil.parseArgs({
  options: {
    layer: { type: "string" },
    scenarios: { type: "string" },
    transports: { type: "string" },
    workloads: { type: "string" },
    timeout: { type: "string" },
    repeat: { type: "string" },
    seed: { type: "string" },
    suite: { type: "boolean" },
    "suite-name": { type: "string" },
    "dry-run": { type: "boolean" },
    baseline: { type: "string" },
    out: { type: "string" },
    help: { type: "boolean", short: "h" },
  },
});
if (values.help) {
  console.log(`Usage: node apps/server/bench/network/run.ts [options]
  --layer raw|rpc|operations  Raw framing, synthetic RPC, or production thread sync
  --scenarios LIST      ${presets.map((s) => s.name).join(",")}
                        Custom: slow:delay=150,jitter=15,loss=2%,nthDelay=3/300
  --transports LIST     raw: ${transportNames.join(",")}
                        rpc: ${rpcTransportNames.join(",")}
  --workloads LIST      connect,rpc,stream,fanout,hol
                        operations: ${operationNames.join(",")}
  --timeout SECONDS     Deadline per case (default: 60)
  --repeat N            Repeat each case (default: 1)
  --out PATH            Save JSON after each case, including partial results
  --seed INTEGER        Seed netem's loss and jitter for a manual run
  --suite               Run the complete fixed network-v1 matrix
  --suite-name NAME     network-v1 or operations-v1 (requires --suite)
  --dry-run             Print the suite manifest without running packets
  --baseline PATH       Compare a suite run with a previous suite result
Linux only; needs unshare, ip, tc, iptables, ethtool, sysctl, and openssl.
All packet impairments run inside a private network namespace.`);
  process.exit(0);
}
const list = (value: string | undefined) => value?.split(",").map((entry) => entry.trim());
const timeoutMs = Number(values.timeout ?? 60) * 1000;
const repeat = Number(values.repeat ?? 1);
if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) throw new Error("--timeout must be positive");
if (!Number.isInteger(repeat) || repeat < 1) throw new Error("--repeat must be a positive integer");
const layer = values.layer ?? "raw";
if (layer !== "raw" && layer !== "rpc" && layer !== "operations")
  throw new Error(`Unknown layer "${layer}"; use raw, rpc, or operations`);
const scenarios = values.scenarios ? parseScenarios(values.scenarios) : presets;
const seed = values.seed === undefined ? undefined : Number(values.seed);
if (seed !== undefined && (!Number.isInteger(seed) || seed < 1 || seed >= 0xffffffff))
  throw new Error("--seed must be an integer between 1 and 4294967294");
if (
  values["suite-name"] &&
  (!values.suite || !["network-v1", "operations-v1"].includes(values["suite-name"]))
)
  throw new Error("--suite-name must be network-v1 or operations-v1 and requires --suite");
const suite = values.suite
  ? values["suite-name"] === "operations-v1"
    ? makeOperationSuitePlan()
    : makeSuitePlan()
  : undefined;
if (
  suite &&
  ["layer", "scenarios", "transports", "workloads", "timeout", "repeat", "seed"].some(
    (key) => values[key as keyof typeof values] !== undefined,
  )
)
  throw new Error(
    "--suite fixes every benchmark setting; only --out, --baseline, and --dry-run may accompany it",
  );
if (!suite && (values.baseline || values["dry-run"]))
  throw new Error("--baseline and --dry-run require --suite");
if (values["dry-run"]) {
  console.log(JSON.stringify(suite, null, 2));
  process.exit(0);
}
const knownTransports: ReadonlyArray<string> = layer === "raw" ? transportNames : rpcTransportNames;
const transports = list(values.transports) ?? [...knownTransports];
for (const transport of transports) {
  if (!knownTransports.includes(transport))
    throw new Error(`Unknown ${layer} transport "${transport}"`);
}
const availableWorkloads = layer === "operations" ? operationNames : workloadNames;
const selectedWorkloads = list(values.workloads) ?? [...availableWorkloads];
for (const name of selectedWorkloads) {
  if (!(availableWorkloads as readonly string[]).includes(name))
    throw new Error(`Unknown workload "${name}"`);
}
const cases: readonly BenchCase[] =
  suite?.cases ??
  scenarios.flatMap((scenario) =>
    selectedWorkloads.flatMap((workload) =>
      transports.flatMap((transport) =>
        Array.from({ length: repeat }, (_, index) => ({
          layer,
          scenario,
          workload,
          transport,
          repetition: index + 1,
          seed,
          timeoutMs,
        })),
      ),
    ),
  );

if (process.env.NETBENCH_IN_NETNS !== "1") {
  const child = NodeChildProcess.spawn(
    "unshare",
    [
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
  // A private PID namespace also removes child servers if the runner is interrupted or crashes.
  // unshare's fork wrapper blocks termination signals while waiting. Reach the namespace's Node
  // process over IPC so it can stop its captured server and remove scratch files before exiting.
  const interrupt = () =>
    child.send("interrupt", (error) => {
      if (error) child.kill("SIGKILL");
    });
  process.once("SIGINT", interrupt);
  process.once("SIGTERM", interrupt);
  const code = await new Promise<number>((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code, signal) => resolve(code ?? (signal === "SIGTERM" ? 143 : 1)));
  });
  process.exit(code);
}

// Both layers' workloads share names and shapes, so results line up across layers. Each run
// resolves "timeout" when cut off at the deadline; its recorder still holds what it measured.
function runCase(
  entry: BenchCase,
  tls: TlsMaterial,
  record: Recorder,
): Promise<"done" | "timeout"> {
  if (entry.layer === "raw") {
    const workload = workloads.find((workload) => workload.name === entry.workload)!;
    let timer: ReturnType<typeof setTimeout> | undefined;
    return Promise.race([
      workload
        .run(entry.transport as (typeof transportNames)[number], tls, record)
        .then(() => "done" as const),
      new Promise<"timeout">(
        (resolve) => (timer = setTimeout(() => resolve("timeout"), entry.timeoutMs)),
      ),
    ]).finally(() => {
      clearTimeout(timer);
      record.abort();
    });
  }
  const operationControl = (count: number) =>
    Effect.callback<unknown>((resume) => {
      const child = activeServer!;
      const receive = (message: unknown) => resume(Effect.succeed(message));
      child.once("message", receive);
      child.send({ count }, (error) => {
        if (error) resume(Effect.die(error));
      });
      return Effect.sync(() => child.off("message", receive));
    }).pipe(Effect.flatMap(decodeOperationControl), Effect.orDie);
  const task =
    entry.layer === "operations"
      ? Effect.scoped(
          runOperation(
            entry.workload as OperationName,
            entry.transport as (typeof rpcTransportNames)[number],
            tls,
            record,
            operationControl,
          ),
        )
      : rpcWorkloads
          .find((workload) => workload.name === entry.workload)!
          .run(entry.transport as (typeof rpcTransportNames)[number], tls, record);
  return Effect.runPromise(
    task.pipe(
      Effect.timeoutOption(entry.timeoutMs),
      Effect.map((result) => (Option.isSome(result) ? ("done" as const) : ("timeout" as const))),
    ),
  );
}

const scratchRoot = NodePath.join(NodeOS.homedir(), "codex-tmp");
NodeFS.mkdirSync(scratchRoot, { recursive: true });
const tlsDir = NodeFS.mkdtempSync(NodePath.join(scratchRoot, "netbench-"));
const certPath = NodePath.join(tlsDir, "cert.pem");
const keyPath = NodePath.join(tlsDir, "key.pem");
let activeServer: NodeChildProcess.ChildProcess | undefined;
let activeRecorder: Recorder | undefined;
let exitCode = 0;

async function stopServer() {
  const child = activeServer;
  activeServer = undefined;
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  await new Promise<void>((resolve) => {
    child.once("exit", () => resolve());
    child.kill("SIGKILL");
  });
}

async function cleanup() {
  activeRecorder?.abort();
  await stopServer();
  NodeFS.rmSync(tlsDir, { recursive: true, force: true });
}
for (const [signal, code] of [
  ["SIGINT", 130],
  ["SIGTERM", 143],
] as const) {
  process.once(signal, () => {
    void cleanup().finally(() => process.exit(code));
  });
}
process.on("message", (message) => {
  if (message === "interrupt") void cleanup().finally(() => process.exit(143));
});
process.once("disconnect", () => {
  void cleanup().finally(() => process.exit(143));
});

try {
  prepareLoopback(tlsDir);
  // ECDSA and under 14 days: the constraints browsers put on serverCertificateHashes certificates.
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
      keyPath,
      "-out",
      certPath,
      "-days",
      "10",
      "-subj",
      "/CN=localhost",
      "-addext",
      "subjectAltName=DNS:localhost,IP:127.0.0.1",
    ],
    { stdio: "ignore" },
  );
  const certPem = NodeFS.readFileSync(certPath, "utf8");
  const tls: TlsMaterial = {
    certPem,
    certSha256: new Uint8Array(
      NodeCrypto.createHash("sha256").update(new NodeCrypto.X509Certificate(certPem).raw).digest(),
    ),
  };

  async function startServer(entry: BenchCase) {
    const child = NodeChildProcess.fork(
      NodePath.join(
        import.meta.dirname,
        entry.layer === "operations" ? "operationsServer.ts" : "server.ts",
      ),
      [certPath, keyPath, entry.workload],
      {
        stdio: ["ignore", "inherit", "inherit", "ipc"],
      },
    );
    activeServer = child;
    let timer: ReturnType<typeof setTimeout> | undefined;
    await new Promise<void>((resolve, reject) => {
      timer = setTimeout(() => reject(new Error("bench server readiness timed out")), 15000);
      child.once("error", reject);
      child.once("message", (message) =>
        message === "ready" ? resolve() : reject(new Error("Unexpected readiness message")),
      );
      child.once("exit", (code) => reject(new Error(`bench server exited with ${code}`)));
    }).finally(() => clearTimeout(timer));
  }

  interface Row {
    readonly scenario: string;
    readonly layer: "raw" | "rpc" | "operations";
    readonly transport: string;
    readonly workload: string;
    readonly repetition: number;
    readonly seed?: number | undefined;
    readonly timeoutMs: number;
    readonly elapsedMs: number;
    readonly metrics: Metrics;
  }
  const rows: Row[] = [];
  const startedAt = DateTime.formatIso(DateTime.nowUnsafe());
  const environment = {
    node: process.version,
    kernel: NodeOS.release(),
    arch: Effect.runSync(HostProcess.Architecture),
    platform: Effect.runSync(HostProcess.Platform),
    hostname: NodeOS.hostname(),
    cpuModel: NodeOS.cpus()[0]?.model,
    cpuCount: NodeOS.cpus().length,
    tc: NodeChildProcess.execFileSync("tc", ["-V"], { encoding: "utf8" }).trim(),
    iptables: NodeChildProcess.execFileSync("iptables", ["-V"], { encoding: "utf8" }).trim(),
  };
  const metadata = {
    revision: NodeChildProcess.execFileSync("git", ["rev-parse", "HEAD"], {
      encoding: "utf8",
    }).trim(),
    dirty:
      NodeChildProcess.execFileSync("git", ["status", "--porcelain", "--untracked-files=no"], {
        encoding: "utf8",
      }).trim().length > 0,
    lockfileHash: NodeCrypto.createHash("sha256")
      .update(
        NodeFS.readFileSync(NodePath.resolve(import.meta.dirname, "../../../../pnpm-lock.yaml")),
      )
      .digest("hex"),
    environment,
    environmentHash: NodeCrypto.createHash("sha256")
      .update(JSON.stringify(environment))
      .digest("hex"),
    startedAt,
  };
  const out =
    values.out ??
    (suite
      ? NodePath.join(scratchRoot, "network-suite", startedAt.replaceAll(":", "-"), "results.json")
      : undefined);
  if (out && values.baseline && NodePath.resolve(out) === NodePath.resolve(values.baseline))
    throw new Error("The candidate output would overwrite its baseline; choose another --out path");
  const baseline = values.baseline
    ? Schema.decodeUnknownSync(SuiteResults)(
        JSON.parse(NodeFS.readFileSync(values.baseline, "utf8")),
      )
    : undefined;
  if (baseline) {
    validateSuiteResults(baseline);
    if (baseline.suite.hash !== suite?.hash)
      throw new Error("Suite configuration differs; compare the same suite version");
    if (baseline.metadata.environmentHash !== metadata.environmentHash)
      throw new Error(
        "Benchmark environments differ; use the same host, Node, kernel, and network tools",
      );
  }
  let complete = false;
  const suiteStartedAt = performance.now();
  const result = () => ({
    metadata,
    suite,
    complete,
    elapsedMs: performance.now() - suiteStartedAt,
    scenarios: suite?.scenarios ?? scenarios,
    rows,
  });
  const save = () => {
    if (out) {
      NodeFS.mkdirSync(NodePath.dirname(out), { recursive: true });
      NodeFS.writeFileSync(`${out}.tmp`, `${JSON.stringify(result(), null, 2)}\n`);
      NodeFS.renameSync(`${out}.tmp`, out);
    }
  };
  save();
  if (suite)
    console.log(
      `${suite.version}: ${cases.length} cases, ${suite.seeds.length} repetitions, configuration ${suite.hash}\nResults: ${out}`,
    );

  let lastGroup = "";
  for (const [index, entry] of cases.entries()) {
    const group = `${entry.layer}/${entry.scenario.name}`;
    if (group !== lastGroup) {
      console.log(`\n## ${group} (${describeScenario(entry.scenario)})`);
      lastGroup = group;
    }
    const caseStartedAt = performance.now();
    // A fresh server per run: a timed-out run can leave streams sending into the next one.
    applyScenario({ name: "clean" });
    await startServer(entry);
    applyScenario(entry.scenario, entry.seed);
    const record = new Recorder();
    activeRecorder = record;
    const outcome = await runCase(entry, tls, record).catch((error: unknown) => {
      const detail =
        isRpcClientError(error) && error.reason._tag === "SocketReadError"
          ? `: ${String(error.reason.cause)}`
          : "";
      return `error: ${String(error)}${detail}`;
    });
    const invalid = record.validationError(outcome === "done");
    const metrics: Metrics = {
      ...(invalid ? { outcome: `error: ${invalid}` } : outcome === "done" ? {} : { outcome }),
      ...record.metrics(),
    };
    await stopServer();
    rows.push({
      scenario: entry.scenario.name,
      layer: entry.layer,
      transport: entry.transport,
      workload: entry.workload,
      repetition: entry.repetition,
      seed: entry.seed,
      timeoutMs: entry.timeoutMs,
      elapsedMs: performance.now() - caseStartedAt,
      metrics,
    });
    save();
    console.log(
      `  [${index + 1}/${cases.length}] ${entry.workload.padEnd(8)} ${entry.transport.padEnd(12)} #${entry.repetition} ${format(metrics)}`,
    );
  }
  complete = true;
  save();
  const completed = rows.filter((row) => row.metrics.outcome === undefined).length;
  console.log(
    `\nFinished ${rows.length} cases in ${((performance.now() - suiteStartedAt) / 60000).toFixed(1)} minutes: ${completed} completed, ${rows.length - completed} error/timeout outcomes.`,
  );
  if (baseline) {
    const comparisons = compareSuites(baseline, Schema.decodeUnknownSync(SuiteResults)(result()));
    const regressions = comparisons.filter(
      (comparison) =>
        comparison.completion === "regressed" ||
        comparison.deliveredState === "changed" ||
        comparison.metrics.some((metric) => metric.signal === "regressed"),
    );
    const reportPath = `${out}.comparison.json`;
    NodeFS.writeFileSync(
      reportPath,
      `${JSON.stringify({ baselineRevision: baseline.metadata.revision, candidateRevision: metadata.revision, comparisons }, null, 2)}\n`,
    );
    console.log(
      `Comparison: ${reportPath}\n${regressions.length} case groups need regression review. Timing signals are conservative repeat-range checks, not statistical proof.`,
    );
    if (regressions.length > 0) exitCode = 1;
  }

  /** Console view: p50/p99/max per series; the --out JSON keeps every percentile. */
  function format(metrics: Metrics) {
    return Object.entries(metrics)
      .filter(([key]) => !key.endsWith(".p95"))
      .map(
        ([key, value]) =>
          `${key}=${typeof value === "number" ? Math.round(value * 10) / 10 : value}`,
      )
      .join(" ");
  }
} finally {
  await cleanup();
}
// The WebTransport client keeps native handles alive after close.
process.exit(exitCode);
