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

import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

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
import { type Metrics, Recorder, workloads } from "./workloads.ts";

const { values } = NodeUtil.parseArgs({
  options: {
    layer: { type: "string", default: "raw" },
    scenarios: { type: "string" },
    transports: { type: "string" },
    workloads: { type: "string" },
    timeout: { type: "string", default: "60" },
    repeat: { type: "string", default: "1" },
    out: { type: "string" },
    help: { type: "boolean", short: "h" },
  },
});
if (values.help) {
  console.log(`Usage: node apps/server/bench/network/run.ts [options]
  --layer raw|rpc       Raw framing or Effect RPC (default: raw)
  --scenarios LIST      ${presets.map((s) => s.name).join(",")}
                        Custom: slow:delay=150,jitter=15,loss=2%,nthDelay=3/300
  --transports LIST     raw: ${transportNames.join(",")}
                        rpc: ${rpcTransportNames.join(",")}
  --workloads LIST      connect,rpc,stream,fanout,hol
  --timeout SECONDS     Deadline per case (default: 60)
  --repeat N            Repeat each case (default: 1)
  --out PATH            Save JSON after each case, including partial results
Linux only; needs unshare, ip, tc, iptables, ethtool, sysctl, and openssl.
All packet impairments run inside a private network namespace.`);
  process.exit(0);
}
const list = (value: string | undefined) => value?.split(",").map((entry) => entry.trim());
const timeoutMs = Number(values.timeout) * 1000;
const repeat = Number(values.repeat);
if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) throw new Error("--timeout must be positive");
if (!Number.isInteger(repeat) || repeat < 1) throw new Error("--repeat must be a positive integer");
const layer = values.layer;
if (layer !== "raw" && layer !== "rpc") throw new Error(`Unknown layer "${layer}"; use raw or rpc`);
const scenarios = values.scenarios ? parseScenarios(values.scenarios) : presets;

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

const knownTransports: ReadonlyArray<string> = layer === "raw" ? transportNames : rpcTransportNames;
const transports = list(values.transports) ?? [...knownTransports];
for (const transport of transports) {
  if (!knownTransports.includes(transport))
    throw new Error(`Unknown ${layer} transport "${transport}"`);
}
// Both layers' workloads share names and shapes, so results line up across layers. Each run
// resolves "timeout" when cut off at the deadline; its recorder still holds what it measured.
const layerWorkloads: ReadonlyArray<{
  readonly name: string;
  readonly run: (
    transport: string,
    tls: TlsMaterial,
    record: Recorder,
  ) => Promise<"done" | "timeout">;
}> =
  layer === "raw"
    ? workloads.map((workload) => ({
        name: workload.name,
        run: (transport, tls, record) => {
          let timer: ReturnType<typeof setTimeout> | undefined;
          return Promise.race([
            workload
              .run(transport as (typeof transportNames)[number], tls, record)
              .then(() => "done" as const),
            new Promise<"timeout">(
              (resolve) => (timer = setTimeout(() => resolve("timeout"), timeoutMs)),
            ),
          ]).finally(() => {
            clearTimeout(timer);
            record.abort();
          });
        },
      }))
    : rpcWorkloads.map((workload) => ({
        name: workload.name,
        run: (transport, tls, record) =>
          Effect.runPromise(
            workload.run(transport as (typeof rpcTransportNames)[number], tls, record).pipe(
              Effect.timeoutOption(timeoutMs),
              Effect.map((result) =>
                Option.isSome(result) ? ("done" as const) : ("timeout" as const),
              ),
            ),
          ),
      }));
const workloadNames = list(values.workloads);
for (const name of workloadNames ?? []) {
  if (!layerWorkloads.some((workload) => workload.name === name))
    throw new Error(`Unknown workload "${name}"`);
}
const selectedWorkloads = workloadNames
  ? layerWorkloads.filter((workload) => workloadNames.includes(workload.name))
  : layerWorkloads;

const scratchRoot = NodePath.join(NodeOS.homedir(), "codex-tmp");
NodeFS.mkdirSync(scratchRoot, { recursive: true });
const tlsDir = NodeFS.mkdtempSync(NodePath.join(scratchRoot, "netbench-"));
const certPath = NodePath.join(tlsDir, "cert.pem");
const keyPath = NodePath.join(tlsDir, "key.pem");
let activeServer: NodeChildProcess.ChildProcess | undefined;
let activeRecorder: Recorder | undefined;

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

  async function startServer() {
    const child = NodeChildProcess.fork(
      NodePath.join(import.meta.dirname, "server.ts"),
      [certPath, keyPath],
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
    readonly layer: string;
    readonly transport: string;
    readonly workload: string;
    readonly repetition: number;
    readonly metrics: Metrics;
  }
  const rows: Row[] = [];
  const metadata = {
    revision: NodeChildProcess.execFileSync("git", ["rev-parse", "HEAD"], {
      encoding: "utf8",
    }).trim(),
    node: process.version,
    kernel: NodeOS.release(),
    startedAt: DateTime.formatIso(DateTime.nowUnsafe()),
    timeoutMs,
    repeat,
  };
  const save = () => {
    if (values.out)
      NodeFS.writeFileSync(
        values.out,
        `${JSON.stringify({ metadata, scenarios, rows }, null, 2)}\n`,
      );
  };
  save();

  for (const scenario of scenarios) {
    console.log(`\n## ${scenario.name} (${describeScenario(scenario)})`);
    for (const workload of selectedWorkloads) {
      for (const transport of transports) {
        for (let repetition = 1; repetition <= repeat; repetition++) {
          // A fresh server per run: a timed-out run can leave streams sending into the next one.
          applyScenario({ name: "clean" });
          await startServer();
          applyScenario(scenario);
          const record = new Recorder();
          activeRecorder = record;
          const outcome = await workload
            .run(transport, tls, record)
            .catch((error: unknown) => `error: ${String(error)}`);
          const metrics: Metrics = {
            ...(outcome === "done" ? {} : { outcome }),
            ...record.metrics(),
          };
          await stopServer();
          rows.push({
            scenario: scenario.name,
            layer,
            transport,
            workload: workload.name,
            repetition,
            metrics,
          });
          save();
          console.log(
            `  ${workload.name.padEnd(8)} ${transport.padEnd(12)} ${repeat > 1 ? `#${repetition} ` : ""}${format(metrics)}`,
          );
        }
      }
    }
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
process.exit(0);
