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

import * as Effect from "effect/Effect";

import { transportNames, type TlsMaterial } from "./client.ts";
import {
  applyScenario,
  describeScenario,
  parseScenario,
  prepareLoopback,
  presets,
} from "./impairments.ts";
import { rpcTransportNames } from "./rpcClient.ts";
import { rpcWorkloads } from "./rpcWorkloads.ts";
import { type Metrics, workloads } from "./workloads.ts";

function option(name: string) {
  const index = process.argv.indexOf(`--${name}`);
  return index === -1 ? undefined : process.argv[index + 1];
}
const list = (name: string) => option(name)?.replaceAll(" ", "").split(",");

if (process.env.NETBENCH_IN_NETNS !== "1") {
  const result = NodeChildProcess.spawnSync(
    "unshare",
    ["-rn", process.execPath, ...process.execArgv, import.meta.filename, ...process.argv.slice(2)],
    { stdio: "inherit", env: { ...process.env, NETBENCH_IN_NETNS: "1" } },
  );
  process.exit(result.status ?? 1);
}

const layer = option("layer") ?? "raw";
if (layer !== "raw" && layer !== "rpc") throw new Error(`Unknown layer "${layer}"; use raw or rpc`);
const scenarios = (list("scenarios") ?? presets.map((scenario) => scenario.name)).map(
  parseScenario,
);
const knownTransports: ReadonlyArray<string> = layer === "raw" ? transportNames : rpcTransportNames;
const transports = list("transports") ?? [...knownTransports];
for (const transport of transports) {
  if (!knownTransports.includes(transport))
    throw new Error(`Unknown ${layer} transport "${transport}"`);
}
// Both layers' workloads share names and shapes, so results line up across layers.
const layerWorkloads: ReadonlyArray<{
  readonly name: string;
  readonly run: (transport: string, tls: TlsMaterial) => Promise<Metrics>;
}> =
  layer === "raw"
    ? workloads.map((workload) => ({
        name: workload.name,
        run: (transport, tls) => workload.run(transport as (typeof transportNames)[number], tls),
      }))
    : rpcWorkloads.map((workload) => ({
        name: workload.name,
        run: (transport, tls) =>
          Effect.runPromise(workload.run(transport as (typeof rpcTransportNames)[number], tls)),
      }));
const selectedWorkloads = list("workloads")
  ? layerWorkloads.filter((workload) => list("workloads")!.includes(workload.name))
  : layerWorkloads;
const timeoutMs = Number(option("timeout") ?? 60) * 1000;

prepareLoopback();
const tlsDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "netbench-tls-"));
const certPath = NodePath.join(tlsDir, "cert.pem");
const keyPath = NodePath.join(tlsDir, "key.pem");
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
  await new Promise<void>((resolve, reject) => {
    child.once("message", () => resolve());
    child.once("exit", (code) => reject(new Error(`bench server exited with ${code}`)));
  });
  return child;
}

interface Row {
  readonly scenario: string;
  readonly layer: string;
  readonly transport: string;
  readonly workload: string;
  readonly metrics: Metrics;
}
const rows: Row[] = [];

for (const scenario of scenarios) {
  console.log(`\n## ${scenario.name} (${describeScenario(scenario)})`);
  for (const workload of selectedWorkloads) {
    for (const transport of transports) {
      // A fresh server per run: a timed-out run can leave streams sending into the next one.
      applyScenario({ name: "clean" });
      const server = await startServer();
      applyScenario(scenario);
      let timer: ReturnType<typeof setTimeout> | undefined;
      const metrics = await Promise.race([
        workload.run(transport, tls),
        new Promise<Metrics>(
          (resolve) => (timer = setTimeout(() => resolve({ error: "timeout" }), timeoutMs)),
        ),
      ]).catch((error: unknown) => ({ error: String(error) }));
      clearTimeout(timer);
      server.kill("SIGKILL");
      rows.push({ scenario: scenario.name, layer, transport, workload: workload.name, metrics });
      console.log(`  ${workload.name.padEnd(8)} ${transport.padEnd(12)} ${format(metrics)}`);
    }
  }
}

function format(metrics: Metrics) {
  return Object.entries(metrics)
    .map(([key, value]) => `${key}=${typeof value === "number" ? value.toFixed(1) : value}`)
    .join("  ");
}

const out = option("out");
if (out) NodeFS.writeFileSync(out, `${JSON.stringify({ scenarios, rows }, null, 2)}\n`);
if (tlsDir.startsWith(NodePath.join(NodeOS.tmpdir(), "netbench-tls-"))) {
  NodeFS.rmSync(tlsDir, { recursive: true, force: true });
}
// The WebTransport client keeps native handles alive after close.
process.exit(0);
