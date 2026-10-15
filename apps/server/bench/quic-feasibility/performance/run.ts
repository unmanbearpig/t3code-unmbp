// @effect-diagnostics nodeBuiltinImport:off globalConsole:off globalDate:off - A bounded command-line benchmark, outside the measured client path.
// node apps/server/bench/quic-feasibility/performance/run.ts --out-dir ~/codex-tmp/quic-performance
// --smoke runs three cases. The default runs 135 paired cases, not the 1,062-case full suite.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import * as Schema from "effect/Schema";

import { Results, scenarios, seeds, summarize, transports, workloads } from "./results.ts";

const { values } = NodeUtil.parseArgs({
  options: {
    "out-dir": { type: "string" },
    smoke: { type: "boolean" },
    "dry-run": { type: "boolean" },
  },
});
const outDir = NodePath.resolve(
  values["out-dir"] ??
    NodePath.join(NodeOS.homedir(), "codex-tmp", "quic-performance-" + Date.now()),
);
const selected = values.smoke
  ? { scenarios: ["clean"], seeds: [seeds[0]], workloads: [workloads[0]] }
  : { scenarios, seeds, workloads };
const command = [
  NodePath.resolve(import.meta.dirname, "../../network/run.ts"),
  "--layer",
  "operations",
  "--scenarios",
  selected.scenarios.join(","),
  "--transports",
  transports.join(","),
  "--workloads",
  selected.workloads.join(","),
  "--seed-list",
  selected.seeds.join(","),
  "--http-protocol",
  "h1",
  "--http-encoding",
  "gzip",
  "--timeout",
  "25",
  "--out",
  NodePath.join(outDir, "raw.json"),
];
const methodology = {
  command: [process.execPath, ...command],
  expectedCases:
    selected.scenarios.length *
    selected.seeds.length *
    selected.workloads.length *
    transports.length,
  pairing: "Same netem seed, rotating transport order, fresh server for each case",
  network:
    "Private user/network/PID namespace; MTU 1500; offloads disabled; packet impairment self-test",
  wssControl:
    "Direct Node HTTPS/TLS 1.3 with CA validation and permessage-deflate; bypasses Tailscale Serve just as QUIC does",
  quic: "WebTransport pinned certificate, TLS 1.3, requireUnreliable=true; native Node client",
  http: "HTTP/1.1 and gzip for every transport; cold snapshot pool; snapshots remain a separate connection",
  correctness:
    "Existing harness verifies shared client state, message contents/order, visible items, runs, cursor, event counts and snapshot path",
  statistics: "Raw repetitions, median and observed min/max; no tail-percentile inference",
  limits: [
    "Linux loopback with artificial delay/loss; no physical NAT, Tailscale, DERP, mobility, browser or mobile client measurement",
    "WSS uses permessage-deflate; QUIC uses the existing dictionary deflate frame codec. Their codec costs and frame boundaries differ",
    "Matching a seed does not drop equivalent logical messages when transports emit different packets",
    "No throughput cap or bandwidth contention in these profiles",
    "Host CPU is shared with other work; namespace isolates networking, not CPU, power or memory",
    "open-long starts after RPC configuration is ready; operationMs excludes the RPC handshake and includes the identical HTTPS snapshot",
    "Prototype multi-stream RPC identity/heartbeat behavior is not production-ready",
  ],
};
if (values["dry-run"]) {
  console.log(JSON.stringify(methodology, null, 2));
  process.exit(0);
}
NodeFS.mkdirSync(outDir, { recursive: true });
if (NodeFS.existsSync(NodePath.join(outDir, "raw.json")))
  throw new Error("Output already exists; choose a fresh --out-dir");
const cpuSnapshot = () => ({
  load: NodeOS.loadavg(),
  cpus: NodeOS.cpus().map((cpu) => cpu.times),
  availableParallelism: NodeOS.availableParallelism(),
});
NodeFS.writeFileSync(
  NodePath.join(outDir, "methodology.json"),
  JSON.stringify(methodology, null, 2) + "\n",
);
const started = cpuSnapshot();
const child = NodeChildProcess.spawn(process.execPath, command, { stdio: "inherit" });
for (const signal of ["SIGTERM", "SIGINT"] as const) process.once(signal, () => child.kill(signal));
const exitCode = await new Promise<number>((resolve, reject) => {
  child.once("error", reject);
  child.once("exit", (code) => resolve(code ?? 1));
});
NodeFS.writeFileSync(
  NodePath.join(outDir, "host-load.json"),
  JSON.stringify({ started, finished: cpuSnapshot() }, null, 2) + "\n",
);
if (NodeFS.existsSync(NodePath.join(outDir, "raw.json"))) {
  const result = Schema.decodeSync(Schema.fromJsonString(Results))(
    NodeFS.readFileSync(NodePath.join(outDir, "raw.json"), "utf8"),
  );
  const summary = summarize(result);
  NodeFS.writeFileSync(
    NodePath.join(outDir, "summary.json"),
    JSON.stringify(summary, null, 2) + "\n",
  );
  console.log(
    `Summary: ${outDir}/summary.json; ${summary.completed}/${methodology.expectedCases} completed`,
  );
  if (!result.complete || result.rows.length !== methodology.expectedCases) process.exit(1);
}
process.exit(exitCode);
