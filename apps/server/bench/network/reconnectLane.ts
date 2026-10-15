// @effect-diagnostics nodeBuiltinImport:off globalConsole:off - One private namespace and process set per lane.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as Schema from "effect/Schema";
import { prepareLoopback } from "./impairments.ts";
import { calibrate } from "./reconnectCalibration.ts";
import { validateManifest } from "./reconnectManifest.ts";
import { mailbox, stopChild } from "./reconnectProcess.ts";
import { VariantOptions } from "./reconnectOptions.ts";

const [manifestFile, scratch, laneText, mode] = process.argv.slice(2);
if (!manifestFile || !scratch || !laneText) throw new Error("Missing lane arguments");
const lane = Number(laneText);
const manifest = validateManifest(JSON.parse(NodeFS.readFileSync(manifestFile, "utf8")));
const inbox = mailbox(process);
const workers = new Map<
  string,
  { child: NodeChildProcess.ChildProcess; inbox: ReturnType<typeof mailbox> }
>();
const cert = NodePath.join(scratch, "cert.pem"),
  key = NodePath.join(scratch, "key.pem");
const cleanup = async () => {
  await Promise.all([...workers.values()].map((w) => stopChild(w.child)));
};
process.once("SIGTERM", () => {
  void cleanup().finally(() => process.exit(143));
});
const command = Schema.decodeUnknownSync(
  Schema.Struct({
    path: Schema.String,
    epoch: Schema.String,
    entry: Schema.Unknown,
    options: VariantOptions,
  }),
);
try {
  prepareLoopback(manifest.mtu);
  const metricsSave = NodeFS.readFileSync("/proc/sys/net/ipv4/tcp_no_metrics_save", "utf8").trim();
  if (metricsSave !== "1") throw new Error("Namespace must disable TCP metric persistence");
  const calibration = await calibrate(manifest, (message) =>
    console.log(`lane ${lane}: ${message}`),
  );
  process.send?.({
    type: "calibrated",
    calibration,
    environment: {
      lane,
      namespace: NodeFS.readlinkSync("/proc/self/ns/net"),
      node: process.version,
      versions: process.versions,
      cpu: NodeOS.cpus()[0]?.model,
      kernel: NodeOS.release(),
      tcpNoMetricsSave: metricsSave,
      affinity: NodeFS.readFileSync("/proc/self/status", "utf8")
        .split("\n")
        .find((s) => s.startsWith("Cpus_allowed_list:")),
      links: JSON.parse(
        NodeChildProcess.execFileSync("ip", ["-json", "link", "show"], { encoding: "utf8" }),
      ),
      offloads: NodeChildProcess.execFileSync("ethtool", ["-k", "lo"], { encoding: "utf8" }),
    },
  });
  if (!calibration.correct) throw new Error("Lane calibration failed");
  for (;;) {
    const raw = await inbox.next(3600000);
    if (typeof raw === "object" && raw !== null && "stop" in raw) break;
    const request = command(raw);
    let worker = workers.get(request.path);
    if (!worker) {
      const child = NodeChildProcess.fork(
        NodePath.join(request.path, "apps/server/bench/network/reconnectWorker.ts"),
        [cert, key, scratch, String(manifest.queuePackets)],
        {
          cwd: request.path,
          stdio: ["ignore", "inherit", "inherit", "ipc"],
          env: { ...process.env, NODE_EXTRA_CA_CERTS: cert },
        },
      );
      worker = { child, inbox: mailbox(child) };
      workers.set(request.path, worker);
      await worker.inbox.next(60000);
    }
    worker.child.send(request);
    const result = await worker.inbox.next(120000);
    if (mode === "fresh") {
      await stopChild(worker.child);
      worker.inbox.close();
      workers.delete(request.path);
    }
    process.send?.(result);
    if (
      typeof result === "object" &&
      result !== null &&
      "type" in result &&
      result.type === "failure"
    )
      break;
  }
} catch (error) {
  process.send?.({ type: "failure", error: String(error) });
  process.exitCode = 1;
} finally {
  await cleanup();
  process.disconnect?.();
}
