// @effect-diagnostics nodeBuiltinImport:off globalConsole:off globalTimers:off - Owns private namespaces, fixtures and measurement deadlines.
// node apps/server/bench/quic-feasibility/performance/control.ts --out ~/codex-tmp/quic-control.json
// Same framed deflate codec over verified TLS WSS and one/multiple QUIC streams. No HTTP snapshots.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import { WebSocket as NodeWebSocket } from "ws";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";
import { RpcClient } from "effect/rpc";
import * as Socket from "effect/socket/Socket";

import { applyScenario, parseScenario, prepareLoopback } from "../../network/impairments.ts";
import { operationMessageText } from "../../network/operationsFixture.ts";
import { verifyPacketImpairments } from "../../network/packetizationCheck.ts";
import { ports, wallClockMs } from "../../network/protocol.ts";
import { BenchRpcs } from "../../network/rpc.ts";
import { makeRpcProtocol } from "../../network/rpcClient.ts";
import { layerDeflatedJson } from "../../network/rpcDeflate.ts";
import { distribution, scenarios, seeds, transports } from "./results.ts";

const { values } = NodeUtil.parseArgs({
  options: { out: { type: "string" }, smoke: { type: "boolean" } },
});
if (!values.out) throw new Error("--out is required");
const out = NodePath.resolve(values.out);
if (process.env.QUIC_CONTROL_NETNS !== "1") {
  if (NodeFS.existsSync(out)) throw new Error("Output already exists");
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
      import.meta.filename,
      ...process.argv.slice(2),
    ],
    {
      stdio: ["inherit", "inherit", "inherit", "ipc"],
      env: {
        ...process.env,
        QUIC_CONTROL_NETNS: "1",
        NETBENCH_HOST_NETNS: NodeFS.readlinkSync("/proc/self/ns/net"),
      },
    },
  );
  for (const signal of ["SIGINT", "SIGTERM"] as const)
    process.once(signal, () => child.send("interrupt"));
  process.exit(
    await new Promise<number>((resolve, reject) => {
      child.once("error", reject);
      child.once("exit", (code) => resolve(code ?? 1));
    }),
  );
}
NodeFS.mkdirSync(NodePath.dirname(out), { recursive: true });
const scratch = NodeFS.mkdtempSync(NodePath.join(NodeOS.homedir(), "codex-tmp", "quic-control-"));
const certPath = NodePath.join(scratch, "cert.pem");
const keyPath = NodePath.join(scratch, "key.pem");
let active: NodeChildProcess.ChildProcess | undefined;
const stop = async () => {
  const child = active;
  active = undefined;
  if (child && child.exitCode === null && child.signalCode === null)
    await new Promise<void>((resolve) => {
      child.once("exit", () => resolve());
      child.kill("SIGKILL");
    });
};
const cleanup = async () => {
  await stop();
  NodeFS.rmSync(scratch, { recursive: true, force: true });
};
process.on("message", (message) => {
  if (message === "interrupt") void cleanup().finally(() => process.exit(143));
});
process.once("disconnect", () => void cleanup().finally(() => process.exit(143)));
for (const signal of ["SIGINT", "SIGTERM"] as const)
  process.once(signal, () => void cleanup().finally(() => process.exit(143)));
const counters = () => {
  const fields = NodeFS.readFileSync("/proc/net/dev", "utf8")
    .split("\n")
    .find((line) => line.trimStart().startsWith("lo:"))!
    .split(":")[1]!
    .trim()
    .split(/\s+/)
    .map(Number);
  return { bytes: fields[8]!, packets: fields[9]! };
};
const rows: Array<{
  scenario: string;
  transport: string;
  seed: number;
  outcome: string;
  snapshotMs?: number;
  pingRttMs?: number[];
  linkTxBytes?: number;
  linkTxPackets?: number;
}> = [];
const loadStart = NodeOS.loadavg();
const selectedScenarios = values.smoke ? ["clean"] : scenarios;
const selectedSeeds = values.smoke ? [seeds[0]] : seeds;
const save = (complete: boolean) =>
  NodeFS.writeFileSync(
    out,
    JSON.stringify(
      {
        complete,
        methodology: {
          kind: "same-codec-compressed-RPC-control",
          codec: "layerDeflatedJson on every transport; WebSocket permessage-deflate disabled",
          workload:
            "Warm connection, 1000 distinct 2000-character messages over RPC while pinging every 20ms",
          correctness: "Exact snapshot ID/order/text/role and every ping response verified",
          network:
            "Private network/PID namespace; MTU 1500; disabled offloads; ingress impairment validated",
          limits: [
            "Linux Node clients only; no Tailscale/DERP/mobility/battery evidence",
            "Shared host CPU; no bandwidth cap",
            "Separate compression dictionaries per QUIC stream; WSS and single-stream QUIC share a connection dictionary",
            "Prototype per-stream RPC identity and heartbeat are not production-ready",
            "No tail percentile claims from these samples",
          ],
          node: process.version,
          cpu: NodeOS.cpus()[0]?.model,
          cpuCount: NodeOS.availableParallelism(),
          loadStart,
          loadEnd: NodeOS.loadavg(),
        },
        rows,
        groups: selectedScenarios.flatMap((scenario) =>
          transports.map((transport) => {
            const selected = rows.filter(
              (row) =>
                row.scenario === scenario && row.transport === transport && row.outcome === "done",
            );
            return {
              scenario,
              transport,
              completed: selected.length,
              snapshotMs: distribution(selected.map((row) => row.snapshotMs!)),
              perRunPingMedianMs: distribution(
                selected.map((row) => distribution(row.pingRttMs!)!.median),
              ),
              perRunPingMaxMs: distribution(selected.map((row) => Math.max(...row.pingRttMs!))),
              linkTxBytes: distribution(selected.map((row) => row.linkTxBytes!)),
            };
          }),
        ),
      },
      null,
      2,
    ) + "\n",
  );
try {
  prepareLoopback(scratch, true);
  verifyPacketImpairments();
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
  const tls = {
    certPem,
    certSha256: new Uint8Array(
      NodeCrypto.createHash("sha256").update(new NodeCrypto.X509Certificate(certPem).raw).digest(),
    ),
  };
  save(false);
  for (const scenario of selectedScenarios)
    for (const [index, seed] of selectedSeeds.entries())
      for (let offset = 0; offset < transports.length; offset++) {
        const transport = transports[(index + offset) % transports.length]!;
        const row: (typeof rows)[number] = { scenario, transport, seed, outcome: "running" };
        rows.push(row);
        applyScenario({ name: "clean" }, undefined, true);
        try {
          const server = NodeChildProcess.fork(
            NodePath.join(import.meta.dirname, "controlServer.ts"),
            [certPath, keyPath],
            { stdio: ["ignore", "inherit", "inherit", "ipc"] },
          );
          active = server;
          await new Promise<void>((resolve, reject) => {
            const timer = setTimeout(() => reject(new Error("Fixture readiness deadline")), 15_000);
            server.once("message", () => {
              clearTimeout(timer);
              resolve();
            });
            server.once("error", reject);
            server.once("exit", () => {
              clearTimeout(timer);
              reject(new Error("Fixture exited"));
            });
          });
          applyScenario(parseScenario(scenario), seed, true);
          const run = Effect.scoped(
            Effect.gen(function* () {
              const scope = yield* Effect.scope;
              const context =
                transport === "rpc-wss"
                  ? yield* Layer.buildWithScope(
                      Layer.effect(
                        RpcClient.Protocol,
                        RpcClient.makeProtocolSocket({
                          retryTransientErrors: false,
                          retryPolicy: Schedule.recurs(0),
                        }),
                      ).pipe(
                        Layer.provide(Socket.layerWebSocket(`wss://127.0.0.1:${ports.rpcWss}/`)),
                        Layer.provide(
                          Layer.succeed(
                            Socket.WebSocketConstructor,
                            (url) =>
                              new NodeWebSocket(url, {
                                ca: certPem,
                                minVersion: "TLSv1.3",
                                maxVersion: "TLSv1.3",
                                perMessageDeflate: false,
                              }),
                          ),
                        ),
                        Layer.provide(layerDeflatedJson),
                      ),
                      scope,
                    )
                  : yield* makeRpcProtocol(transport, tls, {
                      serialization: layerDeflatedJson,
                      requireUnreliable: true,
                    });
              const client = yield* RpcClient.make(BenchRpcs).pipe(Effect.provide(context));
              const pingText = "x".repeat(256);
              const ping = client
                .ping({ text: pingText })
                .pipe(
                  Effect.tap((response) =>
                    response === pingText
                      ? Effect.void
                      : Effect.die(new Error("Corrupt ping response")),
                  ),
                );
              yield* ping;
              const before = counters();
              const startedAt = wallClockMs();
              const snapshot = yield* Effect.forkChild(
                client.snapshot({ messages: 1000, bytes: 2000 }).pipe(
                  Effect.tap(() =>
                    Effect.sync(() => {
                      row.snapshotMs = wallClockMs() - startedAt;
                    }),
                  ),
                ),
              );
              const pings: Array<Fiber.Fiber<void, never>> = [];
              const rtts: number[] = [];
              while (!snapshot.pollUnsafe()) {
                const sentAt = wallClockMs();
                pings.push(
                  yield* Effect.forkChild(
                    ping.pipe(
                      Effect.orDie,
                      Effect.asVoid,
                      Effect.tap(() =>
                        Effect.sync(() => {
                          rtts.push(wallClockMs() - sentAt);
                        }),
                      ),
                    ),
                  ),
                );
                yield* Effect.sleep("20 millis");
              }
              const response = yield* Fiber.join(snapshot);
              yield* Fiber.joinAll(pings);
              if (
                response.messages.length !== 1000 ||
                response.messages.some(
                  (message, index) =>
                    message.id !== `message-${index}` ||
                    message.text !== operationMessageText(index) ||
                    message.role !== (index % 2 === 0 ? "user" : "assistant"),
                )
              )
                return yield* Effect.die(new Error("Corrupt snapshot response"));
              const after = counters();
              row.pingRttMs = rtts;
              row.linkTxBytes = after.bytes - before.bytes;
              row.linkTxPackets = after.packets - before.packets;
            }),
          ).pipe(Effect.timeoutOption("25 seconds"));
          const result = await Effect.runPromise(run);
          row.outcome = Option.isSome(result) ? "done" : "timeout";
        } catch (cause) {
          row.outcome = `error: ${String(cause)}`;
        }
        await stop();
        save(false);
        console.log(
          `${scenario} ${transport} seed=${seed} ${row.outcome} snapshotMs=${row.snapshotMs} pingSamples=${row.pingRttMs?.length}`,
        );
      }
  save(true);
} finally {
  await cleanup();
}
process.exit(rows.every((row) => row.outcome === "done") ? 0 : 1);
