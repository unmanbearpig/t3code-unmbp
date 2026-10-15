// @effect-diagnostics nodeBuiltinImport:off - One cold client runtime and one single-port server per case.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Cause from "effect/Cause";
import * as Schema from "effect/Schema";
import { OperationControlResponse } from "./operationsFixture.ts";
import { ResourceResponse, startProcessMeasurement } from "./resourceUsage.ts";
import { makeBenchmarkConnector } from "./productionConnector.ts";
import { ReconnectCase } from "./reconnectManifest.ts";
import { TerminalProtocol } from "./reconnectComparison.ts";
import { runReconnect } from "./reconnectClient.ts";
import { serverPort } from "./reconnectNetwork.ts";
import { receive, startController, stopChild } from "./reconnectProcess.ts";
import {
  diagnosticError,
  makeReconnectDiagnostics,
  redactDiagnostic,
} from "./reconnectDiagnostics.ts";
const decodeResource = Schema.decodeUnknownSync(ResourceResponse);
const decodeReady = Schema.decodeUnknownSync(
  Schema.toCodecJson(
    Schema.Struct({
      type: Schema.Literal("ready"),
      bearer: Schema.String,
      initial: OperationControlResponse,
    }),
  ),
);
const decodeControl = Schema.decodeUnknownSync(Schema.toCodecJson(OperationControlResponse));
class BenchmarkCaseDeadline extends Schema.TaggedError<BenchmarkCaseDeadline>()(
  "BenchmarkCaseDeadline",
  { timeoutMs: Schema.Number },
) {
  override get message() {
    return "Benchmark case deadline expired";
  }
}
const [entryFile, cert, key, scratch, queue, mode = "websocket"] = process.argv.slice(2);
if (!entryFile || !cert || !key || !scratch || !queue) throw new Error("Missing worker arguments");
const entry = Schema.decodeSync(Schema.fromJsonString(ReconnectCase))(
  NodeFS.readFileSync(entryFile, "utf8"),
);
const server = NodeChildProcess.fork(
  new URL("./reconnectServer.ts", import.meta.url),
  [entry.scheme, String(serverPort), cert, key, scratch, entry.workload],
  { stdio: ["ignore", "inherit", "inherit", "ipc"] },
);
let controller: Awaited<ReturnType<typeof startController>> | undefined;
let native: Awaited<ReturnType<typeof makeBenchmarkConnector>> | undefined;
let clientMeter: ReturnType<typeof startProcessMeasurement> | undefined;
let failureCause: unknown;
let caseDeadline = false;
const trace =
  process.env.RPC_RECONNECT_TRACE === "1" ? makeReconnectDiagnostics("client") : undefined;
const inspectServer = async () => {
  const inspected = receive(server, 5000);
  server.send({ inspect: true });
  return await inspected;
};
const cleanup = async () => {
  native?.dispose();
  await stopChild(controller?.child);
  await stopChild(server);
};
process.once("SIGTERM", () => {
  void cleanup().finally(() => process.exit(143));
});
try {
  const ready = decodeReady(await receive(server, 30000));
  const advance = (count: number) =>
    Effect.tryPromise(async () => {
      const response = receive(server);
      server.send({ count });
      return decodeControl(await response);
    }).pipe(Effect.orDie);
  if (mode !== "websocket" && mode !== "webtransport")
    throw new Error("Unsupported benchmark transport");
  native =
    mode === "webtransport"
      ? await Effect.runPromise(
          Effect.gen(function* () {
            const platform = yield* HostProcess.Platform;
            const arch = yield* HostProcess.Architecture;
            return yield* Effect.promise(() =>
              makeBenchmarkConnector(NodeFS.readFileSync(cert, "utf8"), platform, arch, trace),
            );
          }),
        )
      : undefined;
  controller = await startController(entry, Number(queue));
  const resourceRequest = async (_tag: "netbench-resource-start" | "netbench-resource-stop") => {
    const response = receive(server);
    server.send({ _tag });
    return decodeResource(await response).metrics;
  };
  await resourceRequest("netbench-resource-start");
  clientMeter = startProcessMeasurement();
  const result = await Effect.runPromise(
    Effect.scoped(
      runReconnect({
        entry,
        origin: `${entry.scheme}://127.0.0.1:${serverPort}`,
        bearer: ready.bearer,
        cert: NodeFS.readFileSync(cert, "utf8"),
        initial: ready.initial,
        advance,
        startController: controller.start,
        requestedTransport: mode,
        ...(native ? { connector: native.connector, nativeEvidence: native.evidence } : {}),
        ...(trace ? { diagnostic: trace.event } : {}),
      }),
    ).pipe(
      Effect.timeoutOrElse({
        duration: entry.timeoutMs,
        orElse: () => {
          caseDeadline = true;
          return Effect.fail(new BenchmarkCaseDeadline({ timeoutMs: entry.timeoutMs }));
        },
      }),
      Effect.tapCause((cause) =>
        Effect.sync(() => {
          failureCause = {
            pretty: redactDiagnostic(Cause.pretty(cause)),
            errors: Cause.prettyErrors(cause).map((error) => diagnosticError(error)),
          };
        }),
      ),
    ),
  );
  const clientResources = clientMeter.stop();
  if (
    result.selectedTransport === "webtransport" &&
    (!native ||
      native.evidence.constructed < 1 ||
      native.evidence.connectReady < 1 ||
      native.evidence.controlStreamsOpened < 1 ||
      native.evidence.requestStreamsOpened < 1 ||
      native.evidence.reliability !== "supports-unreliable" ||
      !native.evidence.ticketPresent ||
      native.evidence.admissionPath !== "/rpc")
  )
    throw new Error("Selected QUIC transport has no native CONNECT/control/application evidence");
  const serverResources = await resourceRequest("netbench-resource-stop");
  await controller.done;
  const inspected = receive(server);
  server.send({ inspect: true });
  const diagnostics = Schema.decodeUnknownSync(
    Schema.Struct({
      transportTrace: Schema.optionalKey(Schema.Unknown),
      terminalProtocol: TerminalProtocol,
      serverRequests: Schema.Array(
        Schema.Struct({
          path: Schema.String,
          connection: Schema.Number,
          atMs: Schema.Number,
          bytes: Schema.Number,
        }),
      ),
    }),
  )(await inspected);
  if (
    entry.workload === "terminal-overlap" &&
    (diagnostics.terminalProtocol.requests !== 1 ||
      diagnostics.terminalProtocol.chunks !== 256 ||
      diagnostics.terminalProtocol.wireAcks < 248 ||
      diagnostics.terminalProtocol.maxPendingChunks > 8 ||
      diagnostics.terminalProtocol.maxPendingChunks < 1)
  )
    throw new Error("Actual terminal wire Ack/output window was not exercised correctly");
  process.send?.({
    type: "result",
    ...(trace ? { diagnostics: { client: trace.snapshot(), server: diagnostics } } : {}),
    result: {
      ...result,
      nativeEvidence: native ? { ...native.evidence } : null,
      terminalProtocol: diagnostics.terminalProtocol,
      resources: {
        window: "whole-client-case-including-short-resume-warmup",
        client: clientResources,
        server: serverResources,
      },
      serverRequests: diagnostics.serverRequests
        .filter((r) => r.atMs >= result.measurementStartMs)
        .map((r) => ({ ...r, atMs: r.atMs - result.measurementStartMs })),
    },
    controller: controller.records,
    signalNs: controller.signalNs(),
  });
} catch (error) {
  const clientResources = clientMeter?.stop();
  const serverDiagnostics = trace
    ? await inspectServer().catch((inspectError: unknown) => ({
        inspectError: diagnosticError(inspectError),
      }))
    : undefined;
  const client = trace?.snapshot();
  process.send?.({
    type: "failure",
    error: redactDiagnostic(String(error)),
    diagnostics: {
      complete: false,
      cause: failureCause ?? diagnosticError(error),
      timeoutOwner: caseDeadline ? "benchmark-case-limit" : "operation-or-production",
      caseTimeoutMs: entry.timeoutMs,
      nativeEvidence: native ? { ...native.evidence } : null,
      ...(clientResources ? { clientResources } : {}),
      ...(client
        ? {
            client,
            completedProbeSamples: client.milestones.filter(
              (item) => item.event === "app-probe-result",
            ),
          }
        : {}),
      ...(serverDiagnostics ? { server: serverDiagnostics } : {}),
    },
  });
  process.exitCode = 1;
} finally {
  await cleanup();
}
