// @effect-diagnostics nodeBuiltinImport:off - One cold client runtime and one single-port server per case.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { OperationControlResponse } from "./operationsFixture.ts";
import { VariantOptions, defaultVariantOptions } from "./reconnectOptions.ts";
import { ReconnectCase } from "./reconnectManifest.ts";
import { runReconnect } from "./reconnectClient.ts";
import { serverPort } from "./reconnectNetwork.ts";
import { receive, startController, stopChild } from "./reconnectProcess.ts";
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
const [entryFile, cert, key, scratch, queue, optionsText] = process.argv.slice(2);
if (!entryFile || !cert || !key || !scratch || !queue) throw new Error("Missing worker arguments");
const entry = Schema.decodeUnknownSync(Schema.fromJsonString(ReconnectCase))(
  NodeFS.readFileSync(entryFile, "utf8"),
);
const server = NodeChildProcess.fork(
  new URL("./reconnectServer.ts", import.meta.url),
  [entry.scheme, String(serverPort), cert, key, scratch],
  { stdio: ["ignore", "inherit", "inherit", "ipc"] },
);
let controller: Awaited<ReturnType<typeof startController>> | undefined;
const cleanup = async () => {
  await stopChild(controller?.child);
  await stopChild(server);
};
process.once("SIGTERM", () => {
  void cleanup().finally(() => process.exit(143));
});
try {
  const ready = decodeReady(await receive(server, 30000));
  const advance = () =>
    Effect.tryPromise(async () => {
      const response = receive(server);
      server.send({ count: entry.workload === "idle" ? 0 : 20 });
      return decodeControl(await response);
    }).pipe(Effect.orDie);
  controller = await startController(entry, Number(queue));
  const result = await Effect.runPromise(
    Effect.scoped(
      runReconnect({
        entry,
        origin: `${entry.scheme}://127.0.0.1:${serverPort}`,
        bearer: ready.bearer,
        cert: NodeFS.readFileSync(cert, "utf8"),
        initial: ready.initial,
        advance,
        options: optionsText
          ? Schema.decodeUnknownSync(Schema.fromJsonString(VariantOptions))(optionsText)
          : defaultVariantOptions,
        startController: controller.start,
      }),
    ).pipe(Effect.timeout(entry.timeoutMs)),
  );
  await controller.done;
  const inspected = receive(server);
  server.send({ inspect: true });
  const diagnostics = Schema.decodeUnknownSync(
    Schema.Struct({
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
  const serverRequests = diagnostics.serverRequests
    .filter((r) => r.atMs >= result.measurementStartMs)
    .map((r) => ({ ...r, atMs: r.atMs - result.measurementStartMs }));
  process.send?.({
    type: "result",
    result: {
      ...result,
      serverRequests,
      connections: new Set(serverRequests.map((r) => r.connection)).size,
    },
    controller: controller.records,
    signalNs: controller.signalNs(),
  });
} catch (error) {
  process.send?.({ type: "failure", error: String(error) });
  process.exitCode = 1;
} finally {
  await cleanup();
}
