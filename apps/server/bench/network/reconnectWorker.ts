// @effect-diagnostics nodeBuiltinImport:off - One cold client runtime and one single-port server per case.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { OperationControlResponse } from "./operationsFixture.ts";
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
const [entryFile, cert, key, scratch, queue] = process.argv.slice(2);
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
  const response = receive(server);
  // Twenty completed messages are exactly fifty new domain events.
  server.send({ count: entry.workload === "idle" ? 0 : 20 });
  const expected = decodeControl(await response);
  controller = await startController(entry, Number(queue));
  const result = await Effect.runPromise(
    Effect.scoped(
      runReconnect({
        entry,
        origin: `${entry.scheme}://127.0.0.1:${serverPort}`,
        bearer: ready.bearer,
        cert: NodeFS.readFileSync(cert, "utf8"),
        initial: ready.initial,
        expected,
        startController: controller.start,
      }),
    ).pipe(Effect.timeout(entry.timeoutMs)),
  );
  await controller.done;
  process.send?.({
    type: "result",
    result,
    controller: controller.records,
    signalNs: controller.signalNs(),
  });
} catch (error) {
  process.send?.({ type: "failure", error: String(error) });
  process.exitCode = 1;
} finally {
  await cleanup();
}
