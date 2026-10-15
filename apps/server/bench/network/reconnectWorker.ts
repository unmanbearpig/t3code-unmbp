// @effect-diagnostics nodeBuiltinImport:off - Reuses imports and processes, never per-case runtime resources.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import type NodeWebSocket from "ws";
import { EnvironmentId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { OperationControlResponse } from "./reconnectFixture.ts";
import { VariantOptions } from "./reconnectOptions.ts";
import { ReconnectCase } from "./reconnectManifest.ts";
import { runReconnect } from "./reconnectClient.ts";
import { applyProfile, serverPort } from "./reconnectNetwork.ts";
import { mailbox, startController, stopChild } from "./reconnectProcess.ts";
import {
  assertIsolation,
  makeResourceTracker,
  type IsolationEvidence,
} from "./reconnectIsolation.ts";
import { workloadPlan } from "./reconnectWorkload.ts";

const decodeReady = Schema.decodeUnknownSync(
  Schema.toCodecJson(
    Schema.Struct({
      type: Schema.Literal("ready"),
      bearer: Schema.String,
      initial: OperationControlResponse,
      environmentId: EnvironmentId,
      epoch: Schema.String,
    }),
  ),
);
const decodeControl = Schema.decodeUnknownSync(Schema.toCodecJson(OperationControlResponse));
const decodeCommand = Schema.decodeUnknownSync(
  Schema.Struct({ entry: ReconnectCase, options: VariantOptions, epoch: Schema.String }),
);
const decodeInspect = Schema.decodeUnknownSync(
  Schema.Struct({
    serverRequests: Schema.Array(
      Schema.Struct({
        path: Schema.String,
        connection: Schema.Number,
        connectionId: Schema.String,
        atMs: Schema.Number,
        bytes: Schema.Number,
      }),
    ),
    peakSockets: Schema.Number,
  }),
);
const decodeEnded = Schema.decodeUnknownSync(
  Schema.Struct({ type: Schema.Literal("ended"), serverConnectionsAfter: Schema.Number }),
);
const [cert, key, scratch, queueText] = process.argv.slice(2);
if (!cert || !key || !scratch || !queueText) throw new Error("Missing worker arguments");
const queuePackets = Number(queueText);
const inbox = mailbox(process);
const server = NodeChildProcess.fork(
  new URL("./reconnectServer.ts", import.meta.url),
  [String(serverPort), cert, key, scratch],
  { stdio: ["ignore", "inherit", "inherit", "ipc"] },
);
const serverInbox = mailbox(server);
let controller: Awaited<ReturnType<typeof startController>> | undefined;
let previous: IsolationEvidence | undefined;
const registerResource = makeResourceTracker();
const cleanup = async () => {
  await stopChild(controller?.child);
  await stopChild(server);
};
process.once("SIGTERM", () => {
  void cleanup().finally(() => process.exit(143));
});
await serverInbox.next(60000);
process.send?.({ type: "booted", pid: process.pid, serverPid: server.pid });
try {
  for (;;) {
    const { entry, options, epoch } = decodeCommand(await inbox.next(3600000));
    const sockets: NodeWebSocket[] = [];
    const lifecycle = { cleanupStarted: false };
    const wallStart = performance.now();
    try {
      // Replacing all queues drops queued packets and all flow-specific blackholes.
      applyProfile(entry.profile, entry.seed, queuePackets);
      server.send({ type: "begin", entry, epoch });
      const ready = decodeReady(await serverInbox.next(30000));
      const advance = () =>
        Effect.tryPromise(async () => {
          server.send({ count: workloadPlan(entry).addedMessages });
          return decodeControl(await serverInbox.next());
        }).pipe(Effect.orDie);
      controller = await startController(entry, queuePackets);
      const result = await Effect.runPromise(
        Effect.scoped(
          runReconnect({
            entry,
            environmentId: ready.environmentId,
            queuePackets,
            lifecycle,
            registerResource,
            registerSocket: (socket) => sockets.push(socket),
            origin: `${entry.scheme}://127.0.0.1:${serverPort}`,
            bearer: ready.bearer,
            cert: NodeFS.readFileSync(cert, "utf8"),
            initial: ready.initial,
            advance,
            options,
            startController: controller.start,
          }).pipe(
            Effect.tap((result) =>
              Effect.sync(() =>
                result.stages.push({
                  name: "case-cleanup-start",
                  atMs: Number(process.hrtime.bigint()) / 1e6 - result.measurementStartMs,
                  phase: "cleanup",
                }),
              ),
            ),
            Effect.ensuring(
              Effect.sync(() => {
                lifecycle.cleanupStarted = true;
                for (const socket of sockets) socket.terminate();
              }),
            ),
          ),
        ).pipe(Effect.timeout(entry.timeoutMs)),
      );
      await controller.done;
      server.send({ inspect: true });
      const diagnostics = decodeInspect(await serverInbox.next());
      server.send({ end: true });
      const ended = decodeEnded(await serverInbox.next());
      await Promise.all(
        sockets
          .filter((s) => s.readyState !== 3)
          .map((s) => new Promise<void>((resolve) => s.once("close", resolve))),
      );
      const isolation = {
        ...result.isolation,
        epoch,
        environmentId: ready.environmentId,
        initialMessages: ready.initial.messages,
        initialSequence: ready.initial.sequence,
        socketsAfter: sockets.filter((s) => s.readyState !== 3).length,
        serverConnectionsAfter: ended.serverConnectionsAfter,
      };
      assertIsolation(isolation, previous);
      previous = isolation;
      process.send?.({
        type: "result",
        result: {
          ...result,
          isolation,
          serverPeakSockets: diagnostics.peakSockets,
          serverRequests: diagnostics.serverRequests
            .filter((r) => r.atMs >= result.observationStartMs)
            .map((r) => ({ ...r, atMs: r.atMs - result.measurementStartMs })),
        },
        wallMs: performance.now() - wallStart,
        controller: controller.records,
      });
    } catch (error) {
      process.send?.({ type: "failure", error: String(error) });
      // A failed reset poisons the lane. Never let later rows silently inherit it.
      break;
    } finally {
      await stopChild(controller?.child);
      controller = undefined;
    }
  }
} finally {
  await cleanup();
}
