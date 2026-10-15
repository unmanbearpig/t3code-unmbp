// @effect-diagnostics nodeBuiltinImport:off - Focused real fixture/runtime check without netem or a dev server.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { EnvironmentId } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type NodeWebSocket from "ws";
import * as RpcClient from "../../../../packages/client-runtime/src/rpc/client.ts";
import { runReconnect } from "./reconnectClient.ts";
import { OperationControlResponse } from "./reconnectFixture.ts";
import { makeLifecycleManifest } from "./reconnectManifest.ts";
import { defaultVariantOptions } from "./reconnectOptions.ts";
import { mailbox, stopChild } from "./reconnectProcess.ts";

const Ready = Schema.Struct({
  type: Schema.Literal("ready"),
  bearer: Schema.String,
  environmentId: EnvironmentId,
  initial: OperationControlResponse,
});
const decodeReady = Schema.decodeUnknownSync(Schema.toCodecJson(Ready));
const decodeControl = Schema.decodeUnknownSync(Schema.toCodecJson(OperationControlResponse));

it.effect.each(["idle", "warm-healthy"] as const)(
  "loads authoritative shell state and exercises warm reuse: %s",
  (workload) =>
    Effect.gen(function* () {
      const { scratch, cert, port, server, inbox, sockets, lifecycle } = yield* Effect.promise(
        async () => {
          const scratch = NodeFS.mkdtempSync(
            NodePath.join(NodeOS.homedir(), "codex-tmp", "reconnect-fixture-test-"),
          );
          const cert = NodePath.join(scratch, "cert.pem"),
            key = NodePath.join(scratch, "key.pem");
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
              key,
              "-out",
              cert,
              "-days",
              "1",
              "-subj",
              "/CN=localhost",
            ],
            { stdio: "ignore" },
          );
          const reservation = NodeNet.createServer();
          await new Promise<void>((resolve) => reservation.listen(0, "127.0.0.1", resolve));
          const address = reservation.address();
          if (!address || typeof address === "string") throw new Error("Missing fixture port");
          const port = address.port;
          await new Promise<void>((resolve, reject) =>
            reservation.close((error) => (error ? reject(error) : resolve())),
          );
          const server = NodeChildProcess.fork(
            new URL("./reconnectServer.ts", import.meta.url),
            [String(port), cert, key, scratch],
            { stdio: ["ignore", "ignore", "inherit", "ipc"], execArgv: [] },
          );
          const inbox = mailbox(server);
          const sockets: NodeWebSocket[] = [];
          const lifecycle = { cleanupStarted: false };
          return { scratch, cert, port, server, inbox, sockets, lifecycle };
        },
      );
      try {
        yield* Effect.promise(() => inbox.next(60000));
        server.send({
          type: "begin",
          entry: {
            ...makeLifecycleManifest().cases.find((c) => c.workload === workload)!,
            scheme: "http",
            routes: 1,
          },
          epoch: `fixture-${workload}`,
        });
        const ready = decodeReady(yield* Effect.promise(() => inbox.next(30000)));
        const result = yield* Effect.scoped(
          runReconnect({
            entry: {
              ...makeLifecycleManifest().cases.find((c) => c.workload === workload)!,
              scheme: "http",
              routes: 1,
            },
            environmentId: ready.environmentId,
            queuePackets: 100000,
            lifecycle,
            registerResource: (kind) => kind,
            registerSocket: (socket) => sockets.push(socket),
            origin: `http://127.0.0.1:${port}`,
            bearer: ready.bearer,
            cert: NodeFS.readFileSync(cert, "utf8"),
            initial: ready.initial,
            advance: () =>
              Effect.tryPromise(async () => {
                server.send({ count: 0 });
                return decodeControl(await inbox.next());
              }).pipe(Effect.orDie),
            options: {
              ...defaultVariantOptions,
              websocketHeaders: false,
              helloHandshake: false,
              helloSupportCached: false,
            },
            startController: () => {},
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                lifecycle.cleanupStarted = true;
                for (const socket of sockets) socket.terminate();
              }),
            ),
          ),
        );
        expect(result.correct).toBe(true);
        expect(result.shellSnapshots).toBe(0);
        const supportsReuse = "getSessionServerStartedAt" in RpcClient;
        expect(result.requests["/api/orchestration/shell"] ?? 0).toBe(
          workload === "warm-healthy" && supportsReuse ? 0 : 1,
        );
        if (supportsReuse) expect(result.rpcRequests.subscribeServerLifecycle).toBeGreaterThan(0);
        server.send({ end: true });
        expect(yield* Effect.promise(() => inbox.next())).toEqual({
          type: "ended",
          serverConnectionsAfter: 0,
        });
      } finally {
        for (const socket of sockets) socket.terminate();
        yield* Effect.promise(() => stopChild(server));
        inbox.close();
        NodeFS.rmSync(scratch, { recursive: true, force: true });
      }
    }).pipe(Effect.scoped),
  60000,
);
