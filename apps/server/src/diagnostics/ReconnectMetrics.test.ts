import { expect, it } from "@effect/vitest";
import type { ReconnectSample, ReconnectLoopSample } from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as ServerConfig from "../config.ts";
import * as ReconnectMetrics from "./ReconnectMetrics.ts";

it.effect.each(["normal", "loop"] as const)(
  "persists %s samples separately and retains one previous bounded log",
  (kind) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const logsDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-reconnect-metrics-" });
      const configLayer = ServerConfig.layerTest(process.cwd(), logsDir);
      const config = yield* ServerConfig.ServerConfig.pipe(Effect.provide(configLayer));
      const filename = path.join(
        config.logsDir,
        kind === "normal" ? "reconnect-samples.ndjson" : "reconnect-loop-samples.ndjson",
      );
      const collector = yield* ReconnectMetrics.ReconnectMetrics.pipe(
        Effect.provide(ReconnectMetrics.layer.pipe(Layer.provide(configLayer))),
      );
      const sample: ReconnectSample = {
        version: 1,
        id: "trial",
        recordedAt: "2026-11-07T00:00:00Z",
        arm: "new",
        pipelineArm: "old",
        platform: "android",
        appVersion: "test",
        serverVersion: "test",
        networkType: "cellular",
        transport: "websocket",
        outcome: "ready",
        cachedProviders: 10,
        reusedCatalogs: 10,
        configReadyMs: 12,
        shellReadyMs: 45,
        rpcRttMs: 250,
        receivedJsonBytes: 200,
        sentJsonBytes: 100,
        receivedFrames: 3,
        sentFrames: 2,
        rpcAcksSent: 1,
        appTraffic: null,
      };
      const loopSample: ReconnectLoopSample = {
        version: 1,
        id: "run:0:restart",
        runId: "run",
        at: sample.recordedAt,
        round: 0,
        arm: "restart",
        pipelineArm: "new",
        platform: "android",
        appVersion: "test",
        serverVersion: "test",
        networkType: "cellular",
        transport: "websocket",
        ok: true,
        prepareMs: 10,
        configMs: 12,
        syncedMs: 45,
        jsonDown: 200,
        jsonUp: 100,
        sentRevisions: 10,
        reusedCatalogs: 10,
        projectFrames: 0,
        appTraffic: null,
      };
      const record = (id: string) =>
        kind === "normal"
          ? collector.record([{ ...sample, id }])
          : collector.recordLoop([{ ...loopSample, id }]);
      yield* record("trial");
      expect(JSON.parse(yield* fs.readFileString(filename))).toEqual({
        ...(kind === "normal" ? sample : loopSample),
        id: "trial",
      });
      expect(
        yield* fs.exists(
          path.join(
            config.logsDir,
            kind === "normal" ? "reconnect-loop-samples.ndjson" : "reconnect-samples.ndjson",
          ),
        ),
      ).toBe(false);
      yield* fs.writeFileString(`${filename}.previous`, "older");
      yield* fs.writeFileString(filename, "x".repeat(4 * 1024 * 1024));
      yield* record("next");
      expect(JSON.parse(yield* fs.readFileString(filename))).toMatchObject({ id: "next" });
      expect(Number((yield* fs.stat(`${filename}.previous`)).size)).toBe(4 * 1024 * 1024);
    }).pipe(Effect.provide(NodeServices.layer)),
);
