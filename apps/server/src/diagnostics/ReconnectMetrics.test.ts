import { expect, it } from "@effect/vitest";
import type { ReconnectSample } from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as ServerConfig from "../config.ts";
import * as ReconnectMetrics from "./ReconnectMetrics.ts";

it.effect("persists samples verbatim and retains only one previous bounded log", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const logsDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-reconnect-metrics-" });
    const configLayer = ServerConfig.layerTest(process.cwd(), logsDir);
    const config = yield* ServerConfig.ServerConfig.pipe(Effect.provide(configLayer));
    const filename = path.join(config.logsDir, "reconnect-samples.ndjson");
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
    yield* collector.record([sample]);
    expect(JSON.parse(yield* fs.readFileString(filename))).toEqual(sample);
    yield* fs.writeFileString(`${filename}.previous`, "older");
    yield* fs.writeFileString(filename, "x".repeat(4 * 1024 * 1024));
    yield* collector.record([{ ...sample, id: "next" }]);
    expect(JSON.parse(yield* fs.readFileString(filename))).toMatchObject({ id: "next" });
    expect(Number((yield* fs.stat(`${filename}.previous`)).size)).toBe(4 * 1024 * 1024);
  }).pipe(Effect.provide(NodeServices.layer)),
);
