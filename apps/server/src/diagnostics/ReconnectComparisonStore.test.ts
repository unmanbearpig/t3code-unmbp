import * as NodeOS from "node:os";
import { expect, it } from "@effect/vitest";
import { EnvironmentId, type ReconnectComparisonPlan } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as ServerConfig from "../config.ts";
import * as Store from "./ReconnectComparisonStore.ts";

it.effect("deduplicates retries within a batch, concurrently, and after restarting the store", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const root = `${NodeOS.homedir()}/codex-tmp`;
    yield* fs.makeDirectory(root, { recursive: true });
    const directory = yield* fs.makeTempDirectoryScoped({
      directory: root,
      prefix: "reconnect-comparison-store-",
    });
    const configLayer = ServerConfig.layerTest(directory, directory);
    const store = yield* Store.make.pipe(Effect.provide(configLayer));
    const record: ReconnectComparisonPlan = {
      version: 2,
      kind: "plan",
      id: "run:plan",
      runId: "run",
      protocolId: "test-protocol",
      buildId: "fixture",
      sourceCommit: "fixture",
      appPackage: "fixture",
      network: "cellular",
      upstreamCommit: "fixture",
      installId: "fixture",
      environmentId: EnvironmentId.make("fixture"),
      at: "2026-11-08T00:00:00.000Z",
      timeoutMs: 60_000,
      assignments: [],
    };
    yield* Effect.all([store.record([record, record]), store.record([record])], {
      concurrency: "unbounded",
    });
    const restarted = yield* Store.make.pipe(Effect.provide(configLayer));
    yield* restarted.record([record, { ...record, id: "next:plan", runId: "next" }]);
    const conflict = yield* restarted
      .record([{ ...record, sourceCommit: "different" }])
      .pipe(Effect.flip);
    expect(conflict._tag).toBe("ReconnectComparisonError");
    const raw = yield* fs.readFileString(
      `${directory}/userdata/logs/reconnect-comparison-v2.ndjson`,
    );
    expect(raw.trim().split("\n")).toHaveLength(2);
    expect(raw.match(/"id":"run:plan"/g)).toHaveLength(1);
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
