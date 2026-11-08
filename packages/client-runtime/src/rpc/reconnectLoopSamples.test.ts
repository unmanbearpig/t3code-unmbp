import { expect, it } from "@effect/vitest";
import { EnvironmentId, type ReconnectLoopSample, WS_METHODS } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";

import type { WsRpcProtocolClient } from "./protocol.ts";
import { make, ReconnectLoopStorageError } from "./reconnectLoopSamples.ts";

it.effect.each(["transport", "acknowledgement"] as const)(
  "retains %s failures across outbox recreation and drains only the destination in bounded batches",
  (failure) =>
    Effect.gen(function* () {
      const environmentId = EnvironmentId.make("target");
      const other = EnvironmentId.make("other");
      const disk = new Map<EnvironmentId, Map<string, ReconnectLoopSample>>();
      let fail = true;
      const storage = {
        save: (env: EnvironmentId, sample: ReconnectLoopSample) =>
          Effect.sync(() => {
            let samples = disk.get(env);
            if (!samples) disk.set(env, (samples = new Map()));
            samples.set(sample.id, sample);
          }),
        pending: (env: EnvironmentId) => Effect.sync(() => [...(disk.get(env)?.values() ?? [])]),
        acknowledge: (env: EnvironmentId, ids: ReadonlyArray<string>) =>
          Effect.gen(function* () {
            if (fail && failure === "acknowledgement") {
              return yield* new ReconnectLoopStorageError({ cause: "disk write failed" });
            }
            for (const id of ids) disk.get(env)?.delete(id);
          }),
      };
      const serverRows = new Map<string, ReconnectLoopSample>();
      const batches: number[] = [];
      const client = {
        [WS_METHODS.serverRecordReconnectLoopSamples]: ({
          samples,
        }: {
          samples: ReadonlyArray<ReconnectLoopSample>;
        }) =>
          Effect.gen(function* () {
            batches.push(samples.length);
            if (fail && failure === "transport") return yield* Effect.fail("offline");
            for (const sample of samples) serverRows.set(sample.id, sample);
          }),
      } as unknown as WsRpcProtocolClient;
      const sample: ReconnectLoopSample = {
        version: 1,
        id: "run:0",
        runId: "run",
        at: "2026-11-08T12:00:00Z",
        round: 0,
        arm: "restart",
        pipelineArm: "new",
        platform: "android",
        appVersion: "test",
        serverVersion: "test",
        networkType: "cellular",
        transport: "websocket",
        ok: true,
        prepareMs: 1,
        configMs: 5,
        syncedMs: 10,
        jsonDown: 100,
        jsonUp: 10,
        sentRevisions: 10,
        reusedCatalogs: 10,
        projectFrames: 0,
        appTraffic: null,
      };
      const first = yield* make(storage);
      for (let index = 0; index < 26; index++) {
        yield* first.save(environmentId, { ...sample, id: `run:${index}`, round: index });
      }
      yield* first.save(other, { ...sample, id: "another-environment" });
      const result = yield* first.flush(environmentId, client).pipe(Effect.exit);
      expect(Exit.isFailure(result)).toBe(failure === "acknowledgement");
      expect(yield* storage.pending(environmentId)).toHaveLength(26);
      fail = false;
      batches.length = 0;
      const afterRestart = yield* make(storage);
      expect(yield* afterRestart.flush(environmentId, client)).toBe(0);
      expect(batches).toEqual([20, 6]);
      expect(yield* storage.pending(environmentId)).toEqual([]);
      expect(yield* storage.pending(other)).toHaveLength(1);
      expect(serverRows.size).toBe(26);
      expect(
        [...serverRows.values()].every(
          (row) => row.runId === "run" && row.arm === "restart" && row.networkType === "cellular",
        ),
      ).toBe(true);
    }),
);
