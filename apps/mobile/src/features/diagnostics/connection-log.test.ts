import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import { vi } from "vite-plus/test";

const openDatabaseAsync = vi.hoisted(() => vi.fn());
vi.mock("expo-sqlite", () => ({ openDatabaseAsync }));

import * as MobileDatabase from "../../persistence/mobile-database";
import * as ConnectionLog from "./connection-log";

const logLayer = ConnectionLog.layer.pipe(Layer.provide(MobileDatabase.layer));

describe("on-device connection log storage", () => {
  it.effect("persists connection timings with the A/B experiment disabled", () =>
    Effect.gen(function* () {
      const saved = yield* Deferred.make<string>();
      yield* Effect.gen(function* () {
        const log = yield* ConnectionLog.MobileConnectionLog;
        expect(log.experimentEnabled()).toBe(false);
        log.record({ kind: "attempt-start", environmentId: "environment" });
        log.record({
          kind: "attempt-end",
          environmentId: "environment",
          outcome: "connected",
          error: null,
        });
        log.record({
          kind: "shell-sync",
          environmentId: "environment",
          milestone: "live",
          completion: "marker",
        });
        const payload = yield* Deferred.await(saved);
        expect(JSON.parse(payload)).toMatchObject({
          experiment: false,
          arm: "new",
          outcome: "connected",
          shell: { outcome: "live", completion: "marker" },
        });
        expect(log.prompt()).toBeNull();
      }).pipe(
        Effect.provide(
          ConnectionLog.layer.pipe(
            Layer.provide(
              Layer.mock(MobileDatabase.MobileDatabase)({
                loadCache: () => Effect.succeedNone,
                listCache: () => Effect.succeed([]),
                saveCache: (_environment, kind, _key, _version, payload) =>
                  kind === "connection-log"
                    ? Deferred.succeed(saved, payload).pipe(Effect.asVoid)
                    : Effect.void,
              }),
            ),
          ),
        ),
      );
    }),
  );

  it.effect(
    "persists at most 2000 records, restores them on a fresh service and clears the stored log",
    () =>
      Effect.gen(function* () {
        const writes = yield* Queue.unbounded<string>();
        const { DatabaseSync } = yield* Effect.promise(() => import("node:sqlite"));
        const sqlite = new DatabaseSync(":memory:");
        yield* Effect.addFinalizer(() => Effect.sync(() => sqlite.close()));
        sqlite.exec("PRAGMA user_version = 1");
        let lastPayload = "";
        const database = {
          execAsync: async (sql: string) => {
            sqlite.exec(sql);
          },
          runAsync: async (sql: string, ...args: Array<string | number>) => {
            const result = sqlite.prepare(sql).run(...args);
            if (sql.includes("INSERT INTO client_cache") && args[1] === "connection-log")
              lastPayload = String(args[4]);
            if (sql.includes("OFFSET 2000")) Queue.offerUnsafe(writes, lastPayload);
            if (sql.includes("DELETE FROM client_cache WHERE environment_id = ? AND kind = ?"))
              Queue.offerUnsafe(writes, "cleared");
            return result;
          },
          getFirstAsync: async (sql: string, ...args: Array<string | number>) =>
            sqlite.prepare(sql).get(...args),
          getAllAsync: async (sql: string, ...args: Array<string | number>) =>
            sqlite.prepare(sql).all(...args),
          withExclusiveTransactionAsync: async (run: (transaction: unknown) => Promise<void>) => {
            await run(database);
          },
          // The test owns this in-memory database across service restarts.
          closeAsync: async () => undefined,
        };
        openDatabaseAsync.mockResolvedValue(database);
        yield* Effect.gen(function* () {
          const log = yield* ConnectionLog.MobileConnectionLog;
          expect(log.experimentEnabled()).toBe(false);
          yield* log.setExperimentEnabled(true);
          expect(log.experimentEnabled()).toBe(true);
          for (let i = 0; i < 2005; i++) {
            log.record({
              kind: "wakeup",
              environmentId: "environment",
              reason: "network-changed",
              hasSession: false,
              desired: true,
            });
            log.record({ kind: "attempt-start", environmentId: "environment" });
            log.record({
              kind: "attempt-end",
              environmentId: "environment",
              outcome: "failed",
              error: "network",
            });
            if (i === 2004) {
              log.record({
                kind: "shell-sync",
                environmentId: "environment",
                milestone: "live",
                completion: "marker",
              });
              log.record({
                kind: "cleanup",
                environmentId: "environment",
                cleanupId: 1,
                resource: "subscription",
                method: "shell",
                milestone: "finished",
                durationMs: 0,
                outcome: "interrupted",
              });
            }
          }
          expect(log.read()).toHaveLength(2000);
          yield* Queue.take(writes).pipe(
            Effect.repeat({
              until: (payload) => {
                const record: unknown = JSON.parse(payload);
                return (
                  typeof record === "object" &&
                  record !== null &&
                  "id" in record &&
                  typeof record.id === "string" &&
                  record.id.endsWith("-2005")
                );
              },
            }),
          );
          const stored = sqlite
            .prepare("SELECT payload FROM client_cache WHERE kind = 'connection-log'")
            .all();
          expect(stored).toHaveLength(2000);
          expect(JSON.stringify(stored)).not.toContain("environmentId");
          // Force separate disk appends so the SQL cap is exercised as well as coalescing.
          for (let i = 0; i < 5; i++) {
            log.record({
              kind: "wakeup",
              environmentId: "environment",
              reason: "network-changed",
              hasSession: false,
              desired: true,
            });
            log.record({ kind: "attempt-start", environmentId: "environment" });
            log.record({
              kind: "attempt-end",
              environmentId: "environment",
              outcome: "failed",
              error: "network",
            });
            yield* Queue.take(writes);
          }
          expect(
            sqlite.prepare("SELECT payload FROM client_cache WHERE kind = 'connection-log'").all(),
          ).toHaveLength(2000);
          log.record({
            kind: "wakeup",
            environmentId: "environment",
            reason: "application-active-probe",
            hasSession: false,
            desired: true,
          });
          log.record({ kind: "attempt-start", environmentId: "environment" });
          log.record({
            kind: "attempt-end",
            environmentId: "environment",
            outcome: "connected",
            error: null,
          });
          log.record({
            kind: "shell-sync",
            environmentId: "environment",
            milestone: "live",
            completion: "marker",
          });
          const trial = log.read().at(-1)!;
          expect(trial.experiment).toBe(true);
          expect(["old", "new"]).toContain(trial.arm);
          expect(log.prompt()?.id).toBe(trial.eventId);
          log.guess(trial.eventId!, "old");
          expect(log.prompt()).toBeNull();
          log.record({
            kind: "http",
            environmentId: "environment",
            path: "/:redacted",
            durationMs: 1,
            status: 200,
            outcome: "response",
          });
          expect(log.read().at(-1)?.guess).toBe("old");
          yield* Queue.take(writes).pipe(
            Effect.repeat({ until: (payload) => JSON.parse(payload).guess === "old" }),
          );
        }).pipe(Effect.provide(logLayer));
        yield* Effect.gen(function* () {
          const log = yield* ConnectionLog.MobileConnectionLog;
          expect(log.read()).toHaveLength(2000);
          expect(log.storageAvailable()).toBe(true);
          expect(log.experimentEnabled()).toBe(true);
          expect(log.read().at(-1)?.guess).toBe("old");
          const restored = log.read().find((record) => record.shell.outcome === "live");
          expect(restored?.shell.completion).toBe("marker");
          expect(restored?.cleanup).toMatchObject([
            { resource: "subscription", method: "shell", durationMs: 0, outcome: "interrupted" },
          ]);
          log.clear();
          expect(log.read()).toEqual([]);
          expect(log.experimentEnabled()).toBe(true);
          expect(yield* Queue.take(writes)).toBe("cleared");
        }).pipe(Effect.provide(logLayer));
        expect(
          sqlite.prepare("SELECT payload FROM client_cache WHERE kind = 'connection-log'").all(),
        ).toEqual([]);
      }),
  );

  it.effect("keeps diagnostics usable in memory when device storage fails", () => {
    openDatabaseAsync.mockRejectedValue(new Error("SQLite unavailable"));
    return Effect.gen(function* () {
      const log = yield* ConnectionLog.MobileConnectionLog;
      expect(log.storageAvailable()).toBe(false);
      log.record({ kind: "attempt-start", environmentId: "environment" });
      expect(log.read()).toHaveLength(1);
    }).pipe(Effect.provide(logLayer));
  });
});

it.effect(
  "prompts once after the event's environments finish and stores one guess on every row",
  () =>
    Effect.gen(function* () {
      const log = yield* ConnectionLog.MobileConnectionLog;
      yield* log.setExperimentEnabled(true);
      log.environments(["a", "b"]);
      for (const environmentId of ["a", "b"]) {
        log.begin(environmentId, true);
        log.record({ kind: "attempt-start", environmentId });
        log.record({ kind: "attempt-end", environmentId, outcome: "connected", error: null });
      }
      log.record({
        kind: "shell-sync",
        environmentId: "a",
        milestone: "live",
        completion: "marker",
      });
      expect(log.prompt()).toBeNull();
      log.record({
        kind: "shell-sync",
        environmentId: "b",
        milestone: "live",
        completion: "marker",
      });
      const eventId = log.prompt()!.id;
      expect(log.read().every((record) => record.eventId === eventId)).toBe(true);
      expect(new Set(log.read().map((record) => record.arm)).size).toBe(1);
      log.guess(eventId, "new");
      expect(log.prompt()).toBeNull();
      expect(log.read().map((record) => record.guess)).toEqual(["new", "new"]);
      log.record({
        kind: "shell-sync",
        environmentId: "a",
        milestone: "live",
        completion: "marker",
      });
      expect(log.prompt()).toBeNull();
      log.wakeup({
        launch: "resume",
        wakeup: "application-active-probe",
        backgroundMs: 1000,
        at: Date.now(),
        monotonic: performance.now(),
      });
      expect(log.prompt()).toBeNull();
    }).pipe(
      Effect.provide(
        ConnectionLog.layer.pipe(
          Layer.provide(
            Layer.mock(MobileDatabase.MobileDatabase)({
              loadCache: () => Effect.succeedNone,
              listCache: () => Effect.succeed([]),
              saveCache: () => Effect.void,
              clearCacheKind: () => Effect.void,
            }),
          ),
        ),
      ),
    ),
);
