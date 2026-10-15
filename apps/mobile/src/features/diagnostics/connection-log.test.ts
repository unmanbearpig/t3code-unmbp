import { describe, expect, it } from "@effect/vitest";
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
  it.effect(
    "persists at most 200 records, restores them on a fresh service and clears the stored log",
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
            if (sql.includes("OFFSET 200")) Queue.offerUnsafe(writes, lastPayload);
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
          for (let i = 0; i < 205; i++) {
            log.record({ kind: "attempt-start", environmentId: "environment" });
            log.record({
              kind: "attempt-end",
              environmentId: "environment",
              outcome: "failed",
              error: "network",
            });
            if (i === 204) {
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
          expect(log.read()).toHaveLength(200);
          yield* Queue.take(writes).pipe(
            Effect.repeat({
              until: (payload) => {
                const record: unknown = JSON.parse(payload);
                return (
                  typeof record === "object" &&
                  record !== null &&
                  "id" in record &&
                  typeof record.id === "string" &&
                  record.id.endsWith("-205")
                );
              },
            }),
          );
          const stored = sqlite
            .prepare("SELECT payload FROM client_cache WHERE kind = 'connection-log'")
            .all();
          expect(stored).toHaveLength(200);
          expect(JSON.stringify(stored)).not.toContain("environmentId");
          // Force separate disk appends so the SQL cap is exercised as well as coalescing.
          for (let i = 0; i < 5; i++) {
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
          ).toHaveLength(200);
        }).pipe(Effect.provide(logLayer));
        yield* Effect.gen(function* () {
          const log = yield* ConnectionLog.MobileConnectionLog;
          expect(log.read()).toHaveLength(200);
          expect(log.storageAvailable()).toBe(true);
          const restored = log.read().find((record) => record.shell.outcome === "live");
          expect(restored?.shell.completion).toBe("marker");
          expect(restored?.cleanup).toMatchObject([
            { resource: "subscription", method: "shell", durationMs: 0, outcome: "interrupted" },
          ]);
          log.clear();
          expect(log.read()).toEqual([]);
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
