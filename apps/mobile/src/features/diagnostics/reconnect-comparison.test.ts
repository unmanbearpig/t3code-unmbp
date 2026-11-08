import type { ConnectionCatalogEntry } from "@t3tools/client-runtime/connection";
import type { RpcSession } from "@t3tools/client-runtime/rpc";
import type { PreparedConnection } from "../../../../../packages/client-runtime/src/connection/model.ts";
vi.mock("expo-constants", () => ({
  default: { expoConfig: { android: { package: "fixture.preview" } } },
}));
import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import {
  EnvironmentId,
  ReconnectComparisonRecord,
  WS_METHODS,
  type ServerConfig,
} from "@t3tools/contracts";
import { EnvironmentRegistry, EnvironmentSupervisor } from "@t3tools/client-runtime/connection";
import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
} from "../../../../../packages/client-runtime/src/connection/model.ts";
import { v2ShellSnapshot } from "../../../../../packages/client-runtime/src/state/orchestrationV2TestFixtures.ts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SubscriptionRef from "effect/SubscriptionRef";
import * as TestClock from "effect/testing/TestClock";
import * as Database from "../../persistence/mobile-database";
import { make, ROUNDS } from "./reconnect-comparison";

const decoder = Schema.decodeUnknownSync(Schema.fromJsonString(ReconnectComparisonRecord));

vi.mock("react-native", () => ({
  AppState: { currentState: "active", addEventListener: () => ({ remove() {} }) },
}));
vi.mock("expo-network", () => ({
  NetworkStateType: { CELLULAR: "cellular", WIFI: "wifi" },
  getNetworkStateAsync: async () => ({ type: "cellular" }),
  addNetworkStateListener: () => ({ remove() {} }),
}));
vi.mock("expo-crypto", () => ({
  randomUUID: () => "00000000-0000-4000-8000-000000000001",
  CryptoDigestAlgorithm: { SHA256: "SHA-256" },
  digestStringAsync: async () => "fixture-state-hash",
}));

it.effect.each(["timeout", "interrupted"] as const)(
  "preserves the schedule and a scored %s attempt",
  (outcome) =>
    Effect.gen(function* () {
      const rows = new Map<string, string>();
      const saved: Array<ReconnectComparisonRecord> = [];
      const uploaded = new Map<string, ReconnectComparisonRecord>();

      const service = yield* make.pipe(
        Effect.provide(
          Layer.mock(Database.MobileDatabase)({
            loadCache: (_env, _kind, key) => Effect.succeed(Option.fromUndefinedOr(rows.get(key))),
            saveCache: (_env, _kind, key, _version, payload) =>
              Effect.sync(() => {
                rows.set(key, payload);
                if (payload.startsWith("{")) saved.push(decoder(payload));
              }),
            listCache: () => Effect.succeed([...rows.values()]),
            removeCache: (_env, _kind, key) =>
              Effect.sync(() => {
                rows.delete(key);
              }),
          }),
        ),
      );
      const id = EnvironmentId.make("fixture");
      const state = yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE);
      const stalled = yield* Deferred.make<void>();
      const complete = {
        client: {
          [WS_METHODS.serverRecordReconnectComparison]: (input: {
            records: ReadonlyArray<ReconnectComparisonRecord>;
          }) =>
            Effect.sync(() => {
              for (const record of input.records) uploaded.set(record.id, record);
            }),
        },
      } as unknown as RpcSession;
      let connection = 0;
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: new PrimaryConnectionTarget({
          environmentId: id,
          label: "Fixture",
          httpBaseUrl: "https://fixture.invalid",
          wsBaseUrl: "wss://fixture.invalid",
        }),
        state,
        session: yield* SubscriptionRef.make(Option.some(complete)),
        prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
        disconnect: SubscriptionRef.set(state, AVAILABLE_CONNECTION_STATE),
        retryNow: Effect.void,
        connect: Effect.gen(function* () {
          if (!service.read().running) return;
          connection++;
          const observer = service.control.observe(id, "https://fixture.invalid");
          expect(observer).toBeDefined();
          observer!.config({
            providers: [],
            environment: { serverVersion: "fixture" },
          } as unknown as ServerConfig);
          if (connection === 3) {
            yield* Deferred.succeed(stalled, undefined);
            return;
          }
          yield* observer!.synchronized(v2ShellSnapshot);
        }),
      });
      const registry = yield* Effect.service(EnvironmentRegistry.EnvironmentRegistry).pipe(
        Effect.provide(
          Layer.mock(EnvironmentRegistry.EnvironmentRegistry)({
            entries: yield* SubscriptionRef.make<
              ReadonlyMap<EnvironmentId, ConnectionCatalogEntry>
            >(new Map()),
            networkStatus: yield* SubscriptionRef.make<"online" | "offline" | "unknown">("online"),
            run: (_id, effect) =>
              effect.pipe(
                Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
              ),
          }),
        ),
      );
      const running = yield* service.run(id, registry).pipe(Effect.forkScoped);
      yield* Deferred.await(stalled);
      const plan = saved.find((row) => row.kind === "plan");
      expect(plan?.kind).toBe("plan");
      if (plan?.kind !== "plan") return yield* Effect.die("Missing plan");
      expect(plan.assignments).toHaveLength(2 + ROUNDS * 2);
      for (let i = 0; i < plan.assignments.length; i += 2) {
        expect(new Set(plan.assignments.slice(i, i + 2).map((row) => row.mode))).toEqual(
          new Set(["baseline", "proposed"]),
        );
      }
      if (outcome === "timeout") {
        yield* TestClock.adjust("60 seconds");
        yield* Fiber.join(running);
      } else yield* Fiber.interrupt(running);
      const attempts = saved.filter((row) => row.kind === "attempt");
      expect(attempts[2]).toMatchObject({
        outcome,
        cappedReadyMs: 60_000,
        warmup: false,
        index: 2,
      });
      expect(attempts).toHaveLength(outcome === "timeout" ? 62 : 3);
      expect(service.read().running).toBe(false);
      expect(service.control.mode(id)).toBe("baseline");
      expect(uploaded.size).toBe(1 + attempts.length);
    }).pipe(Effect.scoped),
);
