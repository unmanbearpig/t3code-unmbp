import type { ConnectionCatalogEntry } from "@t3tools/client-runtime/connection";
import type { RpcSession } from "@t3tools/client-runtime/rpc";
import type { PreparedConnection } from "../../../../../packages/client-runtime/src/connection/model.ts";
vi.mock("expo-constants", () => ({
  default: { expoConfig: { android: { package: "fixture.preview" } } },
}));
import { expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import * as Crypto from "expo-crypto";
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
import * as Queue from "effect/Queue";
import * as Database from "../../persistence/mobile-database";
import { make, ROUNDS, NATURAL_PROTOCOL_ID } from "./reconnect-comparison";

const decoder = Schema.decodeUnknownSync(Schema.fromJsonString(ReconnectComparisonRecord));

it.effect("starts in baseline when diagnostics storage is unavailable", () =>
  Effect.gen(function* () {
    let writes = 0;
    const service = yield* make.pipe(
      Effect.provide(
        Layer.mock(Database.MobileDatabase)({
          loadCache: () =>
            Effect.fail(
              new Database.MobileDatabaseError({ operation: "load-cache", cause: "offline" }),
            ),
          saveCache: () =>
            Effect.sync(() => {
              writes++;
            }),
        }),
      ),
    );
    expect(service.read().mode).toBe("baseline");
    expect(service.read().randomized).toBe(true);
    expect(writes).toBe(0);
  }),
);

it.effect("reconnects in the previous mode when saving a mode fails", () =>
  Effect.gen(function* () {
    const service = yield* make.pipe(
      Effect.provide(
        Layer.mock(Database.MobileDatabase)({
          loadCache: () => Effect.succeedNone,
          saveCache: (_env, _kind, key) =>
            key === "mode"
              ? Effect.fail(
                  new Database.MobileDatabaseError({ operation: "save-cache", cause: "full" }),
                )
              : Effect.void,
        }),
      ),
    );
    const id = EnvironmentId.make("fixture");
    const state = yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE);
    let reconnects = 0;
    const supervisor = yield* Effect.service(EnvironmentSupervisor.EnvironmentSupervisor).pipe(
      Effect.provide(
        Layer.mock(EnvironmentSupervisor.EnvironmentSupervisor)({
          target: new PrimaryConnectionTarget({
            environmentId: id,
            label: "Fixture",
            httpBaseUrl: "https://fixture.invalid",
            wsBaseUrl: "wss://fixture.invalid",
          }),
          state,
          session: yield* SubscriptionRef.make(Option.none<RpcSession>()),
          prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
          disconnect: SubscriptionRef.set(state, AVAILABLE_CONNECTION_STATE),
          connect: Effect.sync(() => {
            reconnects++;
          }),
        }),
      ),
    );
    const registry = yield* Effect.service(EnvironmentRegistry.EnvironmentRegistry).pipe(
      Effect.provide(
        Layer.mock(EnvironmentRegistry.EnvironmentRegistry)({
          entries: yield* SubscriptionRef.make<ReadonlyMap<EnvironmentId, ConnectionCatalogEntry>>(
            new Map(),
          ),
          networkStatus: yield* SubscriptionRef.make<"online" | "offline" | "unknown">("online"),
          run: (_id, effect) =>
            effect.pipe(
              Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
            ),
        }),
      ),
    );
    const result = yield* service
      .choose({ environmentId: id, mode: "proposed" }, registry)
      .pipe(Effect.exit);
    expect(result._tag).toBe("Failure");
    expect(service.control.mode(id)).toBe("baseline");
    expect(reconnects).toBe(1);
  }),
);

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

it.effect("randomizes ordinary sessions once and keeps their assigned mode stable", () =>
  Effect.gen(function* () {
    const service = yield* make.pipe(
      Effect.provide(
        Layer.mock(Database.MobileDatabase)({
          loadCache: (_env, _kind, key) =>
            Effect.succeed(key === "randomized" ? Option.some("true") : Option.none()),
        }),
      ),
    );
    const id = EnvironmentId.make("fixture");
    using uuid = vi
      .spyOn(Crypto, "randomUUID")
      .mockReturnValueOnce("ff000000-0000-4000-8000-000000000001")
      .mockReturnValueOnce("00000000-0000-4000-8000-000000000001");
    service.control.beginSession(id);
    expect(service.control.mode(id)).toBe("baseline");
    expect(service.control.mode(id)).toBe("baseline");
    service.control.beginSession(id);
    expect(service.control.mode(id)).toBe("baseline");
    yield* service.control.observe(id, "https://fixture.invalid")!.synchronized(v2ShellSnapshot);
    service.control.beginSession(id);
    expect(service.control.mode(id)).toBe("proposed");
    expect(uuid).toHaveBeenCalledTimes(2);
  }),
);

it.effect("records ordinary retries with one arm and starts a new event after readiness", () =>
  Effect.gen(function* () {
    const rows = new Map<string, string>();
    const service = yield* make.pipe(
      Effect.provide(
        Layer.mock(Database.MobileDatabase)({
          loadCache: (_env, _kind, key) => Effect.succeed(Option.fromUndefinedOr(rows.get(key))),
          saveCache: (_env, _kind, key, _version, value) =>
            Effect.sync(() => {
              rows.set(key, value);
            }),
        }),
      ),
    );
    const id = EnvironmentId.make("fixture");
    using uuid = vi
      .spyOn(Crypto, "randomUUID")
      .mockReturnValueOnce("ff000000-0000-4000-8000-000000000001")
      .mockReturnValueOnce("12000000-0000-4000-8000-000000000001")
      .mockReturnValueOnce("00000000-0000-4000-8000-000000000001");
    service.control.beginSession(id);
    yield* service.control.beginAttempt?.(id) ?? Effect.void;
    yield* service.control.finishAttempt?.(id, "failed") ?? Effect.void;
    const attempts = () =>
      [...rows.values()]
        .filter((s) => s.startsWith("{"))
        .map((value) => decoder(value))
        .filter((r) => r.kind === "attempt");
    expect(attempts()).toHaveLength(1);
    expect(attempts()[0]).toMatchObject({
      mode: "baseline",
      outcome: "failed",
      socketCount: 0,
      protocolId: NATURAL_PROTOCOL_ID,
    });
    yield* Queue.take(service.recordsSaved);
    service.control.beginSession(id);
    expect(service.control.mode(id)).toBe("baseline");
    yield* service.control.beginAttempt(id);
    const observer = service.control.observe(id, "https://fixture.invalid")!;
    observer.socket();
    observer.config(
      { providers: [], environment: { serverVersion: "fixture" } } as unknown as ServerConfig,
      0,
    );
    yield* observer.synchronized(v2ShellSnapshot);
    yield* Queue.take(service.recordsSaved);
    expect(attempts()).toHaveLength(2);
    expect(attempts()[1]).toMatchObject({ mode: "baseline", outcome: "ready" });
    expect(attempts()[0]!.runId.split(".")[0]).toBe(attempts()[1]!.runId.split(".")[0]);
    service.control.beginSession(id);
    expect(service.control.mode(id)).toBe("proposed");
    expect(uuid).toHaveBeenCalledTimes(3);
    yield* service.control.beginAttempt(id);
    const pendingBefore = rows.size;
    yield* service.upload(id, {} as EnvironmentRegistry.EnvironmentRegistry["Service"]);
    expect(rows.size).toBe(pendingBefore);
    yield* TestClock.adjust("60 seconds");
    yield* Queue.take(service.recordsSaved);
    expect(attempts()[2]).toMatchObject({
      mode: "proposed",
      outcome: "timeout",
      cappedReadyMs: 60_000,
    });
    service.control.beginSession(id);
    expect(uuid).toHaveBeenCalledTimes(3);
  }).pipe(Effect.scoped),
);

it.effect.each(["timeout", "interrupted", "connecting", "unavailable"] as const)(
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
      const connectionRequested = yield* Deferred.make<void>();
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
      const session = yield* SubscriptionRef.make(
        outcome === "connecting" || outcome === "unavailable"
          ? Option.none<RpcSession>()
          : Option.some(complete),
      );
      let connection = 0;
      const supervisor = EnvironmentSupervisor.EnvironmentSupervisor.of({
        target: new PrimaryConnectionTarget({
          environmentId: id,
          label: "Fixture",
          httpBaseUrl: "https://fixture.invalid",
          wsBaseUrl: "wss://fixture.invalid",
        }),
        state,
        session,
        prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
        disconnect: SubscriptionRef.set(state, AVAILABLE_CONNECTION_STATE),
        retryNow: Effect.void,
        connect: Effect.gen(function* () {
          if (Option.isNone(yield* SubscriptionRef.get(session))) {
            yield* Deferred.succeed(connectionRequested, undefined);
            return;
          }
          if (!service.read().running) return;
          connection++;
          const observer = service.control.observe(id, "https://fixture.invalid");
          expect(observer).toBeDefined();
          observer!.socket();
          observer!.config(
            {
              providers: [],
              environment: { serverVersion: "fixture" },
            } as unknown as ServerConfig,
            0,
          );
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
      if (outcome === "connecting" || outcome === "unavailable") {
        yield* Deferred.await(connectionRequested);
        expect(saved).toHaveLength(0);
        if (outcome === "unavailable") {
          yield* TestClock.adjust("60 seconds");
          const result = yield* Fiber.join(running).pipe(Effect.exit);
          expect(result._tag).toBe("Failure");
          expect(saved).toHaveLength(0);
          expect(service.read().running).toBe(false);
          return;
        }
        yield* SubscriptionRef.set(session, Option.some(complete));
      }
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
      if (outcome !== "interrupted") {
        yield* TestClock.adjust("60 seconds");
        yield* Fiber.join(running);
      } else yield* Fiber.interrupt(running);
      const attempts = saved.filter((row) => row.kind === "attempt");
      expect(attempts[2]).toMatchObject({
        outcome: outcome === "interrupted" ? "interrupted" : "timeout",
        cappedReadyMs: 60_000,
        warmup: false,
        index: 2,
      });
      expect(attempts).toHaveLength(outcome === "interrupted" ? 3 : 62);
      expect(service.read().running).toBe(false);
      expect(service.control.mode(id)).toBe("baseline");
      expect(uploaded.size).toBe(1 + attempts.length);
    }).pipe(Effect.scoped),
);
