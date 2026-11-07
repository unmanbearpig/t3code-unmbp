import {
  EnvironmentId,
  WS_METHODS,
  type ReconnectSample,
  type ServerConfigStreamEvent,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as TestClock from "effect/testing/TestClock";
import { SnapshotExperiment } from "../connection/snapshotExperiment.ts";
import { makeReconnectMeasurement } from "./reconnectMeasurement.ts";
import type { WsRpcProtocolClient } from "./protocol.ts";

describe("reconnect measurement", () => {
  it.effect(
    "freezes readiness traffic before probing and durably acknowledges one measured trial",
    () =>
      Effect.gen(function* () {
        const probeStarted = yield* Deferred.make<void>();
        const probeReply = yield* Deferred.make<void>();
        const uploaded = yield* Deferred.make<void>();
        const saved = new Map<string, ReconnectSample>();
        const environmentId = EnvironmentId.make("test");
        let acknowledged = 0;
        const client = {
          [WS_METHODS.serverProbe]: () =>
            Deferred.succeed(probeStarted, undefined).pipe(
              Effect.andThen(Deferred.await(probeReply)),
            ),
          [WS_METHODS.serverRecordReconnectSamples]: (input: {
            samples: ReadonlyArray<ReconnectSample>;
          }) =>
            Effect.sync(() => {
              expect(input.samples).toHaveLength(1);
              expect(input.samples[0]).toMatchObject({
                arm: "old",
                configReadyMs: 25,
                shellReadyMs: 25,
                rpcRttMs: 125,
                receivedJsonBytes: 5,
                rpcAcksSent: 1,
              });
            }),
        } as unknown as WsRpcProtocolClient;
        yield* Effect.scoped(
          Effect.gen(function* () {
            const measurement = yield* makeReconnectMeasurement(environmentId);
            measurement.track("in", "hello");
            measurement.track("out", '{"_tag":"Ack","requestId":1}');
            yield* TestClock.adjust("25 millis");
            yield* measurement.snapshot({
              version: 1,
              type: "snapshot",
              config: {
                environment: { serverVersion: "test", capabilities: { connectionProbe: true } },
                reconnectMeasurements: true,
              },
            } as unknown as ServerConfigStreamEvent);
            yield* measurement.ready(client, "websocket");
            yield* Deferred.await(probeStarted);
            measurement.track("in", "excluded after readiness");
            yield* TestClock.adjust("125 millis");
            yield* Deferred.succeed(probeReply, undefined);
            yield* Deferred.await(uploaded);
            yield* measurement.ready(client, "websocket");
            expect(acknowledged).toBe(1);
          }),
        ).pipe(
          Effect.provideService(SnapshotExperiment, {
            begin: () => ({
              id: "trial",
              arm: "old",
              platform: "android",
              appVersion: "test",
              networkType: "cellular",
              readTraffic: () => null,
            }),
            save: (_environmentId, sample) =>
              Effect.sync(() => {
                saved.set(sample.id, sample);
              }),
            pending: () => Effect.sync(() => [...saved.values()]),
            acknowledge: (_environmentId, ids) =>
              Effect.sync(() => {
                acknowledged += ids.length;
              }).pipe(Effect.andThen(Deferred.succeed(uploaded, undefined)), Effect.asVoid),
          }),
        );
      }),
  );

  it.effect("retains a failed setup sample before any config arrives", () =>
    Effect.gen(function* () {
      const samples: ReconnectSample[] = [];
      yield* Effect.scoped(makeReconnectMeasurement(EnvironmentId.make("test"))).pipe(
        Effect.provideService(SnapshotExperiment, {
          begin: () => ({
            id: "failed-trial",
            arm: "new",
            platform: "android",
            appVersion: "test",
            networkType: "cellular",
            readTraffic: () => null,
          }),
          save: (_environmentId, sample) =>
            Effect.sync(() => {
              samples.push(sample);
            }),
          pending: () => Effect.succeed([]),
          acknowledge: () => Effect.void,
        }),
      );
      expect(samples).toMatchObject([
        {
          id: "failed-trial",
          outcome: "interrupted",
          configReadyMs: null,
          shellReadyMs: null,
          rpcRttMs: null,
        },
      ]);
    }),
  );
});
