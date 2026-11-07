import { SnapshotExperiment } from "@t3tools/client-runtime/connection";
import { EnvironmentId, ReconnectSample, ReconnectTraffic } from "@t3tools/contracts";
import { requireOptionalNativeModule } from "expo";
import { randomUUID } from "expo-crypto";
import Constants from "expo-constants";
import { Platform } from "react-native";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Result from "effect/Result";
import * as MobileDatabase from "../../persistence/mobile-database";
import { MobileConnectionLog } from "./connection-log";

const readSample = Schema.decodeUnknownResult(ReconnectSample);
const readTraffic = Schema.decodeUnknownResult(ReconnectTraffic);
const StoredSample = Schema.Struct({ environmentId: Schema.String, sample: ReconnectSample });
const readStored = Schema.decodeUnknownResult(StoredSample);
const native = requireOptionalNativeModule<{ getAppNetworkTraffic?: () => unknown }>(
  "T3NativeControls",
);

function appTraffic(): ReconnectTraffic | null {
  try {
    const result = readTraffic(native?.getAppNetworkTraffic?.());
    return Result.isSuccess(result) ? result.success : null;
  } catch {
    return null;
  }
}

export const layer = Layer.effect(
  SnapshotExperiment.SnapshotExperiment,
  Effect.gen(function* () {
    const database = yield* MobileDatabase.MobileDatabase;
    const log = yield* MobileConnectionLog;
    const pending = new Map<string, { environmentId: string; sample: ReconnectSample }>();
    const persisted = yield* database
      .listCache("reconnect-sample")
      .pipe(Effect.orElseSucceed(() => []));
    for (const json of persisted) {
      try {
        const result = readStored(JSON.parse(json));
        if (Result.isSuccess(result)) pending.set(result.success.sample.id, result.success);
      } catch {
        // Interrupted writes and older sample schemas cannot participate in this trial.
      }
    }
    return {
      begin: () =>
        !log.experimentEnabled()
          ? undefined
          : {
              id: randomUUID(),
              arm: Math.random() < 0.5 ? ("old" as const) : ("new" as const),
              platform: Platform.OS === "android" ? ("android" as const) : ("ios" as const),
              appVersion: Constants.expoConfig?.version ?? "dev",
              networkType: log.networkType(),
              readTraffic: appTraffic,
            },
      save: Effect.fn("SnapshotExperiment.save")(function* (environmentId, sample) {
        const checked = readSample(sample);
        if (Result.isFailure(checked)) return;
        const value = { environmentId, sample: checked.success };
        pending.set(sample.id, value);
        yield* database
          .saveCache(environmentId, "reconnect-sample", sample.id, 1, JSON.stringify(value))
          .pipe(Effect.ignore);
        while (pending.size > 500) {
          const first = pending.entries().next().value;
          if (first === undefined) break;
          pending.delete(first[0]);
          yield* database
            .removeCache(EnvironmentId.make(first[1].environmentId), "reconnect-sample", first[0])
            .pipe(Effect.ignore);
        }
      }),
      pending: (environmentId) =>
        Effect.sync(() =>
          log.experimentEnabled()
            ? [...pending.values()]
                .filter((value) => value.environmentId === environmentId)
                .slice(0, 20)
                .map((value) => value.sample)
            : [],
        ),
      acknowledge: Effect.fn("SnapshotExperiment.acknowledge")(function* (environmentId, ids) {
        for (const id of ids) {
          pending.delete(id);
          yield* database.removeCache(environmentId, "reconnect-sample", id).pipe(Effect.ignore);
        }
      }),
    };
  }),
);
