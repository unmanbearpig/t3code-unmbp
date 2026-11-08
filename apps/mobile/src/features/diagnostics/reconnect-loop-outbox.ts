import { ReconnectLoopSamples } from "@t3tools/client-runtime/rpc";
import { EnvironmentId, ReconnectLoopSample } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import * as MobileDatabase from "../../persistence/mobile-database";

const StoredSample = Schema.Struct({ environmentId: EnvironmentId, sample: ReconnectLoopSample });
const decodeStored = Schema.decodeUnknownEffect(Schema.fromJsonString(StoredSample));
const storageError = (cause: unknown) =>
  new ReconnectLoopSamples.ReconnectLoopStorageError({ cause });

export const layer = Layer.effect(
  ReconnectLoopSamples.ReconnectLoopOutbox,
  Effect.gen(function* () {
    const database = yield* MobileDatabase.MobileDatabase;
    return yield* ReconnectLoopSamples.make({
      save: Effect.fn("ReconnectLoopOutbox.save")((environmentId, sample) =>
        database
          .saveCache(
            environmentId,
            "reconnect-loop-sample",
            sample.id,
            1,
            JSON.stringify({ environmentId, sample }),
          )
          .pipe(Effect.mapError(storageError)),
      ),
      pending: Effect.fn("ReconnectLoopOutbox.pending")(function* (environmentId) {
        const stored = yield* database.listCache("reconnect-loop-sample");
        const decoded = yield* Effect.forEach(stored, (json) => decodeStored(json));
        return decoded
          .filter((row) => row.environmentId === environmentId)
          .map((row) => row.sample);
      }, Effect.mapError(storageError)),
      acknowledge: Effect.fn("ReconnectLoopOutbox.acknowledge")((environmentId, ids) =>
        Effect.forEach(
          ids,
          (id) => database.removeCache(environmentId, "reconnect-loop-sample", id),
          { discard: true },
        ).pipe(Effect.mapError(storageError)),
      ),
    });
  }),
);
