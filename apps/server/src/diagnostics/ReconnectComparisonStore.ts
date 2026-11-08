import { ReconnectComparisonError, ReconnectComparisonRecord } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as ServerConfig from "../config.ts";

export class ReconnectComparisonStore extends Context.Service<
  ReconnectComparisonStore,
  {
    readonly record: (
      records: ReadonlyArray<ReconnectComparisonRecord>,
    ) => Effect.Effect<void, ReconnectComparisonError>;
  }
>()("t3/diagnostics/ReconnectComparisonStore") {}

export const make = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const config = yield* ServerConfig.ServerConfig;
  const lock = yield* Semaphore.make(1);
  const filename = path.join(config.logsDir, "reconnect-comparison-v2.ndjson");
  const decode = Schema.decodeUnknownEffect(Schema.fromJsonString(ReconnectComparisonRecord));
  const encode = Schema.encodeEffect(Schema.fromJsonString(ReconnectComparisonRecord));
  const seen = new Map<string, string>();
  let initialized = false;
  return ReconnectComparisonStore.of({
    record: Effect.fn("ReconnectComparisonStore.record")(
      function* (records) {
        if (!initialized) {
          if (yield* fs.exists(filename)) {
            const raw = yield* fs.readFileString(filename);
            for (const line of raw.split("\n").filter(Boolean)) {
              const record = yield* decode(line);
              seen.set(record.id, yield* encode(record));
            }
          }
          initialized = true;
        }
        const pending = new Map<string, string>();
        for (const record of records) {
          const encoded = yield* encode(record);
          const known = seen.get(record.id) ?? pending.get(record.id);
          if (known !== undefined && known !== encoded) {
            return yield* new ReconnectComparisonError({
              cause: "Conflicting reconnect record ID.",
            });
          }
          if (!seen.has(record.id)) pending.set(record.id, encoded);
        }
        if (pending.size === 0) return;
        yield* fs.makeDirectory(config.logsDir, { recursive: true });
        yield* fs.writeFileString(filename, [...pending.values()].join("\n") + "\n", {
          flag: "a",
          mode: 0o600,
        });
        for (const [id, record] of pending) seen.set(id, record);
      },
      lock.withPermit,
      Effect.mapError((cause) => new ReconnectComparisonError({ cause })),
    ),
  });
});
export const layer = Layer.effect(ReconnectComparisonStore, make);
