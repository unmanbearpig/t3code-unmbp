import {
  ReconnectMetricsError,
  type ReconnectLoopSample,
  type ReconnectSample,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Semaphore from "effect/Semaphore";
import * as ServerConfig from "../config.ts";

export class ReconnectMetrics extends Context.Service<
  ReconnectMetrics,
  {
    readonly record: (
      samples: ReadonlyArray<ReconnectSample>,
    ) => Effect.Effect<void, ReconnectMetricsError>;
    readonly recordLoop: (
      samples: ReadonlyArray<ReconnectLoopSample>,
    ) => Effect.Effect<void, ReconnectMetricsError>;
  }
>()("t3/diagnostics/ReconnectMetrics") {}

const make = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const config = yield* ServerConfig.ServerConfig;
  const lock = yield* Semaphore.make(1);
  const record = Effect.fn("ReconnectMetrics.append")(
    function* (name: string, samples: ReadonlyArray<ReconnectSample | ReconnectLoopSample>) {
      const filename = path.join(config.logsDir, name);
      if (samples.length === 0) return;
      yield* fs.makeDirectory(config.logsDir, { recursive: true });
      const size = yield* fs.stat(filename).pipe(
        Effect.map((stat) => Number(stat.size)),
        Effect.orElseSucceed(() => 0),
      );
      if (size >= 4 * 1024 * 1024) {
        yield* fs.remove(`${filename}.previous`, { force: true });
        yield* fs.rename(filename, `${filename}.previous`);
      }
      yield* fs.writeFileString(
        filename,
        samples.map((sample) => JSON.stringify(sample)).join("\n") + "\n",
        { flag: "a", mode: 0o600 },
      );
    },
    lock.withPermit,
    Effect.mapError((cause) => new ReconnectMetricsError({ cause })),
  );
  return ReconnectMetrics.of({
    record: Effect.fn("ReconnectMetrics.record")((samples) =>
      record("reconnect-samples.ndjson", samples),
    ),
    recordLoop: Effect.fn("ReconnectMetrics.recordLoop")((samples) =>
      record("reconnect-loop-samples.ndjson", samples),
    ),
  });
});

export const layer = Layer.effect(ReconnectMetrics, make);
