import { type EnvironmentId, type ReconnectLoopSample, WS_METHODS } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

import type { WsRpcProtocolClient } from "./protocol.ts";

export class ReconnectLoopStorageError extends Schema.TaggedError<ReconnectLoopStorageError>()(
  "ReconnectLoopStorageError",
  { cause: Schema.Defect() },
) {
  override get message(): string {
    return "Could not persist reconnect loop results.";
  }
}

export class ReconnectLoopOutbox extends Context.Reference<{
  readonly save: (
    environmentId: EnvironmentId,
    sample: ReconnectLoopSample,
  ) => Effect.Effect<void, ReconnectLoopStorageError>;
  readonly pending: (
    environmentId: EnvironmentId,
  ) => Effect.Effect<ReadonlyArray<ReconnectLoopSample>, ReconnectLoopStorageError>;
  readonly flush: (
    environmentId: EnvironmentId,
    client: WsRpcProtocolClient,
  ) => Effect.Effect<number, ReconnectLoopStorageError>;
} | null>("@t3tools/client-runtime/rpc/ReconnectLoopOutbox", { defaultValue: () => null }) {}

export const make = Effect.fn("ReconnectLoopOutbox.make")(function* (storage: {
  readonly save: NonNullable<Context.Service.Shape<typeof ReconnectLoopOutbox>>["save"];
  readonly pending: NonNullable<Context.Service.Shape<typeof ReconnectLoopOutbox>>["pending"];
  readonly acknowledge: (
    environmentId: EnvironmentId,
    ids: ReadonlyArray<string>,
  ) => Effect.Effect<void, ReconnectLoopStorageError>;
}) {
  const lock = yield* Semaphore.make(1);
  const flush = Effect.fn("ReconnectLoopOutbox.flush")(function* (
    environmentId: EnvironmentId,
    client: WsRpcProtocolClient,
  ) {
    while (true) {
      const pending = yield* storage.pending(environmentId);
      const batch = pending.slice(0, 20);
      if (batch.length === 0) return 0;
      const delivered = yield* client[WS_METHODS.serverRecordReconnectLoopSamples]({
        samples: batch,
      }).pipe(
        Effect.timeoutOption("5 seconds"),
        Effect.map((result) => result._tag === "Some"),
        Effect.orElseSucceed(() => false),
      );
      if (!delivered) return pending.length;
      yield* storage.acknowledge(
        environmentId,
        batch.map((sample) => sample.id),
      );
    }
  }, lock.withPermit);
  return { save: storage.save, pending: storage.pending, flush };
});
