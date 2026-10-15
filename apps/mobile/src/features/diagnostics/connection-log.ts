import { ConnectionDiagnostics } from "@t3tools/client-runtime/connection";
import { EnvironmentId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";

import * as MobileDatabase from "../../persistence/mobile-database";
import {
  appendConnectionLog,
  CONNECTION_LOG_LIMIT,
  createConnectionLog,
  readConnectionLog,
  type ConnectionLogRecord,
  type MobileWakeupContext,
} from "./connection-log-model";

const STORAGE_ENVIRONMENT = EnvironmentId.make("mobile-diagnostics");

export class MobileConnectionLog extends Context.Service<
  MobileConnectionLog,
  {
    readonly record: (event: ConnectionDiagnostics.ConnectionDiagnosticEvent) => void;
    readonly wakeup: (context: MobileWakeupContext) => void;
    readonly visible: (
      thread: { readonly environmentId: string; readonly threadId: string } | null,
    ) => void;
    readonly read: () => ReadonlyArray<ConnectionLogRecord>;
    readonly subscribe: (listener: () => void) => () => void;
    readonly clear: () => void;
    readonly storageAvailable: () => boolean;
  }
>()("@t3tools/mobile/MobileConnectionLog") {}

const make = Effect.gen(function* () {
  const database = yield* MobileDatabase.MobileDatabase;
  const writes = yield* Queue.sliding<void>(1);
  const listeners = new Set<() => void>();
  let records: ReadonlyArray<ConnectionLogRecord> = [];
  let storageAvailable = true;
  const stored = yield* database.listCache("connection-log").pipe(
    Effect.catch(() => {
      storageAvailable = false;
      return Effect.succeed<ReadonlyArray<string>>([]);
    }),
  );
  records = stored
    .slice(-CONNECTION_LOG_LIMIT)
    .flatMap((payload) => readConnectionLog(`[${payload}]`))
    .sort(
      (a, b) =>
        a.timestamp.localeCompare(b.timestamp) ||
        Number(a.id.split("-").at(-1)) - Number(b.id.split("-").at(-1)),
    );
  const pending = new Map<string, ConnectionLogRecord>();
  let clearRequested = false;
  const notify = () => {
    for (const listener of listeners) listener();
  };
  const changed = (record: ConnectionLogRecord) => {
    records = appendConnectionLog(records, record);
    pending.set(record.id, record);
    if (pending.size > CONNECTION_LOG_LIMIT) pending.delete(pending.keys().next().value!);
    Queue.offerUnsafe(writes, undefined);
    notify();
  };
  const assembler = createConnectionLog(
    () => ({ wall: Date.now(), monotonic: performance.now() }),
    changed,
  );
  yield* Queue.take(writes).pipe(
    Effect.andThen(
      Effect.gen(function* () {
        const clear = clearRequested;
        clearRequested = false;
        const updates = [...pending.values()];
        pending.clear();
        if (clear) yield* database.clearCacheKind(STORAGE_ENVIRONMENT, "connection-log");
        // Coalesce in memory, then upsert only changed rows. A stage never rewrites the history.
        for (const record of updates)
          yield* database.saveCache(
            STORAGE_ENVIRONMENT,
            "connection-log",
            record.id,
            1,
            JSON.stringify(record),
          );
      }),
    ),
    Effect.match({
      onSuccess: () => {
        if (!storageAvailable) {
          storageAvailable = true;
          records = [...records];
          notify();
        }
      },
      onFailure: () => {
        storageAvailable = false;
        records = [...records];
        notify();
      },
    }),
    Effect.forever,
    Effect.forkScoped,
  );
  return MobileConnectionLog.of({
    ...assembler,
    read: () => records,
    storageAvailable: () => storageAvailable,
    subscribe: (listener) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    clear: () => {
      assembler.clear();
      records = [];
      pending.clear();
      clearRequested = true;
      Queue.offerUnsafe(writes, undefined);
      notify();
    },
  });
});

export const layer = Layer.effect(MobileConnectionLog, make);

export const layerObserver = Layer.effect(
  ConnectionDiagnostics.ConnectionDiagnostics,
  Effect.map(MobileConnectionLog, (log) => ({ record: log.record })),
);
