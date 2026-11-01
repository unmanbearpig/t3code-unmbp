import { ReconnectExperiment as ConnectionDiagnosticsExperiment } from "@t3tools/client-runtime/connection";
import { ConnectionDiagnostics } from "@t3tools/client-runtime/connection";
import { EnvironmentId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";

import { mobileJsLaunch } from "./launch-clock";
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
    readonly background: () => void;
    readonly wakeup: (context: MobileWakeupContext) => void;
    readonly visible: (
      thread: { readonly environmentId: string; readonly threadId: string } | null,
    ) => void;
    readonly read: () => ReadonlyArray<ConnectionLogRecord>;
    readonly subscribe: (listener: () => void) => () => void;
    readonly clear: () => void;
    readonly storageAvailable: () => boolean;
    readonly experimentEnabled: () => boolean;
    readonly setExperimentEnabled: (
      enabled: boolean,
    ) => Effect.Effect<void, MobileDatabase.MobileDatabaseError>;
    readonly environments: (ids: ReadonlyArray<string>) => void;
    readonly begin: (environmentId: string, reset: boolean) => void;
    readonly arm: (environmentId: string) => "old" | "new";
    readonly network: (type: "wifi" | "cellular" | "other") => void;
    readonly prompt: () => {
      readonly id: string;
      readonly expiresAt: number;
    } | null;
    readonly guess: (id: string, guess: "old" | "new" | "skip") => void;
    readonly dismissPrompt: (id: string) => void;
  }
>()("@t3tools/mobile/MobileConnectionLog") {}

const make = Effect.gen(function* () {
  const database = yield* MobileDatabase.MobileDatabase;
  const writes = yield* Queue.sliding<void>(1);
  const listeners = new Set<() => void>();
  let records: ReadonlyArray<ConnectionLogRecord> = [];
  let storageAvailable = true;
  let networkType: ConnectionLogRecord["networkType"] = "other";
  let experimentEnabled = yield* database
    .loadCache(STORAGE_ENVIRONMENT, "reconnect-experiment", "enabled")
    .pipe(
      Effect.map((stored) => stored._tag === "Some" && stored.value === "true"),
      Effect.orElseSucceed(() => false),
    );
  let prompt: {
    readonly id: string;
    readonly expiresAt: number;
  } | null = null;
  const prompted = new Set<string>();
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
  const changed = (next: ConnectionLogRecord) => {
    const previous = records.find((value) => value.id === next.id);
    const eventGuess = records.find(
      (value) => value.eventId !== null && value.eventId === next.eventId && value.guess !== null,
    )?.guess;
    const record = { ...next, guess: previous?.guess ?? eventGuess ?? next.guess };
    records = appendConnectionLog(records, record);
    const eventId = assembler.completedEvent();
    if (experimentEnabled && eventId && !prompted.has(eventId)) {
      prompted.add(eventId);
      if (prompted.size > CONNECTION_LOG_LIMIT) prompted.delete(prompted.values().next().value!);
      prompt = { id: eventId, expiresAt: Date.now() + 10_000 };
    }
    pending.set(record.id, record);
    if (pending.size > CONNECTION_LOG_LIMIT) pending.delete(pending.keys().next().value!);
    Queue.offerUnsafe(writes, undefined);
    notify();
  };
  const assembler = createConnectionLog(
    () => ({ wall: Date.now(), monotonic: performance.now() }),
    changed,
    () => ({
      experiment: experimentEnabled,
      arm: experimentEnabled && Math.random() < 0.5 ? "old" : "new",
      networkType,
    }),
    mobileJsLaunch,
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
    begin: assembler.ensureTrial,
    wakeup: (context) => {
      prompt = null;
      assembler.wakeup(context);
      notify();
    },
    background: () => {
      assembler.background();
      prompt = null;
      notify();
    },
    experimentEnabled: () => experimentEnabled,
    setExperimentEnabled: (enabled) =>
      database
        .saveCache(
          STORAGE_ENVIRONMENT,
          "reconnect-experiment",
          "enabled",
          1,
          JSON.stringify(enabled),
        )
        .pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              experimentEnabled = enabled;
              if (!enabled) prompt = null;
              notify();
            }),
          ),
        ),
    network: (type) => {
      networkType = type;
      assembler.network(type);
    },
    prompt: () => prompt,
    dismissPrompt: (id) => {
      if (prompt?.id === id) {
        prompt = null;
        notify();
      }
    },
    guess: (id, guess) => {
      const eventRecords = records.filter((value) => value.eventId === id);
      if (
        !eventRecords.length ||
        eventRecords.some((record) => !record.experiment || record.guess !== null)
      )
        return;
      for (const record of eventRecords) changed({ ...record, guess });
      if (prompt?.id === id) {
        prompt = null;
        notify();
      }
    },
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
      prompt = null;
      prompted.clear();
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

export const layerExperiment = Layer.effect(
  // The mobile log chooses the arm before the shared runtime starts work.
  ConnectionDiagnosticsExperiment.ReconnectExperiment,
  Effect.map(MobileConnectionLog, (log) => ({
    begin: log.begin,
    arm: log.arm,
    environments: log.environments,
  })),
);
