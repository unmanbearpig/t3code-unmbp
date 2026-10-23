import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type { ConnectionDiagnostics } from "@t3tools/client-runtime/connection";
import type { PerformanceReport } from "./performance-telemetry";

type Event = ConnectionDiagnostics.ConnectionDiagnosticEvent;
type Route = ConnectionDiagnostics.DiagnosticRoute;

export const CONNECTION_LOG_LIMIT = 2_000;
const DETAIL_LIMIT = 40;

export interface ConnectionLogEvent {
  readonly id: string;
  readonly timestamp: string;
  readonly timingOrigin: "js-launch" | "wakeup" | "attempt";
  readonly launch: "cold" | "resume" | "running";
  readonly wakeup: string;
  readonly backgroundMs: number | null;
  readonly environments: ReadonlyArray<number>;
  readonly visibleThreadEnvironment: number | null;
}

export interface ConnectionLogRecord {
  readonly recordVersion: 1 | 2 | 3 | 4;
  readonly event: ConnectionLogEvent | null;
  readonly eventId: string | null;
  readonly eventEnvironmentCount: number;
  readonly eventAssignment: "random" | "continuation";
  readonly legacyPacingSupported: boolean | null;
  readonly id: string;
  readonly timestamp: string;
  readonly experiment: boolean;
  readonly arm: "old" | "new";
  readonly networkType: "wifi" | "cellular" | "other";
  readonly networkChanged: boolean;
  readonly interruptedAfterMs: number | null;
  readonly guess: "old" | "new" | "skip" | null;
  readonly timingOrigin: "js-launch" | "wakeup" | "attempt";
  readonly environment: number;
  readonly launch: "cold" | "resume" | "running";
  readonly wakeup: string;
  readonly backgroundMs: number | null;
  readonly route: Route | null;
  readonly oldSocket: "present" | "closed" | "absent" | "unknown";
  readonly continuedFrom: string | null;
  readonly sessionId: number | null;
  readonly probe: { readonly result: string; readonly durationMs: number | null } | null;
  readonly attempts: ReadonlyArray<{
    readonly id: number | null;
    readonly startedAfterMs: number | null;
    readonly stagesMs: Readonly<Record<"preparing" | "opening" | "synchronizing", number>>;
    readonly route: Route | null;
    readonly outcome: string;
    readonly error: string | null;
  }>;
  readonly http: ReadonlyArray<{
    readonly path: string;
    readonly durationMs: number;
    readonly status: number | null;
    readonly outcome: string;
  }>;
  readonly detailsTruncated: boolean;
  readonly cleanup: ReadonlyArray<{
    readonly id: number;
    readonly resource: "session" | "subscription";
    readonly method: "shell" | "thread" | "other" | null;
    readonly startedAfterMs: number | null;
    readonly durationMs: number | null;
    readonly outcome: "completed" | "failed" | "interrupted" | null;
  }>;
  readonly shell: {
    readonly liveAfterMs: number | null;
    readonly completion: "marker" | "legacy" | null;
    readonly outcome: "waiting" | "live" | "failed" | "unknown";
  };
  readonly visibleThread: {
    readonly mode: "replay" | "snapshot" | null;
    readonly liveAfterMs: number | null;
    readonly completion: "marker" | "legacy" | null;
    readonly bytes: null;
    readonly outcome: "waiting" | "live" | "failed" | "interrupted" | "not-visible";
  };
  readonly durationMs: number;
  readonly outcome: string;
  readonly error: string | null;
}

export interface MobileWakeupContext {
  readonly launch: "resume" | "running";
  readonly wakeup: string;
  readonly backgroundMs: number | null;
  readonly at: number;
  readonly monotonic: number;
}

/** One assembler per JS process. Raw environment/thread identifiers never enter stored records. */
export function createConnectionLog(
  now: () => { readonly wall: number; readonly monotonic: number },
  changed: (record: ConnectionLogRecord) => void,
  metadata: () => Pick<ConnectionLogRecord, "experiment" | "arm" | "networkType"> = () => ({
    experiment: false,
    arm: "new",
    networkType: "other",
  }),
  launchTime?: { readonly wall: number; readonly monotonic: number },
) {
  let sequence = 0;
  let attemptSequence = 0;
  let eventSequence = 0;
  let environments: ReadonlyArray<string> = [];
  let environmentsKnown = false;
  let visible: { environmentId: string; threadId: string } | null = null;
  const seen = new Map<string, number>();
  let environmentSequence = 0;
  const environmentNumber = (id: string) => {
    let number = seen.get(id);
    if (number === undefined) {
      number = ++environmentSequence;
      seen.set(id, number);
    }
    return number;
  };
  const newEvent = (
    ids: ReadonlyArray<string>,
    context?: MobileWakeupContext,
    continuation?: Pick<ConnectionLogRecord, "experiment" | "arm">,
    cold = false,
    wakeup = "reconnect",
  ) => {
    const origin = context
      ? { wall: context.at, monotonic: context.monotonic }
      : cold && launchTime
        ? launchTime
        : now();
    const cohort = [...new Set(ids)];
    const pinned = visible && cohort.includes(visible.environmentId) ? visible : null;
    const event: ConnectionLogEvent = {
      id: `${now().wall}-event-${++eventSequence}`,
      timestamp: new Date(origin.wall).toISOString(),
      timingOrigin: context ? "wakeup" : cold && launchTime ? "js-launch" : "attempt",
      launch: context?.launch ?? (cold ? "cold" : "running"),
      wakeup: context?.wakeup ?? wakeup,
      backgroundMs: context?.backgroundMs ?? null,
      environments: cohort.map(environmentNumber),
      visibleThreadEnvironment: pinned ? environmentNumber(pinned.environmentId) : null,
    };
    return {
      ...metadata(),
      ...(continuation ? { experiment: continuation.experiment, arm: continuation.arm } : {}),
      assignment: continuation ? ("continuation" as const) : ("random" as const),
      origin,
      cohort,
      pinned,
      event,
    };
  };
  let trialEvent: ReturnType<typeof newEvent> | undefined;
  const contexts = new Map<string, ReturnType<typeof newEvent>>();
  const active = new Map<
    string,
    {
      record: ConnectionLogRecord;
      trial: ReturnType<typeof newEvent>;
      started: number;
      probeStarted: number | null;
      stage: "preparing" | "opening" | "synchronizing" | null;
      stageStarted: number;
      visibleThreadId: string | null;
    }
  >();
  const closed = new Set<string>();
  const publish = (entry: ReturnType<typeof begin>) => {
    entry.record = { ...entry.record, durationMs: Math.max(0, now().monotonic - entry.started) };
    changed(entry.record);
  };
  const begin = (
    environmentId: string,
    wakeup: string,
    hasSession?: boolean,
    selected?: ReturnType<typeof newEvent>,
  ) => {
    const time = now();
    const previous = active.get(environmentId);
    if (!trialEvent)
      trialEvent = newEvent(
        environmentsKnown ? environments : [environmentId],
        undefined,
        undefined,
        seen.size === 0,
      );
    // A native wakeup can precede catalog hydration. Its first participant
    // supplies the otherwise empty cohort before any record is published.
    const candidate = selected ?? trialEvent;
    if (!environmentsKnown && candidate.cohort.length === 0) {
      const initialized = {
        ...candidate,
        cohort: [environmentId],
        event: { ...candidate.event, environments: [environmentNumber(environmentId)] },
      };
      if (trialEvent === candidate) trialEvent = initialized;
      for (const [reason, context] of contexts) {
        if (context === candidate) contexts.set(reason, initialized);
      }
      selected = initialized;
    }
    // Retries retain their event clock. A connection added after the captured
    // cohort, or after its previous measurement finished, gets its own event.
    const unfinished =
      previous &&
      previous.record.outcome !== "idle" &&
      (previous.record.shell.outcome !== "live" ||
        previous.record.visibleThread.outcome === "waiting");
    const selection =
      (selected?.cohort.includes(environmentId) ? selected : undefined) ??
      (unfinished
        ? previous.trial
        : !previous && trialEvent.cohort.includes(environmentId)
          ? trialEvent
          : newEvent([environmentId], undefined, trialEvent, false, wakeup));
    const { origin, event } = selection;
    const entry = {
      trial: selection,
      record: {
        recordVersion: 4,
        event,
        eventId: event.id,
        eventAssignment: selection.assignment,
        eventEnvironmentCount: event.environments.length,
        legacyPacingSupported: previous?.record.legacyPacingSupported ?? null,
        id: `${time.wall}-${++sequence}`,
        timestamp: event.timestamp,
        experiment: selection.experiment,
        arm: selection.arm,
        networkType: selection.networkType,
        networkChanged: false,
        guess: null,
        interruptedAfterMs: null,
        timingOrigin: event.timingOrigin,
        environment: environmentNumber(environmentId),
        launch: event.launch,
        wakeup: event.wakeup,
        backgroundMs: event.backgroundMs,
        route: null,
        oldSocket: closed.has(environmentId)
          ? "closed"
          : hasSession === true
            ? "present"
            : hasSession === false
              ? "absent"
              : previous
                ? "unknown"
                : "absent",
        probe: null,
        continuedFrom: null,
        sessionId: null,
        attempts: [],
        http: [],
        cleanup: [],
        shell: { liveAfterMs: null, completion: null, outcome: "waiting" },
        detailsTruncated: false,
        visibleThread: {
          mode: null,
          liveAfterMs: null,
          completion: null,
          bytes: null,
          outcome: selection.pinned?.environmentId === environmentId ? "waiting" : "not-visible",
        },
        durationMs: 0,
        outcome: "pending",
        error: null,
      } as ConnectionLogRecord,
      started: origin.monotonic,
      probeStarted: null as number | null,
      stage: null as "preparing" | "opening" | "synchronizing" | null,
      stageStarted: time.monotonic,
      visibleThreadId:
        selection.pinned?.environmentId === environmentId ? selection.pinned.threadId : null,
    };
    if (previous) {
      const continuing = previous.record.attempts.at(-1)?.outcome === "pending";
      const releasing = previous.record.cleanup.some((item) => item.outcome === null);
      if (continuing || releasing) {
        endStage(previous, time.monotonic);
        const offset = previous.started - entry.started;
        entry.record = {
          ...entry.record,
          continuedFrom: previous.record.id,
          route: previous.record.route,
          oldSocket: continuing ? previous.record.oldSocket : entry.record.oldSocket,
          attempts: previous.record.attempts.map((attempt) => ({
            ...attempt,
            startedAfterMs:
              attempt.startedAfterMs === null ? null : attempt.startedAfterMs + offset,
          })),
          http: previous.record.http,
          cleanup: previous.record.cleanup.map((item) => ({
            ...item,
            startedAfterMs: item.startedAfterMs === null ? null : item.startedAfterMs + offset,
          })),
          detailsTruncated: previous.record.detailsTruncated,
        };
        entry.stage = previous.stage;
        entry.stageStarted = previous.stageStarted;
      }
      if (hasSession)
        entry.record = {
          ...entry.record,
          sessionId: previous.record.sessionId,
          route: previous.record.route,
        };
    }
    if (
      previous &&
      (previous.record.outcome === "pending" ||
        (["connected", "healthy"].includes(previous.record.outcome) &&
          (previous.record.shell.outcome !== "live" ||
            previous.record.visibleThread.outcome === "waiting")))
    ) {
      previous.record = { ...previous.record, outcome: "superseded" };
      publish(previous);
    }
    seen.set(environmentId, entry.record.environment);
    active.set(environmentId, entry);
    // Environments can be added and removed all day without growing diagnostic memory forever.
    if (active.size > CONNECTION_LOG_LIMIT) {
      const first = active.keys().next().value!;
      active.delete(first);
      seen.delete(first);
      closed.delete(first);
    }
    publish(entry);
    return entry;
  };
  const endStage = (entry: ReturnType<typeof begin>, time: number) => {
    const attempt = entry.record.attempts.at(-1);
    if (entry.stage === null || attempt === undefined) return;
    entry.record = {
      ...entry.record,
      attempts: [
        ...entry.record.attempts.slice(0, -1),
        {
          ...attempt,
          stagesMs: {
            ...attempt.stagesMs,
            [entry.stage]: attempt.stagesMs[entry.stage] + Math.max(0, time - entry.stageStarted),
          },
        },
      ],
    };
    entry.stageStarted = time;
  };
  return {
    environments: (ids: ReadonlyArray<string>) => {
      environmentsKnown = true;
      environments = [...new Set(ids)];
      for (const [id, entry] of active) {
        if (
          !environments.includes(id) &&
          entry.record.outcome !== "idle" &&
          (entry.record.shell.outcome !== "live" ||
            entry.record.visibleThread.outcome === "waiting")
        ) {
          entry.record = { ...entry.record, outcome: "idle" };
          publish(entry);
        }
      }
    },
    completedEvent: () => {
      if (!trialEvent) return null;
      const entries = trialEvent.cohort.map((id) => active.get(id));
      const rows = entries.map((entry) => entry?.record);
      if (
        !rows.length ||
        rows.some(
          (record) =>
            !record ||
            record.eventId !== trialEvent?.event.id ||
            (!["idle", "blocked", "offline"].includes(record.outcome) &&
              (!["connected", "healthy"].includes(record.outcome) ||
                record.interruptedAfterMs !== null ||
                record.shell.outcome !== "live" ||
                !["live", "not-visible"].includes(record.visibleThread.outcome))),
        )
      )
        return null;
      return rows.some((record) => record?.experiment && record.shell.completion === "marker")
        ? trialEvent.event.id
        : null;
    },
    clear: () => {
      active.clear();
      closed.clear();
      contexts.clear();
      trialEvent = undefined;
    },
    ensureTrial: (environmentId: string, reset: boolean) => {
      if (environmentsKnown && !environments.includes(environmentId)) return;
      const existing = active.get(environmentId);
      if (
        !existing ||
        (reset && ["connected", "healthy", "idle"].includes(existing.record.outcome))
      )
        begin(environmentId, "reconnect");
    },
    arm: (environmentId: string): "old" | "new" => active.get(environmentId)?.record.arm ?? "new",
    background: () => {
      for (const entry of active.values()) {
        if (
          entry.record.outcome !== "idle" &&
          entry.record.interruptedAfterMs === null &&
          (entry.record.shell.outcome !== "live" ||
            entry.record.visibleThread.outcome === "waiting")
        ) {
          entry.record = {
            ...entry.record,
            interruptedAfterMs: Math.max(0, now().monotonic - entry.started),
          };
          publish(entry);
        }
      }
    },
    network: (networkType: ConnectionLogRecord["networkType"]) => {
      for (const entry of active.values()) {
        if (
          entry.record.outcome !== "idle" &&
          (entry.record.shell.outcome !== "live" ||
            entry.record.visibleThread.outcome === "waiting") &&
          entry.record.networkType !== networkType &&
          !entry.record.networkChanged
        ) {
          entry.record = { ...entry.record, networkChanged: true };
          publish(entry);
        }
      }
    },
    wakeup: (next: MobileWakeupContext) => {
      // An attempt's arm also owns its finalizers. Carry that assignment across
      // a wakeup while it is unfinished; every environment joins the same arm.
      const cohort = environmentsKnown ? environments : [...active.keys()];
      const continuing = cohort
        .map((id) => active.get(id))
        .find(
          (entry) =>
            entry !== undefined &&
            entry.record.outcome !== "idle" &&
            (entry.record.attempts.at(-1)?.outcome === "pending" ||
              (["connected", "healthy"].includes(entry.record.outcome) &&
                (entry.record.shell.outcome === "waiting" ||
                  entry.record.visibleThread.outcome === "waiting"))),
        );
      trialEvent = newEvent(cohort, next, continuing?.record);
      contexts.set(next.wakeup, trialEvent);
      // Choose before any wakeup subscriber can start a shell/thread refresh.
      for (const environmentId of cohort) {
        const entry = active.get(environmentId);
        begin(
          environmentId,
          next.wakeup,
          entry?.record.sessionId !== null && entry !== undefined,
          trialEvent,
        );
      }
    },
    visible: (next: typeof visible) => {
      // Navigation after an event starts must not become cold catch-up time.
      // Pin visibility for future events and interrupt an abandoned measurement.
      visible = next;
      for (const [id, entry] of active) {
        if (
          entry.visibleThreadId !== null &&
          entry.record.visibleThread.outcome === "waiting" &&
          (next?.environmentId !== id || next.threadId !== entry.visibleThreadId)
        ) {
          entry.record = {
            ...entry.record,
            visibleThread: {
              ...entry.record.visibleThread,
              outcome: "interrupted",
              liveAfterMs: null,
            },
          };
          publish(entry);
        }
      }
    },
    record: (event: Event) => {
      let entry = active.get(event.environmentId);
      if (event.kind === "session-closed") {
        closed.add(event.environmentId);
        return;
      }
      if (event.kind === "wakeup") {
        if (event.reason === "credentials-changed") return;
        if (!event.desired) {
          if (
            entry &&
            entry.record.outcome !== "idle" &&
            (entry.record.shell.outcome !== "live" ||
              entry.record.visibleThread.outcome === "waiting")
          ) {
            entry.record = { ...entry.record, outcome: "idle" };
            publish(entry);
          }
          return;
        }
        const selected = contexts.get(event.reason);
        if (entry && entry.record.eventId === selected?.event.id) {
          return;
        }
        if (!selected)
          trialEvent = newEvent(
            environmentsKnown ? environments : [event.environmentId],
            undefined,
            undefined,
            false,
            event.reason,
          );
        entry = begin(event.environmentId, event.reason, event.hasSession, selected ?? trialEvent);
        return;
      }
      if (event.kind === "attempt-start") {
        if (!entry || ["connected", "healthy", "idle"].includes(entry.record.outcome))
          entry = begin(event.environmentId, "reconnect");
        if (entry.record.attempts.length >= DETAIL_LIMIT)
          entry.record = { ...entry.record, detailsTruncated: true };
        entry.record = {
          ...entry.record,
          attempts: [
            ...entry.record.attempts.slice(-(DETAIL_LIMIT - 1)),
            {
              id: ++attemptSequence,
              startedAfterMs: now().monotonic - entry.started,
              stagesMs: { preparing: 0, opening: 0, synchronizing: 0 },
              route: null,
              outcome: "pending",
              error: null,
            },
          ],
        };
        entry.record = { ...entry.record, sessionId: null };
        entry.stage = null;
        closed.delete(event.environmentId);
      }
      if (!entry) return;
      const time = now().monotonic;
      switch (event.kind) {
        case "replay-capability":
          entry.record = { ...entry.record, legacyPacingSupported: event.legacyPacingSupported };
          break;
        case "state":
          // Later socket loss belongs to the next reconnect, not a completed resume.
          if (entry.record.outcome === "connected" || entry.record.outcome === "healthy") return;
          entry.record = {
            ...entry.record,
            outcome: event.phase === "backoff" ? "failed" : event.phase,
            error: event.error,
          };
          break;
        case "stage": {
          endStage(entry, time);
          entry.stage = event.stage;
          entry.stageStarted = time;
          const attempt = entry.record.attempts.at(-1);
          if (event.route) entry.record = { ...entry.record, route: event.route };
          if (attempt && event.route)
            entry.record = {
              ...entry.record,
              attempts: [...entry.record.attempts.slice(0, -1), { ...attempt, route: event.route }],
            };
          break;
        }
        case "attempt-end": {
          endStage(entry, time);
          entry.stage = null;
          const attempt = entry.record.attempts.at(-1);
          entry.record = {
            ...entry.record,
            outcome: event.outcome,
            error: event.error,
            sessionId: event.outcome === "connected" ? (attempt?.id ?? null) : null,
            attempts: attempt
              ? [
                  ...entry.record.attempts.slice(0, -1),
                  { ...attempt, outcome: event.outcome, error: event.error },
                ]
              : entry.record.attempts,
          };
          break;
        }
        case "cleanup": {
          const existing = entry.record.cleanup.find((item) => item.id === event.cleanupId);
          const item = {
            id: event.cleanupId,
            resource: event.resource,
            method: event.method ?? null,
            startedAfterMs:
              existing?.startedAfterMs ??
              (event.milestone === "started" ? time - entry.started : null),
            durationMs: event.durationMs,
            outcome: event.outcome,
          };
          entry.record = {
            ...entry.record,
            cleanup: existing
              ? entry.record.cleanup.map((value) => (value.id === item.id ? item : value))
              : [...entry.record.cleanup.slice(-(DETAIL_LIMIT - 1)), item],
            detailsTruncated:
              entry.record.detailsTruncated ||
              (!existing && entry.record.cleanup.length >= DETAIL_LIMIT),
          };
          break;
        }
        case "shell-sync":
          if (entry.record.shell.outcome === "live" || entry.record.interruptedAfterMs !== null)
            return;
          entry.record = {
            ...entry.record,
            shell: {
              liveAfterMs: event.milestone === "live" ? Math.max(0, time - entry.started) : null,
              completion: event.completion,
              outcome: event.milestone === "started" ? "waiting" : event.milestone,
            },
          };
          break;
        case "probe":
          if (event.result === "started") entry.probeStarted = time;
          entry.record = {
            ...entry.record,
            ...(event.route ? { route: event.route } : {}),
            probe: {
              result: event.result,
              durationMs:
                event.result === "started" || entry.probeStarted === null
                  ? null
                  : Math.max(0, time - entry.probeStarted),
            },
            outcome: event.result === "answered" ? "healthy" : "pending",
          };
          break;
        case "http":
          entry.record = {
            ...entry.record,
            http: [
              ...entry.record.http.slice(-(DETAIL_LIMIT - 1)),
              {
                path: event.path,
                durationMs: event.durationMs,
                status: event.status,
                outcome: event.outcome,
              },
            ],
            detailsTruncated:
              entry.record.detailsTruncated || entry.record.http.length >= DETAIL_LIMIT,
          };
          break;
        case "thread-sync":
          if (
            entry.visibleThreadId !== event.threadId ||
            entry.record.visibleThread.outcome === "interrupted" ||
            entry.record.visibleThread.outcome === "live" ||
            entry.record.interruptedAfterMs !== null
          )
            return;
          entry.record = {
            ...entry.record,
            visibleThread: {
              mode: event.mode,
              liveAfterMs: event.milestone === "live" ? Math.max(0, time - entry.started) : null,
              completion: event.completion,
              bytes: null,
              outcome: event.milestone === "started" ? "waiting" : event.milestone,
            },
          };
          if (event.milestone === "failed")
            entry.record = { ...entry.record, error: event.error ?? "stream" };
          break;
      }
      publish(entry);
    },
  };
}

export function appendConnectionLog(
  records: ReadonlyArray<ConnectionLogRecord>,
  record: ConnectionLogRecord,
): ReadonlyArray<ConnectionLogRecord> {
  if (records.some((item) => item.id === record.id))
    return records.map((item) => (item.id === record.id ? record : item));
  return [...records, record].slice(-CONNECTION_LOG_LIMIT);
}

export function formatConnectionLog(
  records: ReadonlyArray<ConnectionLogRecord>,
  app: { readonly version: string; readonly build: string; readonly platform: string },
  performance?: PerformanceReport,
): string {
  return JSON.stringify(
    {
      format: "t3-mobile-connection-log",
      version: 4,
      experiment: {
        comparison: "B+C+M+I+L+S",
        shared: ["G"],
        unit: "wakeup event",
        legacyRecords:
          "recordVersion 1/2 retain the prior comparison; version 3 has historical event metadata",
        coldOrigin: "JavaScript entry",
      },
      app,
      records,
      ...(performance === undefined ? {} : { performance }),
    },
    null,
    2,
  );
}

const RouteSchema = Schema.Struct({
  targetKind: Schema.Literals([
    "PrimaryConnectionTarget",
    "BearerConnectionTarget",
    "RelayConnectionTarget",
    "SshConnectionTarget",
  ]),
  scheme: Schema.Literals(["https", "http", "unknown"]),
  hostType: Schema.Literals([
    "ts.net",
    "tailnet-ip",
    "lan",
    "loopback",
    "relay",
    "public",
    "unknown",
  ]),
});
const NullableNumber = Schema.NullOr(Schema.Number);
const DiagnosticError = Schema.NullOr(
  Schema.Literals([
    "network",
    "timeout",
    "transport",
    "endpoint-unavailable",
    "relay-unavailable",
    "remote-unavailable",
    "authentication",
    "configuration",
    "permission",
    "unsupported",
    "stream",
    "missing",
  ]),
);
const StoredRecord = Schema.Struct({
  recordVersion: Schema.optionalKey(Schema.Literals([1, 2, 3, 4])),
  event: Schema.optionalKey(
    Schema.NullOr(
      Schema.Struct({
        id: Schema.String.check(Schema.isPattern(/^\d+-event-\d+$/)),
        timestamp: Schema.String,
        timingOrigin: Schema.Literals(["js-launch", "wakeup", "attempt"]),
        launch: Schema.Literals(["cold", "resume", "running"]),
        wakeup: Schema.String,
        backgroundMs: NullableNumber,
        environments: Schema.Array(Schema.Number),
        visibleThreadEnvironment: NullableNumber,
      }),
    ),
  ),
  eventId: Schema.optionalKey(
    Schema.NullOr(Schema.String.check(Schema.isPattern(/^\d+-event-\d+$/))),
  ),
  eventEnvironmentCount: Schema.optionalKey(Schema.Number),
  eventAssignment: Schema.optionalKey(Schema.Literals(["random", "continuation"])),
  legacyPacingSupported: Schema.optionalKey(Schema.NullOr(Schema.Boolean)),
  id: Schema.String.check(Schema.isPattern(/^\d+-\d+$/)),
  experiment: Schema.optionalKey(Schema.Boolean),
  arm: Schema.optionalKey(Schema.Literals(["old", "new"])),
  networkType: Schema.optionalKey(Schema.Literals(["wifi", "cellular", "other"])),
  networkChanged: Schema.optionalKey(Schema.Boolean),
  interruptedAfterMs: Schema.optionalKey(NullableNumber),
  guess: Schema.optionalKey(Schema.NullOr(Schema.Literals(["old", "new", "skip"]))),
  timingOrigin: Schema.optionalKey(Schema.Literals(["js-launch", "wakeup", "attempt"])),
  timestamp: Schema.String.check(Schema.isPattern(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/)),
  environment: Schema.Number,
  launch: Schema.Literals(["cold", "resume", "running"]),
  wakeup: Schema.Literals([
    "reconnect",
    "application-active",
    "application-active-probe",
    "application-active-reconnect",
    "network-changed",
  ]),
  backgroundMs: NullableNumber,
  route: Schema.NullOr(RouteSchema),
  oldSocket: Schema.Literals(["present", "closed", "absent", "unknown"]),
  continuedFrom: Schema.optionalKey(
    Schema.NullOr(Schema.String.check(Schema.isPattern(/^\d+-\d+$/))),
  ),
  sessionId: Schema.optionalKey(NullableNumber),
  cleanup: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        id: Schema.Number,
        resource: Schema.Literals(["session", "subscription"]),
        method: Schema.NullOr(Schema.Literals(["shell", "thread", "other"])),
        startedAfterMs: NullableNumber,
        durationMs: NullableNumber,
        outcome: Schema.NullOr(Schema.Literals(["completed", "failed", "interrupted"])),
      }),
    ).check(Schema.isMaxLength(DETAIL_LIMIT)),
  ),
  shell: Schema.optionalKey(
    Schema.Struct({
      liveAfterMs: NullableNumber,
      completion: Schema.NullOr(Schema.Literals(["marker", "legacy"])),
      outcome: Schema.Literals(["waiting", "live", "failed", "unknown"]),
    }),
  ),
  probe: Schema.NullOr(
    Schema.Struct({
      result: Schema.Literals(["started", "answered", "failed", "timeout", "interrupted"]),
      durationMs: NullableNumber,
    }),
  ),
  attempts: Schema.Array(
    Schema.Struct({
      id: Schema.optionalKey(NullableNumber),
      startedAfterMs: Schema.optionalKey(NullableNumber),
      stagesMs: Schema.Struct({
        preparing: Schema.Number,
        opening: Schema.Number,
        synchronizing: Schema.Number,
      }),
      route: Schema.NullOr(RouteSchema),
      outcome: Schema.Literals(["pending", "connected", "failed", "interrupted"]),
      error: DiagnosticError,
    }),
  ).check(Schema.isMaxLength(DETAIL_LIMIT)),
  http: Schema.Array(
    Schema.Struct({
      path: Schema.String.check(
        Schema.isPattern(
          /^(?:\/\.well-known\/t3\/environment|\/api\/auth\/(?:session|websocket-ticket)|\/oauth\/token|\/v1\/client\/dpop-token|\/v1\/environments\/:environmentId\/(?:connect|status)|\/:redacted)$/,
        ),
      ),
      durationMs: Schema.Number,
      status: NullableNumber,
      outcome: Schema.Literals(["response", "failed", "interrupted"]),
    }),
  ).check(Schema.isMaxLength(DETAIL_LIMIT)),
  detailsTruncated: Schema.Boolean,
  visibleThread: Schema.Struct({
    mode: Schema.NullOr(Schema.Literals(["replay", "snapshot"])),
    liveAfterMs: NullableNumber,
    completion: Schema.NullOr(Schema.Literals(["marker", "legacy"])),
    bytes: Schema.Null,
    outcome: Schema.Literals(["waiting", "live", "failed", "interrupted", "not-visible"]),
  }),
  durationMs: Schema.Number,
  outcome: Schema.Literals([
    "pending",
    "connected",
    "healthy",
    "failed",
    "interrupted",
    "superseded",
    "process-ended",
    "offline",
    "blocked",
    "idle",
  ]),
  error: DiagnosticError,
});
const decodeStoredRecord = Schema.decodeUnknownOption(StoredRecord);

export function readConnectionLog(raw: string): ReadonlyArray<ConnectionLogRecord> {
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.slice(-CONNECTION_LOG_LIMIT).flatMap((value) => {
      const decoded = decodeStoredRecord(value);
      if (Option.isNone(decoded)) return [];
      const record = decoded.value;
      if (record.recordVersion === 4 && !record.event) return [];
      return [
        {
          ...record,
          recordVersion: record.recordVersion ?? (record.experiment === undefined ? 1 : 2),
          event: record.event ?? null,
          eventId: record.eventId ?? null,
          eventEnvironmentCount: record.eventEnvironmentCount ?? 1,
          eventAssignment: record.eventAssignment ?? "random",
          legacyPacingSupported: record.legacyPacingSupported ?? null,
          experiment: record.experiment ?? false,
          arm: record.arm ?? "new",
          networkType: record.networkType ?? "other",
          networkChanged: record.networkChanged ?? false,
          guess: record.guess ?? null,
          interruptedAfterMs: record.interruptedAfterMs ?? null,
          timingOrigin: record.timingOrigin ?? "attempt",
          continuedFrom: record.continuedFrom ?? null,
          sessionId: record.sessionId ?? null,
          attempts: record.attempts.map((attempt) => ({
            ...attempt,
            id: attempt.id ?? null,
            startedAfterMs: attempt.startedAfterMs ?? null,
          })),
          cleanup: record.cleanup ?? [],
          shell: record.shell ?? {
            liveAfterMs: null,
            completion: null,
            outcome: "unknown" as const,
          },
          outcome: record.outcome === "pending" ? "process-ended" : record.outcome,
        },
      ];
    });
  } catch {
    return [];
  }
}
