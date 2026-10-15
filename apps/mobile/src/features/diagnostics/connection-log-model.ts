import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type { ConnectionDiagnostics } from "@t3tools/client-runtime/connection";

type Event = ConnectionDiagnostics.ConnectionDiagnosticEvent;
type Route = ConnectionDiagnostics.DiagnosticRoute;

export const CONNECTION_LOG_LIMIT = 200;
const DETAIL_LIMIT = 40;

export interface ConnectionLogRecord {
  readonly id: string;
  readonly timestamp: string;
  readonly environment: number;
  readonly launch: "cold" | "resume" | "running";
  readonly wakeup: string;
  readonly backgroundMs: number | null;
  readonly route: Route | null;
  readonly oldSocket: "present" | "closed" | "absent" | "unknown";
  readonly probe: { readonly result: string; readonly durationMs: number | null } | null;
  readonly attempts: ReadonlyArray<{
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
  readonly visibleThread: {
    readonly mode: "replay" | "snapshot" | null;
    readonly liveAfterMs: number | null;
    readonly completion: "marker" | "legacy" | null;
    readonly bytes: null;
    readonly outcome: "waiting" | "live" | "failed" | "not-visible";
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
) {
  let sequence = 0;
  const contexts = new Map<string, MobileWakeupContext>();
  let visible: { environmentId: string; threadId: string } | null = null;
  const seen = new Map<string, number>();
  let environmentSequence = 0;
  const active = new Map<
    string,
    {
      record: ConnectionLogRecord;
      started: number;
      probeStarted: number | null;
      stage: "preparing" | "opening" | "synchronizing" | null;
      stageStarted: number;
      wakeupAt: number | null;
    }
  >();
  const closed = new Set<string>();
  const recentSync = new Map<
    string,
    { event: Extract<Event, { kind: "thread-sync" }>; at: number }
  >();
  const publish = (entry: ReturnType<typeof begin>) => {
    entry.record = { ...entry.record, durationMs: Math.max(0, now().monotonic - entry.started) };
    changed(entry.record);
  };
  const begin = (environmentId: string, wakeup: string, hasSession?: boolean) => {
    const time = now();
    const fromWakeup = contexts.get(wakeup) ?? null;
    const entry = {
      record: {
        id: `${time.wall}-${++sequence}`,
        timestamp: new Date(fromWakeup ? fromWakeup.at : time.wall).toISOString(),
        environment: seen.get(environmentId) ?? ++environmentSequence,
        launch:
          fromWakeup?.launch === "resume" ? "resume" : seen.has(environmentId) ? "running" : "cold",
        wakeup,
        backgroundMs: fromWakeup ? fromWakeup.backgroundMs : null,
        route: null,
        oldSocket: closed.has(environmentId)
          ? "closed"
          : hasSession === true
            ? "present"
            : hasSession === false
              ? "absent"
              : seen.has(environmentId)
                ? "unknown"
                : "absent",
        probe: null,
        attempts: [],
        http: [],
        detailsTruncated: false,
        visibleThread: {
          mode: null,
          liveAfterMs: null,
          completion: null,
          bytes: null,
          outcome: visible?.environmentId === environmentId ? "waiting" : "not-visible",
        },
        durationMs: 0,
        outcome: "pending",
        error: null,
      } as ConnectionLogRecord,
      started: fromWakeup ? fromWakeup.monotonic : time.monotonic,
      probeStarted: null as number | null,
      stage: null as "preparing" | "opening" | "synchronizing" | null,
      stageStarted: time.monotonic,
      wakeupAt: fromWakeup ? fromWakeup.at : null,
    };
    const previous = active.get(environmentId);
    if (previous?.record.outcome === "pending") {
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
    clear: () => {
      active.clear();
      closed.clear();
      contexts.clear();
      recentSync.clear();
    },
    wakeup: (next: MobileWakeupContext) => {
      contexts.set(next.wakeup, next);
    },
    visible: (next: typeof visible) => {
      visible = next;
      const entry = next === null ? undefined : active.get(next.environmentId);
      if (entry && entry.record.visibleThread.outcome === "not-visible") {
        const recent =
          next === null ? undefined : recentSync.get(`${next.environmentId}:${next.threadId}`);
        entry.record = {
          ...entry.record,
          visibleThread:
            recent && recent.at >= entry.started
              ? {
                  mode: recent.event.mode,
                  completion: recent.event.completion,
                  bytes: null,
                  liveAfterMs: recent.event.milestone === "live" ? recent.at - entry.started : null,
                  outcome:
                    recent.event.milestone === "started" ? "waiting" : recent.event.milestone,
                }
              : { ...entry.record.visibleThread, outcome: "waiting" },
        };
        publish(entry);
      }
    },
    record: (event: Event) => {
      let entry = active.get(event.environmentId);
      if (event.kind === "thread-sync") {
        const key = `${event.environmentId}:${event.threadId}`;
        recentSync.delete(key);
        recentSync.set(key, { event, at: now().monotonic });
        if (recentSync.size > CONNECTION_LOG_LIMIT)
          recentSync.delete(recentSync.keys().next().value!);
      }
      if (event.kind === "session-closed") {
        closed.add(event.environmentId);
        return;
      }
      if (event.kind === "wakeup") {
        if (event.reason === "credentials-changed") return;
        if (
          entry?.wakeupAt !== null &&
          entry?.wakeupAt === contexts.get(event.reason)?.at &&
          entry?.record.wakeup === event.reason
        )
          return;
        entry = begin(event.environmentId, event.reason, event.hasSession);
        if (!event.desired) {
          entry.record = { ...entry.record, outcome: "idle" };
          publish(entry);
        }
        return;
      }
      if (event.kind === "attempt-start") {
        if (!entry || entry.record.outcome !== "pending")
          entry = begin(event.environmentId, "reconnect");
        if (entry.record.attempts.length >= DETAIL_LIMIT)
          entry.record = { ...entry.record, detailsTruncated: true };
        entry.record = {
          ...entry.record,
          attempts: [
            ...entry.record.attempts.slice(-(DETAIL_LIMIT - 1)),
            {
              stagesMs: { preparing: 0, opening: 0, synchronizing: 0 },
              route: null,
              outcome: "pending",
              error: null,
            },
          ],
        };
        entry.stage = null;
        closed.delete(event.environmentId);
      }
      if (!entry) return;
      const time = now().monotonic;
      switch (event.kind) {
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
            attempts: attempt
              ? [
                  ...entry.record.attempts.slice(0, -1),
                  { ...attempt, outcome: event.outcome, error: event.error },
                ]
              : entry.record.attempts,
          };
          break;
        }
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
          if (visible?.environmentId !== event.environmentId || visible.threadId !== event.threadId)
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
): string {
  return JSON.stringify({ format: "t3-mobile-connection-log", version: 1, app, records }, null, 2);
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
  id: Schema.String.check(Schema.isPattern(/^\d+-\d+$/)),
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
  probe: Schema.NullOr(
    Schema.Struct({
      result: Schema.Literals(["started", "answered", "failed", "timeout", "interrupted"]),
      durationMs: NullableNumber,
    }),
  ),
  attempts: Schema.Array(
    Schema.Struct({
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
    outcome: Schema.Literals(["waiting", "live", "failed", "not-visible"]),
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
      return [
        { ...record, outcome: record.outcome === "pending" ? "process-ended" : record.outcome },
      ];
    });
  } catch {
    return [];
  }
}
