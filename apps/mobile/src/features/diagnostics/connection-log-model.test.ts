import { describe, expect, it } from "@effect/vitest";
import {
  appendConnectionLog,
  createConnectionLog,
  formatConnectionLog,
  readConnectionLog,
  type ConnectionLogRecord,
} from "./connection-log-model";

function harness() {
  let time = 0;
  let records: ReadonlyArray<ConnectionLogRecord> = [];
  const log = createConnectionLog(
    () => ({ wall: 1_800_000_000_000 + time, monotonic: time }),
    (record) => {
      records = appendConnectionLog(records, record);
    },
  );
  return {
    log,
    records: () => records,
    latest: () => records.at(-1)!,
    at: (value: number) => {
      time = value;
    },
  };
}
const environmentId = "private-environment-id";
const threadId = "private-thread-id";
const route = {
  targetKind: "BearerConnectionTarget",
  hostType: "ts.net",
  scheme: "https",
} as const;

describe("connection log assembly", () => {
  it("classifies a fresh process, measures each stage and waits for the visible thread", () => {
    const h = harness();
    h.log.visible({ environmentId, threadId });
    h.log.record({ kind: "attempt-start", environmentId });
    h.log.record({ kind: "stage", environmentId, stage: "preparing" });
    h.at(80);
    h.log.record({ kind: "stage", environmentId, stage: "opening", route });
    h.at(100);
    h.log.record({ kind: "stage", environmentId, stage: "synchronizing" });
    h.at(120);
    h.log.record({ kind: "attempt-end", environmentId, outcome: "connected", error: null });
    h.at(140);
    h.log.record({
      kind: "thread-sync",
      environmentId,
      threadId: "other",
      mode: "snapshot",
      completion: "marker",
      milestone: "live",
    });
    expect(h.latest().visibleThread.outcome).toBe("waiting");
    h.at(160);
    h.log.record({
      kind: "thread-sync",
      environmentId,
      threadId,
      mode: "snapshot",
      completion: "marker",
      milestone: "live",
    });
    expect(h.latest()).toMatchObject({
      launch: "cold",
      backgroundMs: null,
      route,
      outcome: "connected",
      durationMs: 160,
      attempts: [{ stagesMs: { preparing: 80, opening: 20, synchronizing: 20 } }],
      visibleThread: { mode: "snapshot", liveAfterMs: 160, bytes: null, outcome: "live" },
    });
    expect(
      formatConnectionLog(h.records(), { version: "1", build: "2", platform: "ios" }),
    ).not.toMatch(/private-environment|private-thread/);
  });

  it("keeps a failed resume probe and its reconnect in one record", () => {
    const h = harness();
    h.log.record({ kind: "attempt-start", environmentId });
    h.log.record({ kind: "attempt-end", environmentId, outcome: "connected", error: null });
    h.at(5000);
    h.log.wakeup({
      launch: "resume",
      wakeup: "application-active-probe",
      backgroundMs: 4900,
      at: 1_800_000_005_000,
      monotonic: 5000,
    });
    h.log.record({
      kind: "wakeup",
      environmentId,
      reason: "application-active-probe",
      hasSession: true,
      desired: true,
    });
    h.log.record({ kind: "probe", environmentId, result: "started", route });
    h.at(8000);
    h.log.record({ kind: "probe", environmentId, result: "timeout" });
    h.log.record({ kind: "attempt-start", environmentId });
    h.log.record({ kind: "stage", environmentId, stage: "preparing" });
    h.at(8100);
    h.log.record({ kind: "attempt-end", environmentId, outcome: "connected", error: null });
    expect(h.records()).toHaveLength(2);
    expect(h.latest()).toMatchObject({
      launch: "resume",
      backgroundMs: 4900,
      oldSocket: "present",
      probe: { result: "timeout", durationMs: 3000 },
      durationMs: 3100,
      attempts: [{ stagesMs: { preparing: 100 } }],
    });
  });

  it("records a healthy network-change probe without a reconnect and observes an already closed socket", () => {
    const h = harness();
    h.log.wakeup({
      launch: "running",
      wakeup: "network-changed",
      backgroundMs: null,
      at: 1_800_000_000_000,
      monotonic: 0,
    });
    h.log.record({
      kind: "wakeup",
      environmentId,
      reason: "network-changed",
      hasSession: true,
      desired: true,
    });
    h.log.record({ kind: "probe", environmentId, result: "started", route });
    h.at(10);
    h.log.record({ kind: "probe", environmentId, result: "answered" });
    expect(h.latest()).toMatchObject({
      outcome: "healthy",
      route,
      attempts: [],
      probe: { result: "answered", durationMs: 10 },
    });
    h.log.record({ kind: "session-closed", environmentId });
    h.at(60_000);
    h.log.wakeup({
      launch: "resume",
      wakeup: "application-active-reconnect",
      backgroundMs: 59_990,
      at: 1_800_000_060_000,
      monotonic: 60_000,
    });
    h.log.record({
      kind: "wakeup",
      environmentId,
      reason: "application-active-reconnect",
      hasSession: true,
      desired: true,
    });
    expect(h.latest()).toMatchObject({ launch: "resume", oldSocket: "closed", probe: null });
  });

  it("totals repeated route stages, stores safe HTTP metadata and reports terminal errors", () => {
    const h = harness();
    h.log.record({ kind: "attempt-start", environmentId });
    h.log.record({ kind: "stage", environmentId, stage: "preparing" });
    h.at(10);
    h.log.record({ kind: "stage", environmentId, stage: "opening", route });
    h.at(30);
    h.log.record({ kind: "stage", environmentId, stage: "preparing" });
    h.at(60);
    h.log.record({ kind: "stage", environmentId, stage: "opening", route });
    h.log.record({
      kind: "http",
      environmentId,
      path: "/api/auth/websocket-ticket",
      durationMs: 25,
      status: 401,
      outcome: "response",
    });
    h.at(80);
    h.log.record({ kind: "attempt-end", environmentId, outcome: "interrupted", error: null });
    h.log.record({ kind: "state", environmentId, phase: "backoff", error: "timeout" });
    expect(h.latest()).toMatchObject({
      outcome: "failed",
      error: "timeout",
      attempts: [{ stagesMs: { preparing: 40, opening: 40, synchronizing: 0 } }],
      http: [{ path: "/api/auth/websocket-ticket", durationMs: 25, status: 401 }],
    });
  });

  it("bounds storage, updates a record in place, caps detail and keeps clear from resurrecting records", () => {
    const h = harness();
    for (let i = 0; i < 205; i++) {
      h.at(i);
      h.log.record({ kind: "attempt-start", environmentId });
      h.log.record({ kind: "attempt-end", environmentId, outcome: "failed", error: "network" });
    }
    expect(h.records()).toHaveLength(200);
    expect(h.records()[0]?.id).toBe("1800000000005-6");
    for (let i = 0; i < 45; i++)
      h.log.record({
        kind: "http",
        environmentId,
        path: "/:redacted",
        durationMs: 1,
        status: null,
        outcome: "failed",
      });
    expect(h.records()).toHaveLength(200);
    expect(h.latest().http).toHaveLength(40);
    expect(h.latest().detailsTruncated).toBe(true);
    h.log.clear();
    const before = h.records();
    h.log.record({
      kind: "thread-sync",
      environmentId,
      threadId,
      mode: "replay",
      completion: "legacy",
      milestone: "live",
    });
    expect(h.records()).toBe(before);
  });
});

it("restores only bounded, validated, redacted records and marks unfinished processes", () => {
  const h = harness();
  h.log.record({ kind: "attempt-start", environmentId });
  const record = { ...h.latest(), token: "secret", fullUrl: "https://secret?token=secret" };
  const restored = readConnectionLog(JSON.stringify(Array.from({ length: 205 }, () => record)));
  expect(restored).toHaveLength(200);
  expect(restored[0]?.outcome).toBe("process-ended");
  expect(JSON.stringify(restored)).not.toMatch(/secret|fullUrl|token/);
  expect(
    readConnectionLog(
      JSON.stringify([{ ...record, http: [{ path: "https://host?token=secret" }] }]),
    ),
  ).toEqual([]);
  expect(readConnectionLog("broken")).toEqual([]);
});

it("keeps resume metadata when a network change arrives before the supervisor consumes the wakeup", () => {
  const h = harness();
  h.log.wakeup({
    launch: "resume",
    wakeup: "application-active-reconnect",
    backgroundMs: 300000,
    at: 1_800_000_000_000,
    monotonic: 0,
  });
  h.log.wakeup({
    launch: "running",
    wakeup: "network-changed",
    backgroundMs: null,
    at: 1_800_000_000_001,
    monotonic: 1,
  });
  h.log.record({
    kind: "wakeup",
    environmentId,
    reason: "application-active-reconnect",
    hasSession: true,
    desired: true,
  });
  expect(h.latest()).toMatchObject({ launch: "resume", backgroundMs: 300000 });
});

it("attaches a cold-launch thread that becomes live before its screen gains focus", () => {
  const h = harness();
  h.log.record({ kind: "attempt-start", environmentId });
  h.at(10);
  h.log.record({
    kind: "thread-sync",
    environmentId,
    threadId,
    mode: "snapshot",
    completion: "marker",
    milestone: "live",
  });
  h.at(20);
  h.log.visible({ environmentId, threadId });
  expect(h.latest().visibleThread).toMatchObject({
    outcome: "live",
    liveAfterMs: 10,
    mode: "snapshot",
  });
});

it("finishes idle wakeups without claiming a reconnect and preserves a completed connection on later loss", () => {
  const h = harness();
  h.log.record({
    kind: "wakeup",
    environmentId,
    reason: "application-active-probe",
    hasSession: false,
    desired: false,
  });
  expect(h.latest().outcome).toBe("idle");
  h.log.record({ kind: "attempt-start", environmentId });
  h.log.record({ kind: "attempt-end", environmentId, outcome: "connected", error: null });
  h.log.record({ kind: "session-closed", environmentId });
  h.log.record({ kind: "state", environmentId, phase: "backoff", error: "network" });
  expect(h.latest()).toMatchObject({ outcome: "connected", error: null });
});
