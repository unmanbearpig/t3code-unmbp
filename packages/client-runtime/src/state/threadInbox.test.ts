import { EnvironmentId, ProviderInstanceId, RunId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  createInboxReturnTracker,
  findNextActionableThread,
  isThreadWorking,
  sortWorkingThreadsBySend,
} from "./threadInbox.ts";
import { scopedThreadKey, scopeThreadRef } from "../environment/scoped.ts";
import { presentThreadShell, type EnvironmentThreadShell } from "./models.ts";
import { v2ThreadShell } from "./orchestrationV2TestFixtures.ts";

const environmentId = EnvironmentId.make("environment-1");

describe("findNextActionableThread", () => {
  function shell(id: string, overrides: Partial<EnvironmentThreadShell> = {}) {
    return {
      ...presentThreadShell(environmentId, { ...v2ThreadShell, id: ThreadId.make(id) }),
      ...overrides,
    };
  }
  function find(
    threads: EnvironmentThreadShell[],
    current = threads[0]!,
    excludedThreadKeys?: ReadonlySet<string>,
  ) {
    const refs = threads.map((thread) => scopeThreadRef(thread.environmentId, thread.id));
    const byKey = new Map(
      threads.map((thread) => [
        scopedThreadKey(scopeThreadRef(thread.environmentId, thread.id)),
        thread,
      ]),
    );
    return findNextActionableThread({
      threadRef: scopeThreadRef(current.environmentId, current.id),
      orderedThreadRefs: refs,
      readThread: (ref) => byKey.get(scopedThreadKey(ref)) ?? null,
      excludedThreadKeys,
      now: "2026-06-20T01:00:00.000Z",
    });
  }

  it.each(["preparing", "queued", "starting", "running", "waiting", "idle"] as const)(
    "skips %s work, including pinned threads",
    (status) => {
      const busy = shell("busy", {
        pinnedAt: "2026-06-20T00:00:00.000Z",
        runtime: { ...thread("busy", true).runtime!, status },
      });
      const ready = shell("ready");
      expect(find([shell("current"), busy, ready])).toBe(ready);
    },
  );

  it.each(["hasPendingApprovals", "hasPendingUserInput"] as const)(
    "visits a thread with %s even while its run is active",
    (field) => {
      const needsUser = shell("needs-user", {
        runtime: thread("needs-user", true).runtime,
        [field]: true,
      });
      expect(find([shell("current"), needsUser, shell("ready")])).toBe(needsUser);
    },
  );

  it("skips parked, archived, deleted, and co-settling threads", () => {
    const ready = shell("ready");
    expect(
      find(
        [
          shell("current"),
          shell("settled", { settledOverride: "settled" }),
          shell("settle-later", { settleWhenIdleAt: "2026-06-20T00:00:00.000Z" }),
          shell("snoozed", { snoozedUntil: "2099-01-01T00:00:00.000Z" }),
          shell("archived", { archivedAt: "2026-06-20T00:00:00.000Z" }),
          shell("deleted", { deletedAt: "2026-06-20T00:00:00.000Z" }),
          shell("co-settling"),
          ready,
        ],
        undefined,
        new Set([`${environmentId}:co-settling`]),
      ),
    ).toBe(ready);
  });

  it("wraps to the first eligible thread above the current one", () => {
    const ready = shell("ready");
    const current = shell("current");
    expect(find([ready, current, shell("settled", { settledOverride: "settled" })], current)).toBe(
      ready,
    );
  });

  it("skips disconnected threads when choosing the next actionable thread", () => {
    const ready = shell("ready");
    const offline = shell("offline", { environmentConnected: false });
    expect(find([shell("current"), offline, ready])).toBe(ready);
    expect(isThreadWorking({ ...offline, runtime: thread("offline", true).runtime })).toBe(false);
  });

  it("returns no target when there are no other eligible threads or the current row is missing", () => {
    const current = shell("current");
    expect(find([current])).toBeNull();
    expect(find([current, shell("settled", { settledOverride: "settled" })])).toBeNull();
    expect(find([shell("ready")], current)).toBeNull();
  });

  it("distinguishes matching thread ids in different environments", () => {
    const current = shell("same-id");
    const remote = shell("same-id", { environmentId: EnvironmentId.make("remote") });
    expect(find([current, remote])).toBe(remote);
  });
});

function thread(id: string, working: boolean) {
  return {
    id: ThreadId.make(id),
    environmentId,
    createdAt: "2026-06-01T00:00:00.000Z",
    unsettledAt: null,
    latestRun: null,
    hasActionableProposedPlan: false,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    interactionMode: "default" as const,
    runtime: working
      ? {
          status: "running" as const,
          activeRunId: null,
          providerInstanceId: ProviderInstanceId.make("codex"),
          providerName: "Codex",
          lastError: null,
          updatedAt: "2026-06-01T00:00:00.000Z",
        }
      : null,
  };
}

describe("createInboxReturnTracker", () => {
  it("waits for a connected result before treating an offline thread as finished", () => {
    const tracker = createInboxReturnTracker();
    const running = thread("a", true);
    tracker.observe([running]);
    tracker.observe([{ ...running, environmentConnected: false }]);
    tracker.observe([{ ...thread("a", false), environmentConnected: false }]);
    expect(tracker.returnedAt(running)).toBeUndefined();

    tracker.observe([{ ...thread("a", false), environmentConnected: true }]);
    expect(tracker.returnedAt(running)).toBeDefined();
  });

  it("stamps a thread when it stops working, but never on the first observation", () => {
    const tracker = createInboxReturnTracker();
    tracker.observe([thread("a", true), thread("b", false)]);
    expect(tracker.returnedAt(thread("a", true))).toBeUndefined();
    expect(tracker.returnedAt(thread("b", false))).toBeUndefined();

    tracker.observe([thread("a", false), thread("b", false)]);
    expect(tracker.returnedAt(thread("a", false))).toBeDefined();
    expect(tracker.returnedAt(thread("b", false))).toBeUndefined();
  });
});

describe("sortWorkingThreadsBySend", () => {
  it("orders by the last message the user sent, not by later runs", () => {
    const sentFirst = {
      ...thread("sent-first", true),
      latestUserAuthoredMessageAt: "2026-06-01T01:00:00.000Z",
      latestRun: {
        runId: RunId.make("run:wake"),
        status: "running" as const,
        requestedAt: "2026-06-01T04:00:00.000Z",
        startedAt: "2026-06-01T04:00:00.000Z",
        completedAt: null,
        assistantMessageId: null,
      },
    };
    const sentLast = {
      ...thread("sent-last", true),
      latestUserAuthoredMessageAt: "2026-06-01T02:00:00.000Z",
    };
    const launched = { ...thread("launched", true), latestUserAuthoredMessageAt: null };
    expect(
      sortWorkingThreadsBySend([launched, sentFirst, sentLast]).map((thread) => thread.id),
    ).toEqual(["sent-last", "sent-first", "launched"]);
  });
});
