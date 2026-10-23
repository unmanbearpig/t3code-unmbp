import { threadRuntimeIsActive, type EnvironmentThreadShell } from "./models.ts";
import type { ScopedThreadRef } from "@t3tools/contracts";
import { scopedThreadKey } from "../environment/scoped.ts";
import { effectiveSnoozed, isFiledAsSettled } from "./threadSettled.ts";
import { toSortableTimestamp } from "./threadSort.ts";

type WorkingThreadInput = Pick<
  EnvironmentThreadShell,
  | "hasActionableProposedPlan"
  | "hasPendingApprovals"
  | "hasPendingUserInput"
  | "pendingSubagentRequest"
  | "interactionMode"
  | "latestRun"
  | "runtime"
  | "environmentConnected"
>;

export function isThreadWorking(thread: WorkingThreadInput): boolean {
  if (thread.environmentConnected === false) return false;
  if (thread.hasPendingApprovals || thread.hasPendingUserInput) return false;
  if (thread.pendingSubagentRequest != null) return false;
  if (!threadRuntimeIsActive(thread.runtime) && thread.runtime?.status !== "idle") return false;
  const run = thread.latestRun;
  const runSettled =
    run !== null &&
    run.status !== "preparing" &&
    run.status !== "queued" &&
    run.status !== "starting" &&
    run.status !== "running" &&
    run.status !== "waiting" &&
    thread.runtime?.activeRunId !== run.runId;
  return !(thread.interactionMode === "plan" && thread.hasActionableProposedPlan && runSettled);
}

/** Follow the order captured before settling, checking each candidate's current state. */
export function findNextActionableThread(input: {
  readonly threadRef: ScopedThreadRef;
  readonly orderedThreadRefs: readonly ScopedThreadRef[];
  readonly readThread: (ref: ScopedThreadRef) => EnvironmentThreadShell | null;
  readonly excludedThreadKeys?: ReadonlySet<string> | undefined;
  readonly now: string;
}): EnvironmentThreadShell | null {
  const currentKey = scopedThreadKey(input.threadRef);
  const index = input.orderedThreadRefs.findIndex((ref) => scopedThreadKey(ref) === currentKey);
  if (index === -1) return null;
  for (let offset = 1; offset < input.orderedThreadRefs.length; offset += 1) {
    const ref = input.orderedThreadRefs[(index + offset) % input.orderedThreadRefs.length]!;
    if (scopedThreadKey(ref) === currentKey || input.excludedThreadKeys?.has(scopedThreadKey(ref)))
      continue;
    const thread = input.readThread(ref);
    if (
      thread !== null &&
      thread.environmentConnected !== false &&
      thread.archivedAt === null &&
      thread.deletedAt === null &&
      !isFiledAsSettled(thread) &&
      !effectiveSnoozed(thread, { now: input.now }) &&
      !isThreadWorking(thread)
    ) {
      return thread;
    }
  }
  return null;
}

type InboxThreadInput = Pick<
  EnvironmentThreadShell,
  "id" | "environmentId" | "createdAt" | "unsettledAt" | "latestRun"
>;

export function sortInboxThreadsByReturn<T extends InboxThreadInput>(
  threads: readonly T[],
  observedReturnAt?: (thread: T) => number | undefined,
): T[] {
  const timestamps = new Map(
    threads.map((thread) => [
      thread,
      Math.max(
        toSortableTimestamp(thread.createdAt) ?? 0,
        toSortableTimestamp(thread.unsettledAt ?? undefined) ?? 0,
        toSortableTimestamp(thread.latestRun?.requestedAt ?? undefined) ?? 0,
        toSortableTimestamp(thread.latestRun?.completedAt ?? undefined) ?? 0,
        observedReturnAt?.(thread) ?? 0,
      ),
    ]),
  );
  return sortNewestFirst(threads, timestamps);
}

type WorkingSortInput = Pick<
  EnvironmentThreadShell,
  "id" | "environmentId" | "createdAt" | "latestRun" | "latestUserAuthoredMessageAt"
>;

export function sortWorkingThreadsBySend<T extends WorkingSortInput>(threads: readonly T[]): T[] {
  const timestamps = new Map(
    threads.map((thread) => [
      thread,
      Math.max(
        toSortableTimestamp(thread.createdAt) ?? 0,
        toSortableTimestamp(
          (thread.latestUserAuthoredMessageAt === undefined
            ? thread.latestRun?.requestedAt
            : thread.latestUserAuthoredMessageAt) ?? undefined,
        ) ?? 0,
      ),
    ]),
  );
  return sortNewestFirst(threads, timestamps);
}

function sortNewestFirst<T extends Pick<EnvironmentThreadShell, "id" | "environmentId">>(
  threads: readonly T[],
  timestamps: ReadonlyMap<T, number>,
): T[] {
  return [...threads].sort(
    (left, right) =>
      timestamps.get(right)! - timestamps.get(left)! ||
      left.id.localeCompare(right.id) ||
      left.environmentId.localeCompare(right.environmentId),
  );
}

/**
 * Keep one at module scope so the inbox order survives routes that unmount the
 * list. Call `observe` with every thread shell on each list rebuild, or with
 * null to reset while the beta is off.
 */
export function createInboxReturnTracker() {
  const keyOf = (thread: Pick<EnvironmentThreadShell, "environmentId" | "id">) =>
    `${thread.environmentId}:${thread.id}`;
  let lastWorkingKeys: ReadonlySet<string> | null = null;
  const returns = new Map<string, number>();
  return {
    observe(threads: ReadonlyArray<WorkingThreadInput & InboxThreadInput> | null): void {
      if (threads === null) {
        lastWorkingKeys = null;
        returns.clear();
        return;
      }
      const working = new Set<string>();
      const present = new Set<string>();
      for (const thread of threads) {
        const key = keyOf(thread);
        present.add(key);
        // A disconnect cannot confirm that work finished. Keep its last known
        // membership until a connected snapshot can report the result.
        if (
          isThreadWorking(thread) ||
          (thread.environmentConnected === false && lastWorkingKeys?.has(key))
        ) {
          working.add(key);
        }
      }
      for (const key of returns.keys()) {
        if (!present.has(key)) returns.delete(key);
      }
      // @effect-diagnostics-next-line globalDate:off
      const at = Date.now();
      for (const key of lastWorkingKeys ?? []) {
        if (present.has(key) && !working.has(key)) returns.set(key, at);
      }
      lastWorkingKeys = working;
    },
    returnedAt: (thread: Pick<EnvironmentThreadShell, "environmentId" | "id">) =>
      returns.get(keyOf(thread)),
  };
}
