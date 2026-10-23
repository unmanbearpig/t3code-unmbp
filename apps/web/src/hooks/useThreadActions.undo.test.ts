import { type Thread } from "../types";
import { updateThreadShortcutTargets } from "../threadShortcutTargets";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { makeThreadFixture } from "../test-fixtures";
import { AsyncResult } from "effect/reactivity";
import { scopedThreadKey } from "@t3tools/client-runtime/environment";
const navigationThreads = vi.hoisted(() => [] as Thread[]);

const newThread = vi.hoisted(() => vi.fn(async () => {}));

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import { useThreadActions } from "./useThreadActions";
import { threadEnvironment } from "../state/threads";
import { toastManager } from "../components/ui/toast";
import { useThreadUndoNotice } from "./showThreadUndoNotice";

const commands = vi.hoisted(() => ({
  pin: vi.fn(),
  unpin: vi.fn(),
  archive: vi.fn(),
  unarchive: vi.fn(),
  settle: vi.fn(),
  unsettle: vi.fn(),
  snooze: vi.fn(),
  unsnooze: vi.fn(),
}));
const router = vi.hoisted(() => ({
  navigate: vi.fn(async () => {}),
  state: { matches: [{ params: {} as Record<string, string> }] },
}));
vi.mock("../state/use-atom-query-runner", () => ({ useAtomQueryRunner: () => vi.fn() }));
vi.mock("../state/session", async (original) => ({
  ...(await original<typeof import("../state/session")>()),
  readEnvironmentScope: () => true,
}));
vi.mock("react", async (original) => ({
  ...(await original<typeof import("react")>()),
  useCallback: (callback: unknown) => callback,
  useMemo: (create: () => unknown) => create(),
  useRef: (value: unknown) => ({ current: value }),
}));
vi.mock("@tanstack/react-router", () => ({ useRouter: () => router }));
vi.mock("./useSettings", () => ({ useClientSettings: () => false }));
vi.mock("./useHandleNewThread", () => ({ useNewThreadHandler: () => newThread }));
vi.mock("../composerDraftStore", () => ({ useComposerDraftStore: () => vi.fn() }));
vi.mock("../terminalUiStateStore", () => ({ useTerminalUiStateStore: () => vi.fn() }));
vi.mock("../uiStateStore", () => ({ useUiStateStore: () => vi.fn() }));
vi.mock("../lib/archivedThreadsState", () => ({ refreshArchivedThreadsForEnvironment: vi.fn() }));
const threadShell = vi.hoisted(() => ({
  title: "Thread",
  pinOrderKey: "a0",
  pinnedAt: null as string | null,
  snoozedUntil: null as string | null,
  projectId: "project",
  environmentId: "undo-env",
  session: null,
}));
vi.mock("../state/entities", async (original) => ({
  ...(await original<typeof import("../state/entities")>()),
  readEnvironmentSupportsPinning: () => true,
  readEnvironmentSupportsPinReorder: () => true,
  readEnvironmentSupportsSettlement: () => true,
  readEnvironmentSupportsSnooze: () => true,
  readThreadShell: (ref: { environmentId: string; threadId: string }) =>
    navigationThreads.length > 0
      ? (navigationThreads.find(
          (thread) => thread.environmentId === ref.environmentId && thread.id === ref.threadId,
        ) ?? null)
      : threadShell,
}));
vi.mock("../state/use-atom-command", () => ({
  useAtomCommand: (command: unknown) => {
    switch (command) {
      case threadEnvironment.pin:
        return commands.pin;
      case threadEnvironment.unpin:
        return commands.unpin;
      case threadEnvironment.archive:
        return commands.archive;
      case threadEnvironment.unarchive:
        return commands.unarchive;
      case threadEnvironment.settle:
        return commands.settle;
      case threadEnvironment.unsettle:
        return commands.unsettle;
      case threadEnvironment.snooze:
        return commands.snooze;
      case threadEnvironment.unsnooze:
        return commands.unsnooze;
      default:
        return vi.fn();
    }
  },
}));

const target = {
  environmentId: EnvironmentId.make("undo-env"),
  threadId: ThreadId.make("thread"),
};
function currentUndo() {
  const notice = useThreadUndoNotice.getState().notice;
  expect(notice).not.toBeNull();
  return notice!.undo;
}

beforeEach(() => {
  vi.useFakeTimers();
  for (const command of Object.values(commands)) {
    command.mockReset().mockResolvedValue({ _tag: "Success", value: undefined });
  }
  router.navigate.mockReset().mockResolvedValue(undefined);
  newThread.mockReset().mockResolvedValue(undefined);
  navigationThreads.length = 0;
  updateThreadShortcutTargets([], null);
  router.state.matches[0]!.params = {};
  threadShell.pinnedAt = null;
  threadShell.snoozedUntil = null;
});

describe("navigation after settlement", () => {
  function ref(thread: Thread) {
    return scopeThreadRef(thread.environmentId, thread.id);
  }
  function open(thread: Thread) {
    router.state.matches[0]!.params = { environmentId: thread.environmentId, threadId: thread.id };
  }
  function recordSettlement() {
    navigationThreads[0] = { ...navigationThreads[0]!, settledOverride: "settled" };
  }

  beforeEach(() => {
    navigationThreads.push(
      ...["current", "below", "last"].map((id) =>
        makeThreadFixture({ id: ThreadId.make(id), environmentId: target.environmentId }),
      ),
    );
    updateThreadShortcutTargets(navigationThreads.map(ref), ref(navigationThreads[0]!));
    open(navigationThreads[0]!);
    commands.settle.mockImplementation(async () => {
      recordSettlement();
      return AsyncResult.success(undefined);
    });
  });

  it("advances using the order before settlement even if the sidebar reorders", async () => {
    commands.settle.mockImplementationOnce(async () => {
      recordSettlement();
      updateThreadShortcutTargets(
        [ref(navigationThreads[2]!), ref(navigationThreads[1]!), ref(navigationThreads[0]!)],
        null,
      );
      return AsyncResult.success(undefined);
    });
    await useThreadActions().settleThread(ref(navigationThreads[0]!));
    expect(router.navigate).toHaveBeenCalledExactlyOnceWith({
      to: "/$environmentId/$threadId",
      params: { environmentId: target.environmentId, threadId: "below" },
    });
  });

  it("skips other threads settling in the same batch", async () => {
    await useThreadActions().settleThread(ref(navigationThreads[0]!), {
      excludedThreadKeys: new Set([scopedThreadKey(ref(navigationThreads[1]!))]),
    });
    expect(router.navigate).toHaveBeenCalledWith(
      expect.objectContaining({
        params: { environmentId: target.environmentId, threadId: "last" },
      }),
    );
  });

  it("opens a draft when no eligible threads remain", async () => {
    navigationThreads.splice(1);
    updateThreadShortcutTargets(navigationThreads.map(ref), ref(navigationThreads[0]!));
    await useThreadActions().settleThread(ref(navigationThreads[0]!));
    expect(newThread).toHaveBeenCalledExactlyOnceWith({
      environmentId: target.environmentId,
      projectId: navigationThreads[0]!.projectId,
    });
    expect(router.navigate).not.toHaveBeenCalled();
  });

  it("does not navigate when settlement fails", async () => {
    commands.settle.mockResolvedValueOnce(
      AsyncResult.failure(Cause.fail(new Error("settlement failed"))),
    );
    const result = await useThreadActions().settleThread(ref(navigationThreads[0]!));
    expect(result._tag).toBe("Failure");
    expect(router.navigate).not.toHaveBeenCalled();
    expect(newThread).not.toHaveBeenCalled();
  });

  it("keeps the user's navigation while settlement is pending", async () => {
    const pending = deferred<ReturnType<typeof AsyncResult.success<undefined>>>();
    commands.settle.mockReturnValueOnce(pending.promise);
    const settling = useThreadActions().settleThread(ref(navigationThreads[0]!));
    open(navigationThreads[2]!);
    recordSettlement();
    pending.resolve(AsyncResult.success(undefined));
    await settling;
    expect(router.navigate).not.toHaveBeenCalled();
    expect(newThread).not.toHaveBeenCalled();
  });

  it("checks candidates again when the next thread starts working during settlement", async () => {
    const pending = deferred<ReturnType<typeof AsyncResult.success<undefined>>>();
    commands.settle.mockReturnValueOnce(pending.promise);
    const settling = useThreadActions().settleThread(ref(navigationThreads[0]!));
    const next = navigationThreads[1]!;
    navigationThreads[1] = {
      ...next,
      runtime: {
        providerInstanceId: next.providerInstanceId,
        providerName: "Codex",
        status: "running",
        activeRunId: null,
        lastError: null,
        updatedAt: next.updatedAt,
      },
    };
    recordSettlement();
    pending.resolve(AsyncResult.success(undefined));
    await settling;
    expect(router.navigate).toHaveBeenCalledWith(
      expect.objectContaining({
        params: { environmentId: target.environmentId, threadId: "last" },
      }),
    );
  });

  it("does not navigate when settling a background thread", async () => {
    open(navigationThreads[1]!);
    await useThreadActions().settleThread(ref(navigationThreads[0]!));
    expect(router.navigate).not.toHaveBeenCalled();
  });

  it("does not navigate if the thread was un-settled before the command returned", async () => {
    commands.settle.mockResolvedValueOnce(AsyncResult.success(undefined));
    await useThreadActions().settleThread(ref(navigationThreads[0]!));
    expect(router.navigate).not.toHaveBeenCalled();
  });

  it("reports failed navigation while retaining successful settlement and Undo", async () => {
    const add = vi.spyOn(toastManager, "add").mockReturnValue("toast");
    router.navigate.mockRejectedValueOnce(new Error("route unavailable"));
    const result = await useThreadActions().settleThread(ref(navigationThreads[0]!));
    expect(result._tag).toBe("Success");
    expect(add).toHaveBeenCalledWith(
      expect.objectContaining({
        title: "Thread settled, but navigation failed",
        description: "route unavailable",
      }),
    );
    expect(useThreadUndoNotice.getState().notice).toMatchObject({ action: "Settled" });
  });
});
afterEach(() => {
  vi.runAllTimers();
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("concurrent thread action Undo", () => {
  it.each([
    ["settleThread", "settle", "unsettle", "Settled"],
    ["snoozeThread", "snooze", "unsnooze", "Snoozed"],
    ["unpinThread", "unpin", "pin", "Unpinned"],
    ["archiveThread", "archive", "unarchive", "Archived"],
  ] as const)(
    "%s retains one undo for concurrent successful requests",
    async (handler, action, inverse, notice) => {
      const actions = useThreadActions();
      const run = () =>
        handler === "snoozeThread"
          ? actions.snoozeThread(target, "2099-01-01T09:00:00.000Z")
          : actions[handler](target);
      await Promise.all([run(), run()]);
      expect(commands[action]).toHaveBeenCalledTimes(2);
      expect(useThreadUndoNotice.getState().notice).toMatchObject({ action: notice, count: 1 });
      await currentUndo()();
      expect(commands[inverse]).toHaveBeenCalledOnce();
    },
  );
});

describe("unpin Undo", () => {
  it("restores the original snooze after restoring the pin", async () => {
    threadShell.pinnedAt = "2026-01-01T00:00:00.000Z";
    threadShell.snoozedUntil = "2030-01-01T09:00:00.000Z";
    await useThreadActions().unpinThread(target);
    await currentUndo()();
    expect(commands.pin).toHaveBeenCalledExactlyOnceWith({
      environmentId: target.environmentId,
      input: { threadId: target.threadId, orderKey: "a0" },
    });
    expect(commands.snooze).toHaveBeenCalledExactlyOnceWith({
      environmentId: target.environmentId,
      input: { threadId: target.threadId, snoozedUntil: threadShell.snoozedUntil },
    });
    expect(commands.pin.mock.invocationCallOrder[0]).toBeLessThan(
      commands.snooze.mock.invocationCallOrder[0]!,
    );
  });

  it("restores the pin without an already expired snooze", async () => {
    threadShell.pinnedAt = "2026-01-01T00:00:00.000Z";
    threadShell.snoozedUntil = "2000-01-01T09:00:00.000Z";
    await useThreadActions().unpinThread(target);
    await currentUndo()();
    expect(commands.pin).toHaveBeenCalledOnce();
    expect(commands.snooze).not.toHaveBeenCalled();
  });

  it("ignores an old notice across hook instances and still restores the latest unpin", async () => {
    const sidebar = useThreadActions();
    const header = useThreadActions();
    await sidebar.unpinThread(target);
    const staleUndo = currentUndo();
    await header.pinThread(target, { orderKey: "a1" });
    await header.unpinThread(target);
    const latestUndo = currentUndo();
    await staleUndo();
    expect(commands.pin).toHaveBeenCalledTimes(1);
    await latestUndo();
    expect(commands.pin).toHaveBeenCalledTimes(2);
    expect(commands.pin).toHaveBeenLastCalledWith({
      environmentId: target.environmentId,
      input: { threadId: target.threadId, orderKey: "a0" },
    });
    await latestUndo();
    expect(commands.pin).toHaveBeenCalledTimes(2);
  });
});

describe("archive Undo", () => {
  it("unarchives and returns to the thread when archiving left it", async () => {
    const add = vi.spyOn(toastManager, "add").mockReturnValue("toast");
    router.state.matches[0]!.params = {
      environmentId: target.environmentId,
      threadId: target.threadId,
    };
    const actions = useThreadActions();
    await actions.archiveThread(target);
    expect(useThreadUndoNotice.getState().notice).toMatchObject({ action: "Archived", count: 1 });
    expect(add).not.toHaveBeenCalled();
    await currentUndo()();
    expect(commands.unarchive).toHaveBeenCalledExactlyOnceWith({
      environmentId: target.environmentId,
      input: { threadId: target.threadId },
    });
    expect(router.navigate).toHaveBeenCalledWith(
      expect.objectContaining({
        to: "/$environmentId/$threadId",
        params: { environmentId: target.environmentId, threadId: target.threadId },
      }),
    );
  });
});

describe("settle and snooze Undo", () => {
  it("un-settles from the notice and expires the Undo after a manual un-settle", async () => {
    const add = vi.spyOn(toastManager, "add").mockReturnValue("toast");
    const actions = useThreadActions();
    await actions.settleThread(target);
    expect(useThreadUndoNotice.getState().notice).toMatchObject({ action: "Settled", count: 1 });
    expect(add).not.toHaveBeenCalled();
    const undo = currentUndo();
    await actions.unsettleThread(target);
    await undo();
    expect(commands.unsettle).toHaveBeenCalledOnce();
  });

  it("re-pins and re-snoozes a thread that settling had cleared", async () => {
    const snoozedUntil = "2030-01-01T09:00:00.000Z";
    threadShell.pinnedAt = "2026-01-01T00:00:00.000Z";
    threadShell.snoozedUntil = snoozedUntil;
    const actions = useThreadActions();
    await actions.settleThread(target);
    await currentUndo()();
    expect(commands.unsettle).toHaveBeenCalledOnce();
    expect(commands.pin).toHaveBeenCalledExactlyOnceWith({
      environmentId: target.environmentId,
      input: { threadId: target.threadId, orderKey: "a0" },
    });
    expect(commands.snooze).toHaveBeenCalledExactlyOnceWith({
      environmentId: target.environmentId,
      input: { threadId: target.threadId, snoozedUntil },
    });
  });

  it("stops settle restoration if restoring the pin fails", async () => {
    threadShell.pinnedAt = "2026-01-01T00:00:00.000Z";
    threadShell.snoozedUntil = "2030-01-01T09:00:00.000Z";
    commands.pin.mockResolvedValue({ _tag: "Failure", cause: Cause.fail(new Error("Pin failed")) });
    vi.spyOn(toastManager, "add").mockReturnValue("toast");
    await useThreadActions().settleThread(target);
    await currentUndo()();
    expect(commands.unsettle).toHaveBeenCalledOnce();
    expect(commands.pin).toHaveBeenCalledOnce();
    expect(commands.snooze).not.toHaveBeenCalled();
  });

  it("expires an older unpin Undo when the thread is settled", async () => {
    const actions = useThreadActions();
    await actions.unpinThread(target);
    const staleUnpinUndo = currentUndo();
    await actions.settleThread(target);
    await staleUnpinUndo();
    expect(commands.pin).not.toHaveBeenCalled();
  });

  // regression: #12972 the sidebar undo notice must reverse a snooze
  it("wakes the thread from the snooze notice", async () => {
    const add = vi.spyOn(toastManager, "add").mockReturnValue("toast");
    const actions = useThreadActions();
    await actions.snoozeThread(target, "2100-01-01T00:00:00.000Z");
    expect(useThreadUndoNotice.getState().notice).toMatchObject({ action: "Snoozed", count: 1 });
    expect(add).not.toHaveBeenCalled();
    await currentUndo()();
    expect(commands.unsnooze).toHaveBeenCalledExactlyOnceWith({
      environmentId: target.environmentId,
      input: { threadId: target.threadId, reason: "user" },
    });
  });
});
