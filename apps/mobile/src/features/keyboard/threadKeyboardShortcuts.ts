import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { THREAD_JUMP_KEYBINDING_COMMANDS } from "@t3tools/contracts";
import { useCallback } from "react";

import { scopedThreadKey } from "../../lib/scopedEntities";
import type { ThreadListV2ListItem } from "../threads/threadListV2";
import {
  useHardwareKeyboardCommand,
  type HardwareKeyboardCommand,
} from "./hardwareKeyboardCommands";

const THREAD_NAVIGATION_COMMANDS = [
  ...THREAD_JUMP_KEYBINDING_COMMANDS,
  "thread.previous",
  "thread.next",
] as const;

type ThreadShortcutListItem = ThreadListV2ListItem | { readonly type: "v2-show-more" };

export function threadJumpIndex(command: HardwareKeyboardCommand) {
  return THREAD_JUMP_KEYBINDING_COMMANDS.findIndex((candidate) => candidate === command);
}

/** Uses the rendered list so filters and shelves keep their order. */
export function threadJumpTarget(
  items: ReadonlyArray<ThreadShortcutListItem>,
  command: HardwareKeyboardCommand,
) {
  let index = threadJumpIndex(command);
  if (index < 0) return null;
  for (const item of items) {
    const thread = item.type === "v2-thread" ? item.item.thread : null;
    if (thread !== null && index-- === 0) return thread;
  }
  return null;
}

export function useThreadJumpShortcuts(
  items: ReadonlyArray<ThreadShortcutListItem>,
  onSelectThread: (thread: EnvironmentThreadShell) => void,
  currentThreadKey: string | null = null,
) {
  const jumpToThread = useCallback(
    (command: HardwareKeyboardCommand) => {
      const threads = items.flatMap((item) =>
        item.type === "v2-thread" ? [item.item.thread] : [],
      );
      const currentIndex = threads.findIndex(
        (thread) => scopedThreadKey(thread.environmentId, thread.id) === currentThreadKey,
      );
      const thread =
        command === "thread.next" || command === "thread.previous"
          ? (threads[
              currentIndex < 0
                ? command === "thread.next"
                  ? 0
                  : threads.length - 1
                : currentIndex + (command === "thread.next" ? 1 : -1)
            ] ?? null)
          : threadJumpTarget(items, command);
      if (thread !== null) onSelectThread(thread);
      return true;
    },
    [items, onSelectThread, currentThreadKey],
  );
  useHardwareKeyboardCommand(THREAD_NAVIGATION_COMMANDS, jumpToThread);
}
