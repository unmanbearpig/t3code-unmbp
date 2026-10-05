import { useEffect, useEffectEvent, useState } from "react";
import { useNavigate, useParams } from "@tanstack/react-router";
import { useAtomValue } from "@effect/atom-react";
import {
  createLeaderKeyController,
  LEADER_TIMEOUT_MS,
  matchesLeaderStroke,
} from "@t3tools/shared/leaderKey";
import { scopedThreadKey } from "@t3tools/client-runtime/environment";
import {
  dispatchLeaderShortcut,
  installShortcutPreprocessor,
  registerLeaderShortcutHandler,
} from "~/appShortcutEvents";
import { useClientSettings } from "~/hooks/useSettings";
import {
  resolveShortcutCommand,
  threadJumpIndexFromCommand,
  threadTraversalDirectionFromCommand,
} from "~/keybindings";
import {
  getLeaderTrigger,
  setLeaderActive,
  setLeaderTrigger,
  useLeaderActive,
} from "~/leaderState";
import { primaryServerKeybindingsAtom } from "~/state/server";
import { readThreadShell } from "~/state/entities";
import { readThreadShortcutTargets } from "~/threadShortcutTargets";
import { resolveThreadRouteRef } from "~/threadRoutes";
import { isCommandPaletteOpen } from "~/commandPaletteBus";
import { isModelPickerOpen } from "~/modelPickerVisibility";
import { isTerminalFocused } from "~/lib/terminalFocus";
import { isPreviewFocused } from "~/lib/previewFocus";
import { isMacPlatform } from "~/lib/utils";
import { selectThreadTerminalUiState, useTerminalUiStateStore } from "~/terminalUiStateStore";
import { selectActiveRightPanel, useRightPanelStore } from "~/rightPanelStore";
import { resolveAdjacentThreadId } from "./Sidebar.logic";

export function LeaderKeyCoordinator() {
  const settings = useClientSettings();
  const keybindings = useAtomValue(primaryServerKeybindingsAtom);
  const navigate = useNavigate();
  const routeThreadRef = useParams({ strict: false, select: resolveThreadRouteRef });
  const active = useLeaderActive();
  const [controller] = useState(createLeaderKeyController);
  const terminalOpen = useTerminalUiStateStore((state) =>
    routeThreadRef
      ? selectThreadTerminalUiState(state.terminalUiStateByThreadKey, routeThreadRef).terminalOpen
      : false,
  );
  const previewOpen = useRightPanelStore((state) =>
    routeThreadRef
      ? selectActiveRightPanel(state.byThreadKey, routeThreadRef) === "preview"
      : false,
  );
  const resolveUnhandledStroke = useEffectEvent((event: KeyboardEvent) =>
    resolveShortcutCommand(
      {
        key: event.key,
        code: event.code,
        ctrlKey: event.ctrlKey,
        metaKey: event.metaKey,
        altKey: event.altKey,
        shiftKey: event.shiftKey,
        leaderKey: true,
        target: event.target,
      },
      keybindings,
      {
        context: {
          terminalFocus: isTerminalFocused(),
          terminalOpen,
          previewFocus: isPreviewFocused(),
          previewOpen,
          modelPickerOpen: isModelPickerOpen(),
        },
      },
    ),
  );

  useEffect(() => {
    setLeaderTrigger(settings.leaderShortcut);
    let timeout: ReturnType<typeof setTimeout> | undefined;
    const reset = () => {
      controller.cancel();
      clearTimeout(timeout);
      setLeaderActive(false);
    };
    const processed = new WeakMap<KeyboardEvent, boolean>();
    const processKey = (event: KeyboardEvent): boolean => {
      const previous = processed.get(event);
      if (previous !== undefined) return previous;
      processed.set(event, false);
      if (event.target instanceof Element && event.target.closest("[data-keybinding-capture]")) {
        reset();
        return false;
      }
      const trigger = getLeaderTrigger();
      const action = controller.handle(
        {
          ...event,
          type: event.type === "keyup" ? "keyup" : "keydown",
          key: event.key,
          code: event.code,
          repeat: event.repeat,
          isComposing: event.isComposing,
        },
        trigger !== null && matchesLeaderStroke(event, trigger, isMacPlatform(navigator.platform)),
        Date.now(),
      );
      setLeaderActive(controller.active(Date.now()));
      if (action === "pass") return false;
      if (action === "prefix") {
        clearTimeout(timeout);
        timeout = setTimeout(() => {
          controller.cancel();
          setLeaderActive(false);
        }, LEADER_TIMEOUT_MS);
      }
      if (action === "stroke" && !dispatchLeaderShortcut(event)) {
        const command = resolveUnhandledStroke(event);
        if (command === null) {
          controller.release({ type: "keydown", key: event.key, code: event.code });
          return false;
        }
      }
      event.preventDefault();
      event.stopImmediatePropagation();
      processed.set(event, true);
      return true;
    };
    const onKey = (event: KeyboardEvent) => {
      processKey(event);
    };
    const removePreprocessor = installShortcutPreprocessor(processKey);
    window.addEventListener("keydown", onKey, true);
    window.addEventListener("keyup", onKey, true);
    window.addEventListener("blur", reset);
    document.addEventListener("visibilitychange", reset);
    return () => {
      reset();
      removePreprocessor();
      window.removeEventListener("keydown", onKey, true);
      window.removeEventListener("keyup", onKey, true);
      window.removeEventListener("blur", reset);
      document.removeEventListener("visibilitychange", reset);
    };
  }, [controller, settings.leaderShortcut]);

  useEffect(() => {
    const preview = window.desktopBridge?.preview;
    void preview
      ?.configureLeader?.({
        trigger: settings.leaderShortcut,
        bindings: keybindings,
        context: { terminalOpen },
      })
      .catch(console.error);
    return preview?.onLeaderInput?.((input) => {
      setLeaderActive(input.type === "prefix");
      if (input.type !== "cancel") controller.suppress({ ...input, type: "keydown" });
      if (input.type === "stroke") {
        dispatchLeaderShortcut(new KeyboardEvent("leader", { ...input, cancelable: true }));
      }
    });
  }, [controller, keybindings, settings.leaderShortcut, terminalOpen]);

  useEffect(
    () =>
      registerLeaderShortcutHandler(
        (event) => {
          if (
            event.defaultPrevented ||
            event.repeat ||
            isCommandPaletteOpen() ||
            isModelPickerOpen()
          )
            return;
          const command = resolveShortcutCommand(event, keybindings, {
            context: {
              terminalFocus: isTerminalFocused(),
              terminalOpen,
              previewFocus: isPreviewFocused(),
              previewOpen,
            },
          });
          const direction = threadTraversalDirectionFromCommand(command);
          const jumpIndex = threadJumpIndexFromCommand(command ?? "");
          if (direction === null && jumpIndex === null) return;
          event.preventDefault();
          const snapshot = readThreadShortcutTargets();
          const targets = snapshot.targets.filter((ref) => readThreadShell(ref) !== null);
          const current = routeThreadRef ?? snapshot.current;
          const target =
            direction !== null
              ? resolveAdjacentThreadId({
                  threadIds: targets.map(scopedThreadKey),
                  currentThreadId: current ? scopedThreadKey(current) : null,
                  direction,
                })
              : targets[jumpIndex ?? -1];
          const ref =
            typeof target === "string"
              ? targets.find((entry) => scopedThreadKey(entry) === target)
              : target;
          if (!ref) return;
          void navigate({
            to: "/$environmentId/$threadId",
            params: { environmentId: ref.environmentId, threadId: ref.threadId },
          });
        },
        false,
        true,
      ),
    [keybindings, navigate, routeThreadRef, terminalOpen, previewOpen],
  );

  return active ? (
    <div
      role="status"
      className="pointer-events-none fixed bottom-4 left-1/2 z-[100] -translate-x-1/2 rounded-md border bg-popover px-3 py-1.5 text-xs text-popover-foreground shadow-sm"
    >
      Leader · press a shortcut or Esc
    </div>
  ) : null;
}
