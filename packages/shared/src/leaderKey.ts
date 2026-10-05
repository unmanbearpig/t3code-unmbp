import type { KeybindingShortcut, KeybindingWhenNode } from "@t3tools/contracts";

export const LEADER_TIMEOUT_MS = 2_000;

export interface LeaderStroke {
  readonly key: string;
  readonly code?: string;
  readonly ctrlKey: boolean;
  readonly metaKey: boolean;
  readonly shiftKey: boolean;
  readonly altKey: boolean;
}

export function matchesLeaderStroke(
  input: LeaderStroke,
  shortcut: KeybindingShortcut,
  isMac: boolean,
) {
  return (
    input.key.toLowerCase() === shortcut.key &&
    input.ctrlKey === (shortcut.ctrlKey || (shortcut.modKey && !isMac)) &&
    input.metaKey === (shortcut.metaKey || (shortcut.modKey && isMac)) &&
    input.shiftKey === shortcut.shiftKey &&
    input.altKey === shortcut.altKey
  );
}

export function matchesLeaderWhen(
  node: KeybindingWhenNode | undefined,
  context: Readonly<Record<string, boolean>>,
): boolean {
  if (!node) return true;
  switch (node.type) {
    case "identifier":
      return node.name === "true" || (node.name !== "false" && context[node.name] === true);
    case "not":
      return !matchesLeaderWhen(node.node, context);
    case "and":
      return matchesLeaderWhen(node.left, context) && matchesLeaderWhen(node.right, context);
    case "or":
      return matchesLeaderWhen(node.left, context) || matchesLeaderWhen(node.right, context);
  }
}

export interface LeaderKeyInput {
  readonly type: "keydown" | "keyup";
  readonly key: string;
  readonly code?: string;
  readonly repeat?: boolean;
  readonly isComposing?: boolean;
}

/** Owns one window's pending leader stroke, including consumed key releases. */
export function createLeaderKeyController() {
  let deadline = 0;
  const suppressedKeys = new Set<string>();
  const keyId = (input: LeaderKeyInput) => input.code || input.key;
  return {
    active(now: number) {
      return deadline > now;
    },
    cancel() {
      deadline = 0;
    },
    reset() {
      deadline = 0;
      suppressedKeys.clear();
    },
    suppress(input: LeaderKeyInput) {
      suppressedKeys.add(keyId(input));
    },
    release(input: LeaderKeyInput) {
      suppressedKeys.delete(keyId(input));
    },
    handle(input: LeaderKeyInput, isTrigger: boolean, now: number) {
      if (input.isComposing) {
        deadline = 0;
        return "pass" as const;
      }
      const id = keyId(input);
      if (input.type === "keyup") {
        return suppressedKeys.delete(id) ? ("consume" as const) : ("pass" as const);
      }
      if (suppressedKeys.has(id) && input.repeat) return "consume" as const;
      if (!input.repeat) suppressedKeys.delete(id);
      if (isTrigger) {
        if (!input.repeat) deadline = now + LEADER_TIMEOUT_MS;
        suppressedKeys.add(id);
        return "prefix" as const;
      }
      if (deadline <= now) {
        deadline = 0;
        return "pass" as const;
      }
      if (["Control", "Meta", "Alt", "Shift"].includes(input.key)) return "pass" as const;
      deadline = 0;
      if (input.repeat) return "pass" as const;
      suppressedKeys.add(id);
      return input.key === "Escape" ? ("consume" as const) : ("stroke" as const);
    },
  };
}
