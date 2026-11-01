import { isLeaderShortcutEvent } from "../appShortcutEvents";

interface EditingKeyEvent {
  key: string;
  ctrlKey: boolean;
  altKey: boolean;
  metaKey: boolean;
  shiftKey: boolean;
  isComposing?: boolean;
  keyCode?: number;
}

export function isSecretInputKillShortcut(
  event: EditingKeyEvent,
  target: EventTarget | null | undefined,
): target is HTMLInputElement {
  return (
    event.key.toLowerCase() === "k" &&
    event.ctrlKey &&
    !event.altKey &&
    !event.metaKey &&
    !event.shiftKey &&
    !event.isComposing &&
    event.keyCode !== 229 &&
    !isLeaderShortcutEvent(event) &&
    typeof HTMLInputElement !== "undefined" &&
    target instanceof HTMLInputElement &&
    (target.type === "password" || target.hasAttribute("data-secret-input")) &&
    !target.readOnly &&
    !target.disabled &&
    target.selectionStart !== null &&
    !target.closest("[data-keybinding-capture], [data-terminal-owner], .t3-ghostty-input")
  );
}

export function installSecretInputEditing(target: Window): () => void {
  const onKeyDown = (event: KeyboardEvent) => {
    const input = event.target;
    if (event.defaultPrevented || !isSecretInputKillShortcut(event, input)) return;
    event.preventDefault();
    event.stopPropagation();
    const start = input.selectionStart!;
    if (start === input.value.length) return;

    const next = input.value.slice(0, start);
    input.setSelectionRange(start, input.value.length);
    // Native deletion preserves the browser's undo history.
    if (input.ownerDocument.execCommand?.("delete", false)) return;

    // Bypass React's value tracker so onChange receives the edit.
    const setValue = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!;
    setValue.call(input, next);
    input.setSelectionRange(start, start);
    input.dispatchEvent(
      new InputEvent("input", { bubbles: true, inputType: "deleteSoftLineForward" }),
    );
  };
  target.addEventListener("keydown", onKeyDown, true);
  return () => target.removeEventListener("keydown", onKeyDown, true);
}
