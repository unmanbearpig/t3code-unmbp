interface NewlineKeyEvent {
  key: string;
  ctrlKey: boolean;
  altKey: boolean;
  metaKey: boolean;
  shiftKey: boolean;
  isComposing?: boolean;
  keyCode?: number;
}

export function isCtrlNewlineShortcut(event: NewlineKeyEvent): boolean {
  return (
    event.key.toLowerCase() === "j" &&
    event.ctrlKey &&
    !event.altKey &&
    !event.metaKey &&
    !event.shiftKey &&
    !event.isComposing &&
    event.keyCode !== 229
  );
}

export function isTextboxNewlineShortcut(
  event: NewlineKeyEvent,
  target: EventTarget | null | undefined,
): boolean {
  if (
    !isCtrlNewlineShortcut(event) ||
    typeof Element === "undefined" ||
    !(target instanceof Element)
  ) {
    return false;
  }
  // Ghostty uses a hidden textarea to receive terminal input.
  if (target.closest("[data-terminal-owner], .t3-ghostty-input, [data-keybinding-capture]"))
    return false;
  if (target instanceof HTMLTextAreaElement) return !target.readOnly && !target.disabled;
  return target.closest('[contenteditable="true"][aria-multiline="true"]') !== null;
}

/** Native insertion keeps browser undo and React's controlled values in sync. */
export function installTextareaNewlineShortcut(target: Window): () => void {
  const onKeyDown = (event: KeyboardEvent) => {
    const textarea = event.target;
    if (
      event.defaultPrevented ||
      !(textarea instanceof HTMLTextAreaElement) ||
      !isTextboxNewlineShortcut(event, textarea)
    )
      return;
    event.preventDefault();
    event.stopPropagation();
    const document = textarea.ownerDocument;
    if (document.execCommand?.("insertText", false, "\n")) return;

    const { selectionStart, selectionEnd, value, maxLength } = textarea;
    const next = `${value.slice(0, selectionStart)}\n${value.slice(selectionEnd)}`;
    if (maxLength >= 0 && next.length > maxLength) return;
    // Bypass React's value tracker so the input event reaches onChange.
    const setValue = Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, "value")!.set!;
    setValue.call(textarea, next);
    textarea.setSelectionRange(selectionStart + 1, selectionStart + 1);
    textarea.dispatchEvent(
      new InputEvent("input", {
        bubbles: true,
        inputType: "insertLineBreak",
        data: "\n",
      }),
    );
  };
  target.addEventListener("keydown", onKeyDown, true);
  return () => target.removeEventListener("keydown", onKeyDown, true);
}
