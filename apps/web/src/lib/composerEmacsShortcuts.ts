interface EditingKeyEvent {
  key: string;
  code?: string;
  ctrlKey: boolean;
  altKey: boolean;
  metaKey: boolean;
  shiftKey: boolean;
  isComposing?: boolean;
  repeat?: boolean;
}

const CONTROL_ACTIONS = {
  a: "lineStart",
  e: "lineEnd",
  b: "backward",
  f: "forward",
  p: "previousLine",
  n: "nextLine",
  h: "backspace",
  d: "delete",
  k: "killEnd",
  u: "killStart",
  w: "killWordBackward",
  y: "yank",
  j: "newline",
  o: "openLine",
  t: "transpose",
  backspace: "killWordBackward",
  delete: "killWordForward",
  arrowleft: "wordBackward",
  arrowright: "wordForward",
} as const;

const ALT_ACTIONS = {
  b: "wordBackward",
  f: "wordForward",
  d: "killWordForward",
  backspace: "killWordBackward",
  delete: "killWordForward",
  arrowleft: "wordBackward",
  arrowright: "wordForward",
  arrowup: "lineStart",
  arrowdown: "lineEnd",
  "<": "documentStart",
  ">": "documentEnd",
} as const;

export function composerEmacsAction(event: EditingKeyEvent) {
  if (event.isComposing || event.metaKey) return null;
  let key = event.key.toLowerCase();
  // Option can produce a symbol on macOS instead of the shortcut's letter.
  // Preserve logical Latin letters on remapped and non-QWERTY layouts.
  if (event.altKey && !/^[a-z]$/.test(key) && /^Key[A-Z]$/.test(event.code ?? "")) {
    key = event.code!.slice(3).toLowerCase();
  }
  let action:
    | (typeof CONTROL_ACTIONS)[keyof typeof CONTROL_ACTIONS]
    | (typeof ALT_ACTIONS)[keyof typeof ALT_ACTIONS]
    | null = null;
  if (event.ctrlKey && !event.altKey) {
    // Ctrl-Shift-N opens an external terminal; Ctrl-N still moves down a line.
    if (key === "n" && event.shiftKey) return null;
    action = CONTROL_ACTIONS[key as keyof typeof CONTROL_ACTIONS] ?? null;
  }
  if (event.altKey && !event.ctrlKey) {
    action = ALT_ACTIONS[key as keyof typeof ALT_ACTIONS] ?? null;
  }
  if (event.shiftKey && action && !COMPOSER_MOVEMENT_ACTIONS.has(action)) return null;
  return action;
}

const COMPOSER_MOVEMENT_ACTIONS = new Set([
  "lineStart",
  "lineEnd",
  "backward",
  "forward",
  "previousLine",
  "nextLine",
  "wordBackward",
  "wordForward",
  "documentStart",
  "documentEnd",
]);

/** Capture-phase app shortcuts yield to editing, except a fresh Ctrl+D on an empty prompt. */
export function isComposerEmacsEditingShortcut(
  event: EditingKeyEvent,
  target: EventTarget | null | undefined,
) {
  const composer =
    typeof Element !== "undefined" && target instanceof Element
      ? target.closest('[data-composer-emacs="true"]')
      : null;
  if (!composer) return false;
  const action = composerEmacsAction(event);
  return (
    action !== null &&
    (action !== "delete" ||
      event.repeat === true ||
      composer.getAttribute("data-composer-empty") !== "true")
  );
}
