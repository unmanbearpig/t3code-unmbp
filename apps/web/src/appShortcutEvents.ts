type ShortcutHandler = (event: KeyboardEvent) => void;
const handlers = new Set<{ handler: ShortcutHandler; capture: boolean; fallback?: boolean }>();
const leaderEvents = new WeakSet<object>();
let preprocess: ((event: KeyboardEvent) => boolean) | undefined;

export function installShortcutPreprocessor(handler: (event: KeyboardEvent) => boolean) {
  preprocess = handler;
  return () => {
    if (preprocess === handler) preprocess = undefined;
  };
}

export function isLeaderShortcutEvent(event: { key: string; leaderKey?: boolean }): boolean {
  return event.leaderKey === true || leaderEvents.has(event);
}

/** Registers a command consumer for native input and leader input alike. */
export function addAppShortcutListener(
  target: Window | Document,
  handler: ShortcutHandler,
  capture = false,
) {
  const entry = { handler, capture };
  handlers.add(entry);
  const nativeHandler = (event: KeyboardEvent) => {
    if (event.target instanceof Element && event.target.closest("[data-keybinding-capture]"))
      return;
    if (!preprocess?.(event)) handler(event);
  };
  target.addEventListener("keydown", nativeHandler as EventListener, capture);
  return () => {
    handlers.delete(entry);
    target.removeEventListener("keydown", nativeHandler as EventListener, capture);
  };
}

/** Local editor consumers register here when their normal input is owned by React. */
export function registerLeaderShortcutHandler(
  handler: ShortcutHandler,
  capture = false,
  fallback = false,
) {
  const entry = { handler, capture, fallback };
  handlers.add(entry);
  return () => {
    handlers.delete(entry);
  };
}

export function dispatchLeaderShortcut(event: KeyboardEvent): boolean {
  leaderEvents.add(event);
  try {
    const orderedHandlers = [...handlers].sort(
      (a, b) => Number(a.fallback ?? false) - Number(b.fallback ?? false),
    );
    for (const capture of [true, false]) {
      for (const entry of orderedHandlers) {
        if (entry.capture !== capture) continue;
        entry.handler(event);
        if (event.defaultPrevented) return true;
      }
    }
    return false;
  } finally {
    leaderEvents.delete(event);
  }
}
