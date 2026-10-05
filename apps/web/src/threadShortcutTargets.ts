import type { ScopedThreadRef } from "@t3tools/contracts";

let targets: readonly ScopedThreadRef[] = [];
let current: ScopedThreadRef | null = null;

/** Keep the displayed order available when Settings replaces the thread sidebar. */
export function updateThreadShortcutTargets(
  next: readonly ScopedThreadRef[],
  active: ScopedThreadRef | null,
) {
  targets = next;
  if (active) current = active;
}
export function readThreadShortcutTargets() {
  return { targets, current };
}
