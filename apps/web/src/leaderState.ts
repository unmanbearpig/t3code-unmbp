import { useSyncExternalStore } from "react";
import type { KeybindingShortcut } from "@t3tools/contracts";
import { DEFAULT_CLIENT_SETTINGS } from "@t3tools/contracts/settings";

let active = false;
let trigger = DEFAULT_CLIENT_SETTINGS.leaderShortcut;
const listeners = new Set<() => void>();
export function setLeaderActive(next: boolean) {
  if (active === next) return;
  active = next;
  for (const listener of listeners) listener();
}
export function setLeaderTrigger(next: KeybindingShortcut | null) {
  trigger = next;
}
export function getLeaderTrigger() {
  return trigger;
}
export function useLeaderActive() {
  return useSyncExternalStore(
    (listener) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    () => active,
  );
}
