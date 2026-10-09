let animationsDisabled = false;
const listeners = new Set<() => void>();

/** Used by imperative animations as well as the renderer's CSS policy. */
export function prefersReducedMotion(view?: Pick<Window, "matchMedia">): boolean {
  if (animationsDisabled) return true;
  const target = view ?? (typeof window === "undefined" ? globalThis : window);
  return target.matchMedia?.("(prefers-reduced-motion: reduce)").matches ?? false;
}

export function subscribeAnimationPreference(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function setAnimationsDisabled(disabled: boolean): void {
  const changed = animationsDisabled !== disabled;
  animationsDisabled = disabled;
  if (typeof document !== "undefined" && document.documentElement) {
    document.documentElement.dataset.animations = disabled ? "disabled" : "enabled";
    if (disabled && changed) {
      for (const animation of document.getAnimations?.() ?? []) {
        try {
          animation.finish();
        } catch {
          // Infinite indicators cannot finish; cancel them instead.
          animation.cancel();
        }
      }
    }
  }
  if (changed) for (const listener of listeners) listener();
}
