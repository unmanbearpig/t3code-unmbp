import { afterEach, expect, it, vi } from "vite-plus/test";

import {
  prefersReducedMotion,
  setAnimationsDisabled,
  subscribeAnimationPreference,
} from "./animationPreference";

afterEach(() => {
  vi.unstubAllGlobals();
  setAnimationsDisabled(false);
});

it("finishes pending transitions, stops infinite indicators, and restores motion", () => {
  const transition = { finish: vi.fn(), cancel: vi.fn() };
  const indicator = {
    finish: () => {
      throw new Error("Infinite animation");
    },
    cancel: vi.fn(),
  };
  const dataset: Record<string, string> = {};
  vi.stubGlobal("document", {
    documentElement: { dataset },
    getAnimations: () => [transition, indicator],
  });
  vi.stubGlobal("matchMedia", () => ({ matches: false }));
  const listener = vi.fn();
  const unsubscribe = subscribeAnimationPreference(listener);
  try {
    setAnimationsDisabled(true);
    expect(prefersReducedMotion()).toBe(true);
    expect(transition.finish).toHaveBeenCalledOnce();
    expect(indicator.cancel).toHaveBeenCalledOnce();
    expect(dataset.animations).toBe("disabled");
    setAnimationsDisabled(false);
    expect(prefersReducedMotion()).toBe(false);
    expect(dataset.animations).toBe("enabled");
    expect(listener).toHaveBeenCalledTimes(2);
  } finally {
    unsubscribe();
  }
});

it("respects the system's reduced motion preference when animations are enabled", () => {
  vi.stubGlobal("matchMedia", () => ({ matches: true }));
  setAnimationsDisabled(false);
  expect(prefersReducedMotion()).toBe(true);
});
