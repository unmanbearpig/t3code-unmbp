// @vitest-environment jsdom
import { act, Profiler } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";

import { PromptCacheElapsed } from "./PromptCacheElapsed";

const start = Date.parse("2026-10-14T12:00:00.000Z");
let root: Root;
let container: HTMLDivElement;

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(start);
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.spyOn(document, "hidden", "get").mockReturnValue(false);
  container = document.createElement("div");
  root = createRoot(container);
});

afterEach(() => {
  act(() => root.unmount());
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

it("counts up without React commits and resets when a new write arrives", () => {
  const onRender = vi.fn();
  const render = (writtenAt: number) =>
    act(() =>
      root.render(
        <Profiler id="timer" onRender={onRender}>
          <PromptCacheElapsed writtenAt={writtenAt} />
        </Profiler>,
      ),
    );
  render(start - 60_000);
  expect(container.textContent).toBe("1:00");
  const commits = onRender.mock.calls.length;
  act(() => vi.advanceTimersByTime(3_000));
  expect(container.textContent).toBe("1:03");
  expect(onRender).toHaveBeenCalledTimes(commits);
  render(start + 3_000);
  expect(container.textContent).toBe("0:00");
  act(() => vi.advanceTimersByTime(1_000));
  expect(container.textContent).toBe("0:01");
});

it("sleeps while hidden, catches up on return, and cleans up on unmount", () => {
  act(() => root.render(<PromptCacheElapsed writtenAt={start} />));
  vi.spyOn(document, "hidden", "get").mockReturnValue(true);
  act(() => document.dispatchEvent(new Event("visibilitychange")));
  expect(vi.getTimerCount()).toBe(0);
  act(() => vi.advanceTimersByTime(90_000));
  expect(container.textContent).toBe("0:00");
  vi.spyOn(document, "hidden", "get").mockReturnValue(false);
  act(() => document.dispatchEvent(new Event("visibilitychange")));
  expect(container.textContent).toBe("1:30");
  expect(vi.getTimerCount()).toBe(1);
  act(() => root.render(null));
  expect(vi.getTimerCount()).toBe(0);
});
