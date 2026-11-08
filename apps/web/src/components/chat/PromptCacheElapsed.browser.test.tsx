import { Profiler } from "react";
import { expect, it, vi } from "vite-plus/test";
import { act, render } from "../../../test/render";
import { PromptCacheElapsed } from "./PromptCacheElapsed";

const start = Date.parse("2026-10-14T12:00:00.000Z");
function setup() {
  const stack = new DisposableStack();
  const now = stack.use(vi.spyOn(Date, "now").mockReturnValue(start));
  const hidden = stack.use(vi.spyOn(document, "hidden", "get").mockReturnValue(false));
  const timers = new Map<number, () => void>();
  let next = 1;
  stack.use(
    vi.spyOn(globalThis, "setInterval").mockImplementation((callback) => {
      const id = next++;
      if (typeof callback === "function") timers.set(id, () => callback());
      return id as unknown as ReturnType<typeof setInterval>;
    }),
  );
  stack.use(
    vi.spyOn(globalThis, "clearInterval").mockImplementation((id) => {
      timers.delete(Number(id));
    }),
  );
  return {
    now,
    hidden,
    timers,
    tick: () => {
      for (const tick of timers.values()) tick();
    },
    [Symbol.dispose]: () => stack.dispose(),
  };
}

it("counts up without React commits and resets when a new write arrives", async () => {
  using clock = setup();
  const onRender = vi.fn();
  const element = (writtenAt: number) => (
    <Profiler id="timer" onRender={onRender}>
      <PromptCacheElapsed writtenAt={writtenAt} />
    </Profiler>
  );
  await using screen = await render(element(start - 60_000));
  expect(screen.container.textContent).toContain("1:00");
  const commits = onRender.mock.calls.length;
  clock.now.mockReturnValue(start + 3_000);
  await act(() => clock.tick());
  expect(screen.container.textContent).toContain("1:03");
  expect(onRender).toHaveBeenCalledTimes(commits);
  await screen.rerender(element(start + 3_000));
  expect(screen.container.textContent).toContain("0:00");
  clock.now.mockReturnValue(start + 4_000);
  await act(() => clock.tick());
  expect(screen.container.textContent).toContain("0:01");
});

it("sleeps while hidden, catches up on return, and cleans up on unmount", async () => {
  using clock = setup();
  await using screen = await render(<PromptCacheElapsed writtenAt={start} />);
  clock.hidden.mockReturnValue(true);
  await act(() => document.dispatchEvent(new Event("visibilitychange")));
  expect(clock.timers.size).toBe(0);
  clock.now.mockReturnValue(start + 90_000);
  expect(screen.container.textContent).toContain("0:00");
  clock.hidden.mockReturnValue(false);
  await act(() => document.dispatchEvent(new Event("visibilitychange")));
  expect(screen.container.textContent).toContain("1:30");
  expect(clock.timers.size).toBe(1);
  await screen.unmount();
  expect(clock.timers.size).toBe(0);
});
