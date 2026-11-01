import { act, useLayoutEffect } from "react";
import { createRoot } from "react-dom/client";
import type { NativeScrollEvent, NativeSyntheticEvent } from "react-native";
import { expect, it, vi } from "vite-plus/test";

import { useSwipeableScrollGate } from "./use-swipeable-scroll-gate";

function scrollEvent(y: number): NativeSyntheticEvent<NativeScrollEvent> {
  return { nativeEvent: { contentOffset: { x: 0, y } } } as NativeSyntheticEvent<NativeScrollEvent>;
}

async function renderScrollGate() {
  let gate: ReturnType<typeof useSwipeableScrollGate> | undefined;
  function Probe() {
    const value = useSwipeableScrollGate();
    useLayoutEffect(() => {
      gate = value;
    });
    return null;
  }
  const root = createRoot(document.createElement("div"));
  try {
    await act(async () => root.render(<Probe />));
    return {
      get current() {
        return gate!;
      },
      [Symbol.dispose]() {
        act(() => root.unmount());
      },
    };
  } catch (error) {
    act(() => root.unmount());
    throw error;
  }
}

it("re-enables swipes when Android interrupts momentum without an end event", async () => {
  using _timers = { [Symbol.dispose]: () => vi.useRealTimers() };
  vi.useFakeTimers();
  using screen = await renderScrollGate();
  expect(screen.current.swipeEnabled).toBe(true);

  act(() => {
    screen.current.scrollGateHandlers.onScrollBeginDrag(scrollEvent(0));
    screen.current.scrollGateHandlers.onScroll(scrollEvent(20));
    screen.current.scrollGateHandlers.onScrollEndDrag();
    screen.current.scrollGateHandlers.onMomentumScrollBegin();
  });
  expect(screen.current.swipeEnabled).toBe(false);

  act(() => {
    vi.advanceTimersByTime(120);
    screen.current.scrollGateHandlers.onScroll(scrollEvent(40));
    vi.advanceTimersByTime(120);
    screen.current.scrollGateHandlers.onScroll(scrollEvent(60));
    vi.advanceTimersByTime(159);
  });
  expect(screen.current.swipeEnabled).toBe(false);

  act(() => vi.advanceTimersByTime(1));
  expect(screen.current.swipeEnabled).toBe(true);
});

it("keeps an ongoing drag gated and re-enables swipes after drag or momentum ends", async () => {
  using _timers = { [Symbol.dispose]: () => vi.useRealTimers() };
  vi.useFakeTimers();
  using screen = await renderScrollGate();

  act(() => {
    screen.current.scrollGateHandlers.onScrollBeginDrag(scrollEvent(0));
    screen.current.scrollGateHandlers.onScroll(scrollEvent(20));
    vi.advanceTimersByTime(500);
  });
  expect(screen.current.swipeEnabled).toBe(false);

  act(() => {
    screen.current.scrollGateHandlers.onScrollEndDrag();
    vi.advanceTimersByTime(160);
  });
  expect(screen.current.swipeEnabled).toBe(true);

  act(() => {
    screen.current.scrollGateHandlers.onScrollBeginDrag(scrollEvent(20));
    screen.current.scrollGateHandlers.onScroll(scrollEvent(40));
    screen.current.scrollGateHandlers.onScrollEndDrag();
    screen.current.scrollGateHandlers.onMomentumScrollBegin();
    screen.current.scrollGateHandlers.onMomentumScrollEnd();
  });
  expect(screen.current.swipeEnabled).toBe(true);
  expect(vi.getTimerCount()).toBe(0);
});
