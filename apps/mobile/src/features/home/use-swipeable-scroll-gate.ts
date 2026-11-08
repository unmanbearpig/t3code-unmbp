import { useCallback, useEffect, useRef, useState } from "react";
import type { NativeScrollEvent, NativeSyntheticEvent } from "react-native";

/** Trackpads can start pans mid-momentum, bypassing failOffsetY; disable swipes during vertical scrolls. */
export function useSwipeableScrollGate(options?: {
  readonly onScroll?: (event: NativeSyntheticEvent<NativeScrollEvent>) => void;
  readonly onScrollBeginDrag?: (event: NativeSyntheticEvent<NativeScrollEvent>) => void;
}) {
  const [gateActive, setGateActive] = useState(false);
  const gateActiveRef = useRef(false);
  const draggingRef = useRef(false);
  const dragStartYRef = useRef(0);
  const settleTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const externalOnScroll = options?.onScroll;
  const externalOnScrollBeginDrag = options?.onScrollBeginDrag;

  const update = useCallback((next: boolean) => {
    if (gateActiveRef.current !== next) {
      gateActiveRef.current = next;
      setGateActive(next);
    }
  }, []);
  const clearSettle = useCallback(() => {
    if (settleTimerRef.current !== null) {
      clearTimeout(settleTimerRef.current);
      settleTimerRef.current = null;
    }
  }, []);
  const scheduleSettle = useCallback(() => {
    clearSettle();
    settleTimerRef.current = setTimeout(() => {
      settleTimerRef.current = null;
      update(false);
    }, 160);
  }, [clearSettle, update]);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- clears the scroll settle timer on unmount
  useEffect(() => clearSettle, [clearSettle]);

  const onScrollBeginDrag = useCallback(
    (event: NativeSyntheticEvent<NativeScrollEvent>) => {
      draggingRef.current = true;
      dragStartYRef.current = event.nativeEvent.contentOffset.y;
      clearSettle();
      externalOnScrollBeginDrag?.(event);
    },
    [clearSettle, externalOnScrollBeginDrag],
  );
  const onScroll = useCallback(
    (event: NativeSyntheticEvent<NativeScrollEvent>) => {
      if (
        draggingRef.current &&
        !gateActiveRef.current &&
        Math.abs(event.nativeEvent.contentOffset.y - dragStartYRef.current) > 4
      ) {
        update(true);
      }
      if (!draggingRef.current && gateActiveRef.current) scheduleSettle();
      externalOnScroll?.(event);
    },
    [externalOnScroll, scheduleSettle, update],
  );
  const onScrollEndDrag = useCallback(() => {
    draggingRef.current = false;
    scheduleSettle();
  }, [scheduleSettle]);
  const onMomentumScrollEnd = useCallback(() => {
    clearSettle();
    update(false);
  }, [clearSettle, update]);

  return {
    swipeEnabled: !gateActive,
    scrollGateHandlers: {
      onScroll,
      onScrollBeginDrag,
      onScrollEndDrag,
      // Android can cancel momentum on touch-down without emitting its end event.
      onMomentumScrollBegin: scheduleSettle,
      onMomentumScrollEnd,
    },
  };
}
