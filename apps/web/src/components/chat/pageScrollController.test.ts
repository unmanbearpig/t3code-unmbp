import { describe, expect, test } from "vite-plus/test";

import {
  createPageScrollController,
  getTimelinePageScrollKey,
  getPageScrollDistancePx,
} from "./pageScrollController";

describe("page scroll helpers", () => {
  const composerPageScrollEvent = (
    overrides: Partial<Parameters<typeof getTimelinePageScrollKey>[0]> = {},
  ) => ({
    altKey: false,
    clientHeight: 200,
    ctrlKey: false,
    defaultPrevented: false,
    isComposing: false,
    key: "PageDown",
    keyCode: 34,
    metaKey: false,
    scrollHeight: 200,
    scrollTop: 0,
    shiftKey: false,
    ...overrides,
  });

  test("leaves page keys to IME composition", () => {
    expect(getTimelinePageScrollKey(composerPageScrollEvent({ isComposing: true }))).toBeNull();
    expect(getTimelinePageScrollKey(composerPageScrollEvent({ keyCode: 229 }))).toBeNull();
  });

  test("leaves page keys to an overflowing composer until it reaches the boundary", () => {
    expect(
      getTimelinePageScrollKey(composerPageScrollEvent({ scrollHeight: 600, scrollTop: 0 })),
    ).toBeNull();
    expect(
      getTimelinePageScrollKey(
        composerPageScrollEvent({
          key: "PageUp",
          keyCode: 33,
          scrollHeight: 600,
          scrollTop: 400,
        }),
      ),
    ).toBeNull();

    expect(
      getTimelinePageScrollKey(
        composerPageScrollEvent({
          key: "PageUp",
          keyCode: 33,
          scrollHeight: 600,
          scrollTop: 0,
        }),
      ),
    ).toBe("PageUp");
    expect(
      getTimelinePageScrollKey(composerPageScrollEvent({ scrollHeight: 600, scrollTop: 400 })),
    ).toBe("PageDown");
  });

  test("hands off page keys within a fractional pixel of the composer boundary", () => {
    expect(
      getTimelinePageScrollKey(
        composerPageScrollEvent({
          key: "PageUp",
          keyCode: 33,
          scrollHeight: 600,
          scrollTop: 0.5,
        }),
      ),
    ).toBe("PageUp");
    expect(
      getTimelinePageScrollKey(composerPageScrollEvent({ scrollHeight: 600, scrollTop: 399.5 })),
    ).toBe("PageDown");
  });
});

describe("createPageScrollController", () => {
  function createController(scrollTop = 0, scrollHeight = 4_000) {
    const container = {
      clientHeight: 600,
      scrollHeight,
      scrollTop,
      getBoundingClientRect: () => ({ height: 600 }),
    };
    const started: string[] = [];
    const controller = createPageScrollController({
      getContainer: () => container,
      getScrollPaddingBottomPx: () => 24,
      onScrollStart: (key) => started.push(key),
    });
    return { container, controller, started };
  }

  test("moves a page immediately and keeps the composer area clear", () => {
    const { container, controller } = createController();
    controller.handleKeyDown("PageDown");
    expect(container.scrollTop).toBe(540);
    controller.handleKeyUp("PageDown");
    expect(container.scrollTop).toBe(540);
    expect(getPageScrollDistancePx({ containerHeightPx: 40, scrollPaddingBottomPx: 24 })).toBe(0);
  });

  test("moves another page on each keyboard repeat without restarting scroll intent", () => {
    const { container, controller, started } = createController();
    controller.handleKeyDown("PageDown");
    controller.handleKeyDown("PageDown");
    controller.handleKeyDown("PageDown");
    expect(container.scrollTop).toBe(1_620);
    expect(started).toEqual(["PageDown"]);
  });

  test("clamps both directions at the timeline edges", () => {
    const { container, controller } = createController(1_000, 1_800);
    controller.handleKeyDown("PageDown");
    expect(container.scrollTop).toBe(1_200);
    controller.handleKeyDown("PageUp");
    controller.handleKeyDown("PageUp");
    controller.handleKeyDown("PageUp");
    expect(container.scrollTop).toBe(0);
  });

  test("starts a new scroll intent after key release or direction change", () => {
    const { container, controller, started } = createController(2_000);
    controller.handleKeyDown("PageUp");
    controller.handleKeyUp("Shift");
    controller.handleKeyDown("PageUp");
    controller.handleKeyUp("PageUp");
    controller.handleKeyDown("PageUp");
    controller.handleKeyDown("PageDown");
    expect(container.scrollTop).toBe(920);
    expect(started).toEqual(["PageUp", "PageUp", "PageDown"]);
  });

  test("releases scroll intent on blur and cleanup", () => {
    const { controller, started } = createController();
    controller.handleKeyDown("PageDown");
    controller.releaseActiveKey();
    controller.handleKeyDown("PageDown");
    controller.dispose();
    controller.handleKeyDown("PageDown");
    expect(started).toEqual(["PageDown", "PageDown", "PageDown"]);
  });

  test("does not start a page scroll at the timeline boundary", () => {
    const { container, controller, started } = createController(0.5, 1_800);
    controller.handleKeyDown("PageUp");
    expect(container.scrollTop).toBe(0.5);
    container.scrollTop = 1_199.5;
    controller.handleKeyDown("PageDown");
    expect(container.scrollTop).toBe(1_199.5);
    expect(started).toEqual([]);
  });

  test("ignores page keys while the timeline is unmounted", () => {
    const started: string[] = [];
    const controller = createPageScrollController({
      getContainer: () => null,
      getScrollPaddingBottomPx: () => 24,
      onScrollStart: (key) => started.push(key),
    });
    controller.handleKeyDown("PageDown");
    expect(started).toEqual([]);
  });
});
