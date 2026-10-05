const PAGE_SCROLL_ALIGNMENT_OFFSET_PX = 36;
const PAGE_SCROLL_BOUNDARY_EPSILON_PX = 1;

export type PageScrollKey = "PageUp" | "PageDown";

type PageScrollMetrics = {
  clientHeight: number;
  scrollHeight: number;
  scrollTop: number;
};

function canScrollInDirection(
  { clientHeight, scrollHeight, scrollTop }: PageScrollMetrics,
  key: PageScrollKey,
): boolean {
  const maxScrollTop = Math.max(0, scrollHeight - clientHeight);
  const clampedScrollTop = Math.min(maxScrollTop, Math.max(0, scrollTop));
  return key === "PageUp"
    ? clampedScrollTop > PAGE_SCROLL_BOUNDARY_EPSILON_PX
    : clampedScrollTop < maxScrollTop - PAGE_SCROLL_BOUNDARY_EPSILON_PX;
}

export function getTimelinePageScrollKey({
  altKey,
  clientHeight,
  ctrlKey,
  defaultPrevented,
  isComposing,
  key,
  keyCode,
  metaKey,
  scrollHeight,
  scrollTop,
  shiftKey,
}: {
  altKey: boolean;
  clientHeight: number;
  ctrlKey: boolean;
  defaultPrevented: boolean;
  isComposing: boolean;
  key: string;
  keyCode: number;
  metaKey: boolean;
  scrollHeight: number;
  scrollTop: number;
  shiftKey: boolean;
}): PageScrollKey | null {
  if (key !== "PageUp" && key !== "PageDown") {
    return null;
  }
  if (
    defaultPrevented ||
    isComposing ||
    keyCode === 229 ||
    altKey ||
    ctrlKey ||
    metaKey ||
    shiftKey
  ) {
    return null;
  }

  const editorCanScroll = canScrollInDirection({ clientHeight, scrollHeight, scrollTop }, key);
  return editorCanScroll ? null : key;
}

type PageScrollContainer = PageScrollMetrics & {
  getBoundingClientRect: () => {
    height: number;
  };
};

export function getPageScrollDistancePx({
  containerHeightPx,
  scrollPaddingBottomPx,
}: {
  containerHeightPx: number;
  scrollPaddingBottomPx: number;
}): number {
  return Math.max(0, containerHeightPx - PAGE_SCROLL_ALIGNMENT_OFFSET_PX - scrollPaddingBottomPx);
}

function getDirection(key: PageScrollKey): -1 | 1 {
  return key === "PageUp" ? -1 : 1;
}

export function createPageScrollController({
  getContainer,
  getScrollPaddingBottomPx,
  onScrollStart,
}: {
  getContainer: () => PageScrollContainer | null;
  getScrollPaddingBottomPx: () => number;
  onScrollStart?: (key: PageScrollKey) => void;
}) {
  let activeKey: PageScrollKey | null = null;
  const releaseActiveKey = () => {
    activeKey = null;
  };

  return {
    handleKeyDown(key: PageScrollKey) {
      const container = getContainer();
      if (!container || !canScrollInDirection(container, key)) return;

      if (activeKey !== key) {
        activeKey = key;
        onScrollStart?.(key);
      }

      const distance = getPageScrollDistancePx({
        containerHeightPx: container.getBoundingClientRect().height,
        scrollPaddingBottomPx: getScrollPaddingBottomPx(),
      });
      const maxScrollTop = Math.max(0, container.scrollHeight - container.clientHeight);
      // Each keydown, including native keyboard repeats, moves one page immediately.
      container.scrollTop = Math.min(
        maxScrollTop,
        Math.max(0, container.scrollTop + distance * getDirection(key)),
      );
    },
    handleKeyUp(key: string) {
      if (activeKey === key) releaseActiveKey();
    },
    releaseActiveKey,
    dispose: releaseActiveKey,
  };
}
