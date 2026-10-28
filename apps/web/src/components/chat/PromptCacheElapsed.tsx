import { formatPromptCacheElapsed } from "@t3tools/client-runtime/prompt-cache";
import { TimerIcon } from "lucide-react";
import { useLayoutEffect, useRef } from "react";

import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";

/** Updates only the timer's text and sleeps while the page is hidden. */
export function PromptCacheElapsed({ writtenAt }: { writtenAt: number }) {
  const textRef = useRef<HTMLSpanElement>(null);

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- updates the timer DOM node and owns its visibility listener and interval
  useLayoutEffect(() => {
    let timer: ReturnType<typeof setInterval> | undefined;
    const stop = () => {
      clearInterval(timer);
      timer = undefined;
    };
    const update = () => {
      const now = Date.now();
      if (textRef.current) {
        textRef.current.textContent = formatPromptCacheElapsed(writtenAt, now);
      }
    };
    const resume = () => {
      stop();
      update();
      if (!document.hidden) {
        timer = setInterval(update, 1_000);
      }
    };
    resume();
    document.addEventListener("visibilitychange", resume);
    return () => {
      stop();
      document.removeEventListener("visibilitychange", resume);
    };
  }, [writtenAt]);

  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <span
            role="timer"
            aria-label="Time since last reported cache write"
            className="inline-flex shrink-0 items-center gap-1 text-muted-foreground text-2xs tabular-nums"
          >
            <TimerIcon aria-hidden="true" className="size-3" />
            <span ref={textRef} />
          </span>
        }
      />
      <TooltipPopup>Time since last reported cache write</TooltipPopup>
    </Tooltip>
  );
}
