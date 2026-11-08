import { useAtomValue } from "@effect/atom-react";
import {
  deriveLastPromptCacheWriteAt,
  formatPromptCacheElapsed,
} from "@t3tools/client-runtime/prompt-cache";
import type { EnvironmentThread } from "@t3tools/client-runtime/state/shell";
import type { ScopedThreadRef } from "@t3tools/contracts";

import { AppText as Text } from "../../components/ui/AppText";
import { environmentThreadDetails } from "../../state/threads";
import { useVisibleSecondClock } from "./use-visible-second-clock";

const selectWrittenAt = (thread: EnvironmentThread | null) =>
  deriveLastPromptCacheWriteAt(thread?.projection ?? null);

function Elapsed({ writtenAt }: { writtenAt: number }) {
  const now = useVisibleSecondClock(true);
  return (
    <Text
      accessibilityLabel="Time since last reported cache write"
      accessibilityLiveRegion="none"
      className="shrink-0 px-2 font-sans text-xs text-foreground-secondary"
      style={{ fontVariant: ["tabular-nums"] }}
    >
      {formatPromptCacheElapsed(writtenAt, now)}
    </Text>
  );
}

export function PromptCacheElapsed({ threadRef }: { threadRef: ScopedThreadRef }) {
  const writtenAt = useAtomValue(environmentThreadDetails.threadAtom(threadRef), selectWrittenAt);
  return writtenAt === null ? null : <Elapsed key={writtenAt} writtenAt={writtenAt} />;
}
