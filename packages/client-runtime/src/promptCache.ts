import type { OrchestrationV2AppThread, OrchestrationV2ProviderTurn } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

/** Uses only writes reported for the active provider thread. */
export function deriveLastPromptCacheWriteAt(
  projection: {
    readonly thread: Pick<OrchestrationV2AppThread, "activeProviderThreadId">;
    readonly providerTurns: ReadonlyArray<
      Pick<OrchestrationV2ProviderTurn, "providerThreadId" | "tokenUsage" | "turnTokenUsage"> &
        Partial<Pick<OrchestrationV2ProviderTurn, "completedAt">>
    >;
  } | null,
): number | null {
  if (projection === null) return null;
  const activeId = projection.thread.activeProviderThreadId;
  if (activeId == null) return null;

  let latestAt: number | null = null;
  for (const turn of projection.providerTurns) {
    if (turn.providerThreadId !== activeId) continue;
    // Older Claude reports identified cache writes through their reported TTL.
    const writtenAt =
      turn.tokenUsage?.lastCacheWriteAt ??
      (turn.tokenUsage?.promptCacheTtlMs === undefined ? undefined : turn.tokenUsage.updatedAt);
    // Providers reporting only turn totals identify writes at turn completion.
    const at =
      writtenAt !== undefined
        ? Date.parse(writtenAt)
        : (turn.turnTokenUsage?.cacheCreationTokens ?? 0) > 0 && turn.completedAt != null
          ? DateTime.toEpochMillis(turn.completedAt)
          : NaN;
    if (!Number.isFinite(at)) continue;
    latestAt = latestAt === null ? at : Math.max(latestAt, at);
  }
  return latestAt;
}

export function formatPromptCacheElapsed(writtenAt: number, now: number): string {
  const seconds = Math.max(0, Math.floor((now - writtenAt) / 1_000));
  const minutes = Math.floor(seconds / 60);
  const remainder = String(seconds % 60).padStart(2, "0");
  return minutes < 60
    ? `${minutes}:${remainder}`
    : `${Math.floor(minutes / 60)}:${String(minutes % 60).padStart(2, "0")}:${remainder}`;
}
