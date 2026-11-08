import { ProviderThreadId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import { describe, expect, it } from "vite-plus/test";

import { deriveLastPromptCacheWriteAt, formatPromptCacheElapsed } from "./promptCache.ts";

const activeId = ProviderThreadId.make("active");
const writtenAt = "2026-10-14T12:00:00.000Z";

describe("deriveLastPromptCacheWriteAt", () => {
  it("retains the last write across newer reads and unordered provider turns", () => {
    const usage = (updatedAt: string, lastCacheWriteAt?: string) => ({
      providerThreadId: activeId,
      tokenUsage: { usedTokens: 100, updatedAt, lastCacheWriteAt },
    });
    expect(
      deriveLastPromptCacheWriteAt({
        thread: { activeProviderThreadId: activeId },
        providerTurns: [
          usage("2026-10-14T12:02:00.000Z"),
          usage("2026-10-14T12:01:00.000Z", writtenAt),
          usage("2026-10-14T11:00:00.000Z", "2026-10-14T11:00:00.000Z"),
        ],
      }),
    ).toBe(Date.parse(writtenAt));
  });

  it("does not carry a different provider thread's writes across a switch", () => {
    expect(
      deriveLastPromptCacheWriteAt({
        thread: { activeProviderThreadId: activeId },
        providerTurns: [
          {
            providerThreadId: ProviderThreadId.make("other"),
            tokenUsage: { usedTokens: 100, updatedAt: writtenAt, lastCacheWriteAt: writtenAt },
          },
        ],
      }),
    ).toBeNull();
  });

  it("recognizes historical Claude write reports without using the TTL as an expiry", () => {
    expect(
      deriveLastPromptCacheWriteAt({
        thread: { activeProviderThreadId: activeId },
        providerTurns: [
          {
            providerThreadId: activeId,
            tokenUsage: { usedTokens: 100, updatedAt: writtenAt, promptCacheTtlMs: 300_000 },
          },
        ],
      }),
    ).toBe(Date.parse(writtenAt));
  });

  it("keeps missing and invalid write timestamps unknown", () => {
    expect(deriveLastPromptCacheWriteAt(null)).toBeNull();
    expect(
      deriveLastPromptCacheWriteAt({
        thread: { activeProviderThreadId: activeId },
        providerTurns: [
          {
            providerThreadId: activeId,
            tokenUsage: { usedTokens: 100, updatedAt: writtenAt, lastCacheWriteAt: "invalid" },
          },
        ],
      }),
    ).toBeNull();
  });

  it("uses completion time for providers that report writes only in turn totals", () => {
    expect(
      deriveLastPromptCacheWriteAt({
        thread: { activeProviderThreadId: activeId },
        providerTurns: [
          {
            providerThreadId: activeId,
            completedAt: DateTime.makeUnsafe(writtenAt),
            turnTokenUsage: {
              usageScope: "main_agent",
              usageStatus: "complete",
              hasSubagents: false,
              inputTokens: 100,
              outputTokens: 0,
              cacheCreationTokens: 100,
            },
          },
        ],
      }),
    ).toBe(Date.parse(writtenAt));
  });
});

it.each([
  [-1_000, "0:00"],
  [0, "0:00"],
  [999, "0:00"],
  [1_000, "0:01"],
  [61_000, "1:01"],
  [3_599_000, "59:59"],
  [3_661_000, "1:01:01"],
  [25 * 3_600_000, "25:00:00"],
])("formats %dms since a cache write as %s", (elapsed, expected) => {
  expect(formatPromptCacheElapsed(Date.parse(writtenAt), Date.parse(writtenAt) + elapsed)).toBe(
    expected,
  );
});
