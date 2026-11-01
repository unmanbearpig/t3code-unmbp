import { expect, it } from "vite-plus/test";

import { openCode2ProviderTurnTokenUsage } from "./adapter.ts";

it("retains cache write time through read-only steps", () => {
  const writtenAt = "2026-10-14T12:00:00.000Z";
  const tokens = { input: 10, output: 5, reasoning: 2, cache: { read: 3, write: 4 } };
  const write = openCode2ProviderTurnTokenUsage(tokens, 200_000, writtenAt);
  expect(write).toMatchObject({
    usedTokens: 22,
    inputTokens: 17,
    cachedInputTokens: 3,
    lastCacheWriteAt: writtenAt,
  });
  const read = openCode2ProviderTurnTokenUsage(
    { ...tokens, cache: { read: 10, write: 0 } },
    200_000,
    "2026-10-14T12:01:00.000Z",
    write.lastCacheWriteAt,
  );
  expect(read.lastCacheWriteAt).toBe(writtenAt);
  expect(
    openCode2ProviderTurnTokenUsage({ ...tokens, cache: { read: 10, write: 0 } }, null, writtenAt)
      .lastCacheWriteAt,
  ).toBeUndefined();
});
