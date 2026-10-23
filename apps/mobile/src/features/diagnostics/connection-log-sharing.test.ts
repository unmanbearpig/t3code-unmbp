import { beforeEach, describe, expect, it, vi } from "vite-plus/test";

const native = vi.hoisted(() => ({
  files: new Map<string, string>(),
  write: vi.fn<() => Promise<void>>(),
  share: vi.fn<(uri: string) => Promise<void>>(),
}));

vi.mock("expo-file-system", () => ({
  Paths: { cache: "file:///cache" },
  File: class {
    readonly uri: string;

    constructor(cache: string, name: string) {
      this.uri = `${cache}/${name}`;
    }

    async write(content: string): Promise<void> {
      await native.write();
      native.files.set(this.uri, content);
    }
  },
}));

vi.mock("expo-sharing", () => ({ shareAsync: native.share }));

import { shareConnectionLog } from "./connection-log-sharing";

const URI = "file:///cache/t3-mobile-connections.json";
const FIRST_REPORT = '{"records":[{"id":"first"}]}';
const NEXT_REPORT = '{"records":[{"id":"first"},{"id":"next"}]}';

beforeEach(() => {
  native.files.clear();
  native.write.mockReset().mockResolvedValue(undefined);
  native.share.mockReset().mockResolvedValue(undefined);
});

describe("shareConnectionLog", () => {
  it("finishes writing before opening the chooser and shares fresh bytes after cancellation", async () => {
    const firstWrite = Promise.withResolvers<void>();
    const chooserOpened = Promise.withResolvers<void>();
    const canceledChooser = Promise.withResolvers<void>();
    const sharedReports: Array<string | undefined> = [];
    native.write.mockReturnValueOnce(firstWrite.promise);
    native.share.mockImplementationOnce(async (uri) => {
      sharedReports.push(native.files.get(uri));
      chooserOpened.resolve();
      // Android resolves shareAsync without a result when the chooser is canceled.
      await canceledChooser.promise;
    });

    const firstShare = shareConnectionLog(FIRST_REPORT);
    const sharesBeforeWrite = native.share.mock.calls.length;
    firstWrite.resolve();
    await chooserOpened.promise;
    canceledChooser.resolve();
    await firstShare;
    expect(sharesBeforeWrite).toBe(0);
    expect(sharedReports).toEqual([FIRST_REPORT]);
    expect(native.files.get(URI)).toBe(FIRST_REPORT);

    const nextWrite = Promise.withResolvers<void>();
    native.write.mockReturnValueOnce(nextWrite.promise);
    native.share.mockImplementationOnce(async (uri) => {
      expect(native.files.get(uri)).toBe(NEXT_REPORT);
    });
    const nextShare = shareConnectionLog(NEXT_REPORT);
    expect(native.share).toHaveBeenCalledTimes(1);
    nextWrite.resolve();
    await nextShare;
    // Receivers may open the shared URI after shareAsync has resolved.
    expect(native.files.get(URI)).toBe(NEXT_REPORT);
    expect(native.share).toHaveBeenCalledTimes(2);
  });

  it("reports an asynchronous write failure without opening the chooser and allows retry", async () => {
    const failure = new Error("Cache write failed");
    native.write.mockRejectedValueOnce(failure);

    await expect(shareConnectionLog(FIRST_REPORT)).rejects.toBe(failure);
    expect(native.share).not.toHaveBeenCalled();
    expect(native.files.has(URI)).toBe(false);

    await shareConnectionLog(NEXT_REPORT);
    expect(native.files.get(URI)).toBe(NEXT_REPORT);
    expect(native.share).toHaveBeenCalledTimes(1);
  });

  it("preserves a native share failure and rewrites the retained file on retry", async () => {
    const failure = new Error("Chooser launch failed");
    native.share.mockRejectedValueOnce(failure);

    await expect(shareConnectionLog(FIRST_REPORT)).rejects.toBe(failure);
    expect(native.files.get(URI)).toBe(FIRST_REPORT);

    await shareConnectionLog(NEXT_REPORT);
    expect(native.files.get(URI)).toBe(NEXT_REPORT);
  });
});
