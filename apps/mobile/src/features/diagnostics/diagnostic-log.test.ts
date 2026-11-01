import * as NodeSqlite from "node:sqlite";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

const mocks = vi.hoisted(() => ({ openDatabaseSync: vi.fn() }));
vi.mock("expo-sqlite", () => mocks);

let sqlite: NodeSqlite.DatabaseSync;
let log: typeof import("./diagnostic-log");
let uninstall: (() => void) | undefined;

beforeEach(async () => {
  vi.resetModules();
  vi.useFakeTimers();
  vi.setSystemTime(0);
  sqlite = new NodeSqlite.DatabaseSync(":memory:");
  mocks.openDatabaseSync.mockReset().mockImplementation(() => ({
    execSync: (sql: string) => sqlite.exec(sql),
    runSync: (sql: string, ...args: Array<string | number>) => sqlite.prepare(sql).run(...args),
    getAllSync: (sql: string, ...args: Array<string | number>) => sqlite.prepare(sql).all(...args),
    withTransactionSync: (run: () => void) => {
      sqlite.exec("BEGIN");
      try {
        run();
        sqlite.exec("COMMIT");
      } catch (error) {
        sqlite.exec("ROLLBACK");
        throw error;
      }
    },
  }));
  log = await import("./diagnostic-log");
});

afterEach(() => {
  uninstall?.();
  uninstall = undefined;
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.useRealTimers();
  sqlite.close();
});

describe("persistent mobile diagnostics", () => {
  it("batches activity and restores it after reloading the logger", async () => {
    log.recordDiagnosticLog("info", "App started");
    expect(mocks.openDatabaseSync).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1_000);
    vi.resetModules();
    const restarted = await import("./diagnostic-log");
    expect(restarted.readDiagnosticLog()).toEqual([
      { timestamp: 0, level: "info", message: "App started" },
    ]);
  });

  it("bounds retention by count, message size, and age", () => {
    log.recordDiagnosticLog("error", "old");
    vi.advanceTimersByTime(8 * 24 * 60 * 60 * 1_000);
    for (let index = 0; index < 510; index++) {
      log.recordDiagnosticLog("debug", `event ${index}`);
    }
    log.recordDiagnosticLog("error", "x".repeat(20_000));
    const entries = log.readDiagnosticLog();
    expect(entries).toHaveLength(500);
    expect(entries[0]?.message).toHaveLength(8_000);
    expect(entries.at(-1)?.message).toBe("event 11");
    expect(entries.some((entry) => entry.message === "old")).toBe(false);
  });

  it("persists errors before passing them to React Native's fatal handler", () => {
    let handler: (error: unknown, fatal?: boolean) => void = () => undefined;
    const original = vi.fn(() => {
      expect(
        sqlite.prepare("SELECT message FROM diagnostic_log WHERE level = 'fatal'").get(),
      ).toMatchObject({ message: expect.stringContaining("launch failed") });
    });
    vi.stubGlobal("ErrorUtils", {
      getGlobalHandler: () => original,
      setGlobalHandler: (next: typeof handler) => {
        handler = next;
      },
    });
    uninstall = log.installDiagnosticLog();
    const error = new Error("launch failed");
    handler(error, true);
    expect(original).toHaveBeenCalledWith(error, true);
  });

  it("keeps warning output and never blocks the error handler on storage failure", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const original = vi.fn();
    let handler = original as (error: unknown, fatal?: boolean) => void;
    vi.stubGlobal("ErrorUtils", {
      getGlobalHandler: () => original,
      setGlobalHandler: (next: typeof handler) => {
        handler = next;
      },
    });
    mocks.openDatabaseSync.mockImplementation(() => {
      throw new Error("disk full");
    });
    uninstall = log.installDiagnosticLog();
    console.warn("connection interrupted");
    expect(warn).toHaveBeenCalledWith("connection interrupted");
    const error = new Error("fatal failure");
    expect(() => handler(error, true)).not.toThrow();
    expect(original).toHaveBeenCalledWith(error, true);
  });

  it("captures debug activity without requiring the console flag", async () => {
    vi.stubGlobal("__DEV__", false);
    vi.stubGlobal("__T3_DEBUG__", undefined);
    const output = vi.spyOn(console, "log").mockImplementation(() => undefined);
    const { createDebugLogger, setDebugLogRecorder } = await import("../../lib/debugLog");
    setDebugLogRecorder((message, data) => log.recordDiagnosticLog("debug", message, data));
    const logger = createDebugLogger("thread-outbox");
    logger.log("queued send", { token: "private-token", count: 1 });
    expect(logger.isEnabled()).toBe(false);
    expect(output).not.toHaveBeenCalled();
    expect(log.readDiagnosticLog()[0]?.message).toBe(
      '[t3-thread-outbox] queued send {"token":"[redacted]","count":1}',
    );
  });

  it("handles circular values, removes URL credentials, and clears pending and saved entries", () => {
    const data: Record<string, unknown> = { url: "https://host/path?token=private&safe=1" };
    data.self = data;
    log.recordDiagnosticLog("warn", "failed", data);
    const report = log.formatDiagnosticLog(log.readDiagnosticLog());
    expect(report).toContain("[warn] failed");
    expect(report).toContain("[circular]");
    expect(report).not.toContain("private");
    log.recordDiagnosticLog("info", "pending");
    log.clearDiagnosticLog();
    vi.advanceTimersByTime(1_000);
    expect(log.readDiagnosticLog()).toEqual([]);
  });

  it("captures console activity and enabled debug output once, while preserving output", async () => {
    vi.stubGlobal("__DEV__", false);
    vi.stubGlobal("__T3_DEBUG__", true);
    const output = vi.spyOn(console, "log").mockImplementation(() => undefined);
    const { createDebugLogger, setDebugLogRecorder } = await import("../../lib/debugLog");
    setDebugLogRecorder((message, data) => log.recordDiagnosticLog("debug", message, data));
    uninstall = log.installDiagnosticLog();
    console.log("connection state changed", { state: "connected" });
    createDebugLogger("cloud").log("relay connected");
    const entries = log.readDiagnosticLog();
    expect(entries).toHaveLength(2);
    expect(entries[0]?.message).toBe("[t3-cloud] relay connected");
    expect(entries[1]?.message).toContain('connection state changed {"state":"connected"}');
    expect(output).toHaveBeenCalledTimes(2);
  });
});
