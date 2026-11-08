import { openDatabaseSync, type SQLiteDatabase } from "expo-sqlite";

export interface DiagnosticLogEntry {
  readonly timestamp: number;
  readonly level: string;
  readonly message: string;
}

const MAX_ENTRIES = 500;
const MAX_MESSAGE_LENGTH = 8_000;
const LOG_WINDOW_MS = 7 * 24 * 60 * 60 * 1000;
let database: SQLiteDatabase | undefined;
let pending: DiagnosticLogEntry[] = [];
let flushTimer: ReturnType<typeof setTimeout> | undefined;

function getDatabase() {
  if (database) return database;
  const db = openDatabaseSync("diagnostics.sqlite");
  db.execSync(`
    PRAGMA journal_mode = WAL;
    CREATE TABLE IF NOT EXISTS diagnostic_log (
      id INTEGER PRIMARY KEY,
      timestamp INTEGER NOT NULL,
      level TEXT NOT NULL,
      message TEXT NOT NULL
    );
  `);
  database = db;
  return db;
}

function formatValues(values: ReadonlyArray<unknown>) {
  const seen = new WeakSet<object>();
  try {
    return values
      .map((value) => {
        if (typeof value === "string") return value;
        if (value instanceof Error) return value.stack ?? value.message;
        return JSON.stringify(value, (key, item: unknown) => {
          if (/token|secret|password|authorization|cookie/i.test(key)) return "[redacted]";
          if (item instanceof Error) return item.stack ?? item.message;
          if (typeof item === "object" && item !== null) {
            if (seen.has(item)) return "[circular]";
            seen.add(item);
          }
          return item;
        });
      })
      .join(" ")
      .replace(
        /([?&](?:[^=&\s]*(?:token|secret|password|code|state)[^=&\s]*)=)[^&\s"'<>]+/gi,
        "$1[redacted]",
      )
      .slice(0, MAX_MESSAGE_LENGTH);
  } catch {
    return "[unserializable log entry]";
  }
}

/** Activity is batched; errors flush synchronously before a fatal handler can exit. */
export function recordDiagnosticLog(level: string, ...values: ReadonlyArray<unknown>) {
  pending.push({ timestamp: Date.now(), level, message: formatValues(values) });
  if (pending.length > MAX_ENTRIES) pending = pending.slice(-MAX_ENTRIES);
  if (level === "error" || level === "fatal" || pending.length >= 50) {
    flushDiagnosticLog();
  } else if (flushTimer === undefined) {
    flushTimer = setTimeout(flushDiagnosticLog, 1_000);
  }
}

export function flushDiagnosticLog() {
  clearTimeout(flushTimer);
  flushTimer = undefined;
  if (pending.length === 0) return;
  try {
    const db = getDatabase();
    db.withTransactionSync(() => {
      for (const entry of pending) {
        db.runSync(
          "INSERT INTO diagnostic_log (timestamp, level, message) VALUES (?, ?, ?)",
          entry.timestamp,
          entry.level,
          entry.message,
        );
      }
      db.runSync("DELETE FROM diagnostic_log WHERE timestamp < ?", Date.now() - LOG_WINDOW_MS);
      db.execSync(`DELETE FROM diagnostic_log WHERE id NOT IN (
        SELECT id FROM diagnostic_log ORDER BY id DESC LIMIT ${MAX_ENTRIES}
      )`);
    });
    pending = [];
  } catch {
    // Logging must never interrupt the app or recursively log storage failures.
    // Keep the bounded pending entries for the next write or report read.
  }
}

export function readDiagnosticLog(): ReadonlyArray<DiagnosticLogEntry> {
  flushDiagnosticLog();
  return [
    ...pending,
    ...getDatabase().getAllSync<DiagnosticLogEntry>(
      "SELECT timestamp, level, message FROM diagnostic_log WHERE timestamp >= ? ORDER BY id DESC",
      Date.now() - LOG_WINDOW_MS,
    ),
  ]
    .sort((left, right) => right.timestamp - left.timestamp)
    .slice(0, MAX_ENTRIES);
}

export function clearDiagnosticLog() {
  getDatabase().execSync("DELETE FROM diagnostic_log");
  pending = [];
  clearTimeout(flushTimer);
  flushTimer = undefined;
}

export function formatDiagnosticLog(entries: ReadonlyArray<DiagnosticLogEntry>): string {
  return entries
    .map((entry) => `${new Date(entry.timestamp).toISOString()} [${entry.level}] ${entry.message}`)
    .join("\n");
}

/** Preserve React Native's error handling and console output while recording locally. */
export function installDiagnosticLog() {
  const restoreConsole = (["debug", "log", "info", "warn", "error"] as const).map((level) => {
    const original = console[level];
    console[level] = (...values: unknown[]) => {
      recordDiagnosticLog(level, ...values);
      original(...values);
    };
    return () => {
      console[level] = original;
    };
  });
  const originalHandler =
    typeof ErrorUtils === "undefined" ? undefined : ErrorUtils.getGlobalHandler();
  if (originalHandler) {
    ErrorUtils.setGlobalHandler((error: unknown, isFatal?: boolean) => {
      recordDiagnosticLog(isFatal ? "fatal" : "error", error);
      originalHandler(error, isFatal);
    });
  }
  return () => {
    for (const restore of restoreConsole) restore();
    if (originalHandler) ErrorUtils.setGlobalHandler(originalHandler);
  };
}
