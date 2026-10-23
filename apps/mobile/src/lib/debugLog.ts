/**
 * Namespaced, filterable debug logging shared by mobile subsystems.
 *
 * Ordinary, expected conditions — a queued send failing while the device is
 * offline, for example — go through a debug logger instead of `console.warn`
 * so warning output stays reserved for failures someone can act on. Output
 * uses `console.log` with a `[t3-<namespace>]` prefix, matching the existing
 * cloud and terminal debug logs. (client-runtime cannot host this: its
 * tooling bans `console.*` in favor of Effect logging.)
 *
 * Every event is saved to the bounded local Diagnostics log in every build.
 * Toggle console output from a JS debugger or the Metro console:
 * - `globalThis.__T3_DEBUG__ = true` enables every namespace;
 * - `globalThis.__T3_DEBUG__ = ["thread-outbox"]` enables only listed ones.
 *
 * Subsystems whose traces are useful by default in development (`__DEV__`)
 * opt in with `enabledInDev`; `legacyGlobalFlag` keeps an older
 * subsystem-specific global (e.g. `__T3_CLOUD_DEBUG__`) working.
 */

type DebugLogRecorder = (message: string, data?: Record<string, unknown>) => void;
let diagnosticRecorder: DebugLogRecorder | undefined;

/** The native entry point installs persistence before loading app subsystems. */
export function setDebugLogRecorder(recorder: DebugLogRecorder) {
  diagnosticRecorder = recorder;
}

export interface DebugLogger {
  readonly isEnabled: () => boolean;
  readonly log: (event: string, data?: Record<string, unknown>) => void;
}

export interface DebugLoggerOptions {
  readonly enabledInDev?: boolean;
  readonly legacyGlobalFlag?: string;
}

function globalValue(name: string): unknown {
  return typeof globalThis === "undefined"
    ? undefined
    : (globalThis as Record<string, unknown>)[name];
}

export function createDebugLogger(
  namespace: string,
  options: DebugLoggerOptions = {},
): DebugLogger {
  const isEnabled = () => {
    if (options.enabledInDev === true && typeof __DEV__ !== "undefined" && __DEV__) {
      return true;
    }
    if (options.legacyGlobalFlag !== undefined && globalValue(options.legacyGlobalFlag) === true) {
      return true;
    }
    const filter = globalValue("__T3_DEBUG__");
    return filter === true || (Array.isArray(filter) && filter.includes(namespace));
  };
  const log = (event: string, data?: Record<string, unknown>) => {
    if (!isEnabled()) {
      diagnosticRecorder?.(`[t3-${namespace}] ${event}`, data);
      return;
    }
    // Enabled console output is captured by the native diagnostics hook.
    if (data === undefined) {
      console.log(`[t3-${namespace}] ${event}`);
    } else {
      console.log(`[t3-${namespace}] ${event}`, data);
    }
  };
  return { isEnabled, log };
}
