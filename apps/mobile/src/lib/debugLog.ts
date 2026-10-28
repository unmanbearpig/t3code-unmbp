/** Stores bounded diagnostic entries; __T3_DEBUG__ also enables console output. */

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

function globalValue(name: string): unknown {
  return typeof globalThis === "undefined"
    ? undefined
    : (globalThis as Record<string, unknown>)[name];
}

export function createDebugLogger(namespace: string): DebugLogger {
  const isEnabled = () => {
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
