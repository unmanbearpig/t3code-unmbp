// Temporary Android share diagnostics. Release builds forward console.warn to
// logcat under ReactNativeJS: `adb logcat -s ReactNativeJS:W ExpoSharingDebug:D`.
// Remove this file and its call sites once the share bugs are understood.
export function shareDebug(event: string, data?: unknown): void {
  console.warn(`[share-debug] ${event}`, data === undefined ? "" : JSON.stringify(data));
}

export function describeError(error: unknown): unknown {
  if (error instanceof Error) {
    const code = "code" in error ? error.code : undefined;
    return { name: error.name, code, message: error.message };
  }
  return String(error);
}

/** URI shape without the full path, enough to tell content: from file: and the provider. */
export function describeUri(uri: string | null | undefined): string | null {
  if (!uri) return null;
  const match = /^([a-z]+):\/\/([^/]*)(.*)$/i.exec(uri);
  if (!match) return uri.slice(0, 40);
  const [, scheme, authority, path] = match;
  const tail = path?.split("/").findLast((segment) => segment.length > 0) ?? "";
  return `${scheme}://${authority}/…/${tail}`;
}
