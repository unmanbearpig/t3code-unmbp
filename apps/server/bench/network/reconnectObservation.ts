import * as Predicate from "effect/Predicate";

/** The fixture has 160 threads. Empty thread arrays identify the normal metadata prefix. */
export function isFullShellSnapshot(value: unknown) {
  if (
    !Predicate.hasProperty(value, "kind") ||
    value.kind !== "snapshot" ||
    !Predicate.hasProperty(value, "snapshot")
  )
    return false;
  const snapshot = value.snapshot;
  return (
    (Predicate.hasProperty(snapshot, "threads") &&
      Array.isArray(snapshot.threads) &&
      snapshot.threads.length > 0) ||
    (Predicate.hasProperty(snapshot, "archivedThreads") &&
      Array.isArray(snapshot.archivedThreads) &&
      snapshot.archivedThreads.length > 0)
  );
}
