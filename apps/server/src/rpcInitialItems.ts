import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";

/** A request-owned prefix; legacy pacing keeps completion behind the data acknowledgement. */
export function rpcInitialItems<A, B = never>(
  items: ReadonlyArray<A>,
  completion?: B,
  legacyPacing = false,
): Stream.Stream<A | B> {
  if (legacyPacing && completion !== undefined) {
    return Stream.concat(rpcInitialItems(items), Stream.make(completion));
  }
  let pending: ReadonlyArray<A | B> | undefined =
    completion === undefined ? items : [...items, completion];
  return Stream.unwrap(
    Effect.sync(() => {
      const initial = pending;
      // concat keeps its initial stream definition alive during the live tail.
      // That definition must no longer own delivered snapshots or replay rows.
      pending = undefined;
      return initial === undefined ? Stream.empty : Stream.fromIterable(initial);
    }),
  );
}
