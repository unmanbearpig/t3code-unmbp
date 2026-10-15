import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";

/** A request-owned prefix; its completion item shares the data's RPC chunk. */
export function rpcInitialItems<A, B = never>(
  items: ReadonlyArray<A>,
  completion?: B,
): Stream.Stream<A | B> {
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
