import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";

/** Advance wall time for background cache expiry while preserving real scheduling. */
export function backgroundClock(base: Clock.Clock, offsetMs: () => number): Clock.Clock {
  return {
    currentTimeMillisUnsafe: () => base.currentTimeMillisUnsafe() + offsetMs(),
    currentTimeMillis: Effect.sync(() => base.currentTimeMillisUnsafe() + offsetMs()),
    currentTimeNanosUnsafe: () => base.currentTimeNanosUnsafe() + BigInt(offsetMs()) * 1000000n,
    currentTimeNanos: Effect.sync(
      () => base.currentTimeNanosUnsafe() + BigInt(offsetMs()) * 1000000n,
    ),
    monotonicTimeNanosUnsafe: () => base.monotonicTimeNanosUnsafe(),
    monotonicTimeNanos: base.monotonicTimeNanos,
    sleep: (duration) => base.sleep(duration),
  };
}
