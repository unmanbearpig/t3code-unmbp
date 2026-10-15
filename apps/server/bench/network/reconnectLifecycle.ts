import * as Effect from "effect/Effect";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

/** Own one child scope so the timestamps bracket actual finalizers, including RPC interrupts. */
export const observedScope = <A, E, R>(
  effect: Effect.Effect<A, E, R | Scope.Scope>,
  stamp: (name: string) => void,
  name: string,
) =>
  Effect.gen(function* () {
    const scope = yield* Effect.acquireRelease(Scope.make(), (scope, exit) =>
      Effect.sync(() => stamp(`${name}-start`)).pipe(
        Effect.andThen(Scope.close(scope, exit)),
        Effect.ensuring(Effect.sync(() => stamp(`${name}-end`))),
      ),
    );
    return yield* effect.pipe(Scope.provide(scope));
  });

export const observedStream = <A, E, R>(
  stream: Stream.Stream<A, E, R>,
  stamp: (name: string) => void,
  name: string,
) =>
  Stream.unwrap(
    Effect.gen(function* () {
      const scope = yield* Scope.make();
      const pull = yield* Stream.toPull(stream).pipe(Scope.provide(scope));
      return Stream.fromPull(Effect.succeed(pull)).pipe(
        Stream.onExit((exit) =>
          Effect.sync(() => stamp(`${name}-start`)).pipe(
            Effect.andThen(Scope.close(scope, exit)),
            Effect.ensuring(Effect.sync(() => stamp(`${name}-end`))),
          ),
        ),
      );
    }),
  );
