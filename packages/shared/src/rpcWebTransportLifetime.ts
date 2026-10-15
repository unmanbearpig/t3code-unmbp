import { transportTry } from "./rpcWebTransportCodec.ts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import {
  RpcWebTransportError,
  type ReliableByteStream,
  type ReliableSession,
} from "./rpcWebTransportCore.ts";

/** The logical lifetime never waits for native close, FIN, reset or stop acknowledgements. */
export const makeRpcWebTransportLifetime = Effect.fnUntraced(function* (session: ReliableSession) {
  const ended = yield* Deferred.make<void>();
  const streams = new Set<ReliableByteStream>();
  let error: RpcWebTransportError | undefined;
  const close = (cause = new RpcWebTransportError("aborted", "RPC transport disposed")) =>
    Effect.gen(function* () {
      if (error) return;
      error = cause;
      yield* Deferred.succeed(ended, undefined);
      for (const stream of streams) {
        try {
          stream.reset(1);
        } catch {
          /* Advisory native teardown. */
        }
        try {
          stream.stop(1);
        } catch {
          /* Advisory native teardown. */
        }
      }
      streams.clear();
      try {
        session.close(cause.category === "aborted" ? 0 : 1);
      } catch {
        /* Advisory native teardown. */
      }
    });
  yield* Effect.addFinalizer(() => close());
  const closed = Deferred.await(ended);
  const failure = closed.pipe(Effect.andThen(Effect.suspend(() => Effect.fail(error!))));
  const race = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.suspend(() => (error ? Effect.fail(error) : effect.pipe(Effect.raceFirst(failure))));
  yield* transportTry(() => session.closed).pipe(
    Effect.flatMap(() => close(new RpcWebTransportError("network", "RPC transport closed"))),
    Effect.catch((cause) => close(cause)),
    Effect.raceFirst(closed),
    Effect.forkScoped,
  );
  return {
    get error() {
      return error;
    },
    close,
    closed,
    failure,
    race,
    own(stream: ReliableByteStream) {
      if (error) {
        try {
          stream.reset(1);
        } catch {
          /* Advisory native teardown. */
        }
        try {
          stream.stop(1);
        } catch {
          /* Advisory native teardown. */
        }
      } else streams.add(stream);
      return stream;
    },
    release(stream: ReliableByteStream) {
      streams.delete(stream);
    },
  };
});
