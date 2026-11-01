import type { EnvironmentId } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";

import type { PreparedRpcWebTransport } from "../connection/model.ts";

interface Attempt {
  readonly key: string;
  readonly generation: number;
}

/** Lives in the application-owned RpcSessionFactory, across connection attempts. */
export const makeRpcWebTransportCooldown = Effect.sync(() => {
  const entries = new Map<
    string,
    {
      generation: number;
      failures: number;
      retryAt: number;
      failed: boolean;
    }
  >();
  let generation = 0;
  return {
    begin: (environmentId: EnvironmentId, candidate: PreparedRpcWebTransport) =>
      Clock.currentTimeMillis.pipe(
        Effect.map((now): Attempt | undefined => {
          const key = JSON.stringify([
            environmentId,
            new URL(candidate.url).toString(),
            candidate.wireVersion,
          ]);
          const previous = entries.get(key);
          if (previous !== undefined && previous.retryAt > now) return undefined;
          const state = {
            generation: ++generation,
            failures: previous?.failures ?? 0,
            retryAt: 0,
            failed: false,
          };
          entries.delete(key);
          entries.set(key, state);
          if (entries.size > 64) entries.delete(entries.keys().next().value!);
          return { key, generation: state.generation };
        }),
      ),
    succeeded: (attempt: Attempt) =>
      Effect.sync(() => {
        const current = entries.get(attempt.key);
        if (current?.generation !== attempt.generation) return;
        current.failures = 0;
        current.retryAt = 0;
      }),
    failed: (attempt: Attempt) =>
      Clock.currentTimeMillis.pipe(
        Effect.map((now) => {
          const current = entries.get(attempt.key);
          if (current?.generation !== attempt.generation || current.failed) return;
          current.failed = true;
          current.failures++;
          current.retryAt =
            now + Math.min(30_000 * 2 ** Math.min(current.failures - 1, 4), 300_000);
        }),
      ),
  };
});
