// @effect-diagnostics globalTimers:off - Let Node fetch release a consumed response before a subsequent request.
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as HttpClient from "effect/http/HttpClient";
import type { Stage } from "./reconnectClient.ts";

/** Observe the bytes consumed by HttpApiClient, including requests canceled by route deadlines. */
export function observeHttpClient(
  client: HttpClient.HttpClient,
  stages: Stage[],
  requests: Record<string, number>,
  elapsed: () => number,
  onRequest: () => void = () => {},
) {
  let descriptors = 0;
  return HttpClient.transform(client, (effect, request) =>
    Effect.gen(function* () {
      const parent = yield* Effect.currentParentSpan.pipe(Effect.option);
      let span = Option.getOrUndefined(parent);
      let routeCheck = false;
      while (span) {
        if (span._tag !== "Span") break;
        if (span.name === "ConnectionDriver.checkRoute") routeCheck = true;
        span = Option.getOrUndefined(span.parent);
      }
      const path = new URL(request.url).pathname;
      onRequest();
      requests[path] = (requests[path] ?? 0) + 1;
      const descriptorIndex = path === "/.well-known/t3/environment" ? ++descriptors : 0;
      const name = descriptorIndex
        ? `descriptor-${descriptorIndex}`
        : path === "/api/auth/websocket-ticket"
          ? "ticket"
          : `http:${path}`;
      const observation: {
        bytes?: number;
        name: string;
        atMs: number;
        endMs?: number;
        outcome?: "succeeded" | "failed";
      } = { name, atMs: elapsed() };
      stages.push(observation);
      const routeObservation = routeCheck
        ? { ...observation, name: `route-check-${descriptorIndex}` }
        : undefined;
      if (routeObservation) stages.push(routeObservation);
      const finish = (outcome: "succeeded" | "failed") => {
        if (observation.endMs !== undefined) return;
        Object.assign(observation, { endMs: elapsed(), outcome });
        if (routeObservation)
          Object.assign(routeObservation, { endMs: observation.endMs, outcome });
      };
      return yield* effect.pipe(
        Effect.map((response) => {
          // Undici otherwise dispatches the next microtask onto a new TCP connection,
          // even though this response is fully consumed. One turn makes its idle socket reusable.
          const released = Effect.promise(
            () => new Promise<void>((resolve) => setImmediate(resolve)),
          );
          const arrayBuffer = response.arrayBuffer.pipe(
            Effect.tap(() => released),
            Effect.tap((body) =>
              Effect.sync(() => {
                observation.bytes = body.byteLength;
              }),
            ),
            Effect.onExit((exit) =>
              Effect.sync(() => finish(exit._tag === "Success" ? "succeeded" : "failed")),
            ),
          );
          const text = response.text.pipe(
            Effect.tap(() => released),
            Effect.tap((body) =>
              Effect.sync(() => {
                observation.bytes = new TextEncoder().encode(body).byteLength;
              }),
            ),
            Effect.onExit((exit) =>
              Effect.sync(() => finish(exit._tag === "Success" ? "succeeded" : "failed")),
            ),
          );
          const json = text.pipe(Effect.map((body) => JSON.parse(body) as unknown));
          return new Proxy(response, {
            get: (target, key, receiver) =>
              key === "arrayBuffer"
                ? arrayBuffer
                : key === "text"
                  ? text
                  : key === "json"
                    ? json
                    : Reflect.get(target, key, receiver),
          });
        }),
        Effect.onExit((exit) =>
          exit._tag === "Failure" ? Effect.sync(() => finish("failed")) : Effect.void,
        ),
      );
    }),
  );
}
