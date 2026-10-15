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
      requests[path] = (requests[path] ?? 0) + 1;
      const descriptorIndex = path === "/.well-known/t3/environment" ? ++descriptors : 0;
      const name = descriptorIndex
        ? `descriptor-${descriptorIndex}`
        : path === "/api/auth/websocket-ticket"
          ? "ticket"
          : `http:${path}`;
      const observation: {
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
          const arrayBuffer = response.arrayBuffer.pipe(
            Effect.onExit((exit) =>
              Effect.sync(() => finish(exit._tag === "Success" ? "succeeded" : "failed")),
            ),
          );
          return new Proxy(response, {
            get: (target, key, receiver) =>
              key === "arrayBuffer" ? arrayBuffer : Reflect.get(target, key, receiver),
          });
        }),
        Effect.onExit((exit) =>
          exit._tag === "Failure" ? Effect.sync(() => finish("failed")) : Effect.void,
        ),
      );
    }),
  );
}
