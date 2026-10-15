import { expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import { HttpClient, HttpClientResponse } from "effect/http";
import { observeHttpClient } from "./reconnectHttp.ts";
import type { Stage } from "./reconnectClient.ts";
import { operationServerConfig } from "./operationsFixture.ts";
import { fetchRemoteEnvironmentDescriptor } from "../../../../packages/client-runtime/src/environment/descriptor.ts";

it.effect(
  "records the real HTTP API decoder consuming arrayBuffer without changing its result",
  () =>
    Effect.gen(function* () {
      const stages: Stage[] = [];
      const requests: Record<string, number> = {};
      let time = 0;
      const client = HttpClient.make((request) =>
        Effect.succeed(
          HttpClientResponse.fromWeb(request, Response.json(operationServerConfig.environment)),
        ),
      );
      const measured = observeHttpClient(client, stages, requests, () => ++time);
      const descriptor = yield* fetchRemoteEnvironmentDescriptor({
        httpBaseUrl: "http://bench.invalid",
      }).pipe(Effect.provideService(HttpClient.HttpClient, measured));
      expect(descriptor.environmentId).toBe(operationServerConfig.environment.environmentId);
      expect(requests).toEqual({ "/.well-known/t3/environment": 1 });
      expect(stages).toEqual([{ name: "descriptor-1", atMs: 1, endMs: 2, outcome: "succeeded" }]);
    }),
);

it.effect("retains route-check timing when its descriptor request times out before headers", () =>
  Effect.gen(function* () {
    const stages: Stage[] = [];
    const requests: Record<string, number> = {};
    const started = yield* Deferred.make<void>();
    const client = HttpClient.make(() =>
      Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
    );
    let time = 0;
    const measured = observeHttpClient(client, stages, requests, () => ++time);
    const pending = yield* fetchRemoteEnvironmentDescriptor({
      httpBaseUrl: "http://bench.invalid",
      timeoutMs: 2500,
    }).pipe(
      Effect.withSpan("ConnectionDriver.checkRoute"),
      Effect.provideService(HttpClient.HttpClient, measured),
      Effect.result,
      Effect.forkChild,
    );
    yield* Deferred.await(started);
    yield* TestClock.adjust(2500);
    const result = yield* Fiber.join(pending);
    expect(result._tag).toBe("Failure");
    expect(requests).toEqual({ "/.well-known/t3/environment": 1 });
    expect(stages).toEqual([
      { name: "descriptor-1", atMs: 1, endMs: 2, outcome: "failed" },
      { name: "route-check-1", atMs: 1, endMs: 2, outcome: "failed" },
    ]);
  }),
);
