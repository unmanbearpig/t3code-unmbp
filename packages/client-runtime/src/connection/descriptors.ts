import type { ExecutionEnvironmentDescriptor } from "@t3tools/contracts";
import * as Cache from "effect/Cache";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as HttpClient from "effect/http/HttpClient";

import { fetchRemoteEnvironmentDescriptor } from "../environment/descriptor.ts";
import type { RemoteEnvironmentRequestError } from "../rpc/http.ts";

export class ConnectionDescriptors extends Context.Service<
  ConnectionDescriptors,
  {
    readonly get: (
      httpBaseUrl: string,
    ) => Effect.Effect<ExecutionEnvironmentDescriptor, RemoteEnvironmentRequestError>;
  }
>()("@t3tools/client-runtime/connection/descriptors/ConnectionDescriptors") {}

const make = Effect.gen(function* () {
  const httpClient = yield* HttpClient.HttpClient;
  const descriptors = yield* Cache.make({
    capacity: Number.POSITIVE_INFINITY,
    lookup: (httpBaseUrl: string) =>
      fetchRemoteEnvironmentDescriptor({ httpBaseUrl }).pipe(
        Effect.provideService(HttpClient.HttpClient, httpClient),
      ),
  });
  return ConnectionDescriptors.of({ get: (httpBaseUrl) => Cache.get(descriptors, httpBaseUrl) });
});

const layer = Layer.effect(ConnectionDescriptors, make);

export const fetch = Effect.fn("ConnectionDescriptors.fetch")(function* (httpBaseUrl: string) {
  const descriptors = yield* Effect.serviceOption(ConnectionDescriptors);
  return yield* Option.isSome(descriptors)
    ? descriptors.value.get(httpBaseUrl)
    : fetchRemoteEnvironmentDescriptor({ httpBaseUrl });
});

/** Shares descriptor requests across route checks, authorization, and compatibility in one attempt. */
export const withConnectionDescriptors = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R | HttpClient.HttpClient> =>
  Effect.gen(function* () {
    const current = yield* Effect.serviceOption(ConnectionDescriptors);
    if (Option.isSome(current)) return yield* effect;
    return yield* effect.pipe(Effect.provide(layer));
  });
