import { RpcClientId, type OrchestrationClientOrigin } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import type * as EnvironmentAuth from "./auth/EnvironmentAuth.ts";
import * as SessionStore from "./auth/SessionStore.ts";
import * as BackgroundPolicy from "./background/BackgroundPolicy.ts";
import * as AnalyticsService from "./telemetry/AnalyticsService.ts";

/** Owns connection bookkeeping independently of transport-local RPC client IDs. */
export class RpcSessionLifetime extends Context.Service<
  RpcSessionLifetime,
  {
    readonly run: <A, E, R>(
      session: EnvironmentAuth.AuthenticatedSession,
      origin: OrchestrationClientOrigin,
      analyticsProps: Readonly<Record<string, unknown>>,
      use: (owner: RpcClientId) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A | undefined, E | SessionStore.SessionCredentialVerificationError, R>;
  }
>()("t3/RpcSessionLifetime") {}

const make = Effect.gen(function* () {
  const sessions = yield* SessionStore.SessionStore;
  const background = yield* BackgroundPolicy.BackgroundPolicy;
  const analytics = yield* AnalyticsService.AnalyticsService;
  // This service is acquired once by the application, shared by WSS and QUIC.
  let nextOwner = 1;
  const run: RpcSessionLifetime["Service"]["run"] = (session, origin, props, use) =>
    Effect.scoped(
      Effect.gen(function* () {
        const owner = RpcClientId.make(nextOwner++);
        yield* Effect.acquireRelease(sessions.markConnected(session.sessionId), () =>
          background
            .removeRpcClient(session.sessionId, owner)
            .pipe(Effect.ensuring(sessions.markDisconnected(session.sessionId))),
        );
        yield* sessions.recordClientConnection(session.sessionId, origin);
        yield* analytics.record("client.connected", props);
        // Subscribe/revalidate before the transport starts. A revoke between ticket
        // admission and acceptance must also close the newly accepted session.
        return yield* Effect.raceFirst(
          sessions.awaitInvalidation(session.sessionId).pipe(Effect.as(undefined)),
          use(owner),
        );
      }),
    );
  return RpcSessionLifetime.of({ run });
});

export const layer = Layer.effect(RpcSessionLifetime, make);
