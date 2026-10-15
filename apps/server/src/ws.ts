import { ORCHESTRATION_PROTOCOL_VERSION } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import {
  HttpRouter,
  HttpServerRequest,
  HttpServerRespondable,
  HttpServerResponse,
} from "effect/http";
import { RpcSerialization, RpcServer } from "effect/rpc";

import * as AuthenticatedRpcSession from "./AuthenticatedRpcSession.ts";
import * as EnvironmentAuth from "./auth/EnvironmentAuth.ts";
import { failEnvironmentAuthInvalid, failEnvironmentInternal } from "./auth/http.ts";

export const layer = Layer.unwrap(
  Effect.gen(function* () {
    const rpc = yield* AuthenticatedRpcSession.AuthenticatedRpcSession;
    return HttpRouter.add(
      "GET",
      "/ws",
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const url = HttpServerRequest.toURL(request);
        if (
          Option.isNone(url) ||
          !AuthenticatedRpcSession.hasCompatibleOrchestrationProtocol(url.value)
        ) {
          return HttpServerResponse.jsonUnsafe(
            {
              code: "orchestration_protocol_incompatible",
              message: `Update this client to one that supports orchestration protocol ${ORCHESTRATION_PROTOCOL_VERSION}.`,
              orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
            },
            { status: 426 },
          );
        }
        const auth = yield* EnvironmentAuth.EnvironmentAuth;
        const session = yield* auth.authenticateWebSocketUpgrade(request).pipe(
          Effect.catchIf(EnvironmentAuth.isServerAuthCredentialError, (error) =>
            failEnvironmentAuthInvalid(
              EnvironmentAuth.serverAuthCredentialReason(error),
              EnvironmentAuth.serverAuthDpopFailureReason(error),
            ),
          ),
          Effect.catchIf(EnvironmentAuth.isServerAuthInternalError, (error) =>
            failEnvironmentInternal("internal_error", error),
          ),
        );
        const { protocol, httpEffect } = yield* RpcServer.makeProtocolWithHttpEffectWebsocket.pipe(
          Effect.provide(RpcSerialization.layerJson),
        );
        return yield* rpc
          .run({
            session,
            protocol,
            lifetime: httpEffect.pipe(
              Effect.provideService(HttpServerRequest.HttpServerRequest, request),
            ),
            origin: AuthenticatedRpcSession.readClientConnectionOrigin(request),
            analyticsProps: AuthenticatedRpcSession.readClientAnalyticsProps(request),
          })
          .pipe(
            Effect.map((response) => response ?? HttpServerResponse.empty()),
            Effect.catchTags({
              SessionCredentialVerificationError: (error) =>
                failEnvironmentInternal("internal_error", error),
            }),
          );
      }).pipe(
        Effect.catchTags({
          EnvironmentAuthInvalidError: HttpServerRespondable.toResponse,
          EnvironmentInternalError: HttpServerRespondable.toResponse,
        }),
      ),
    );
  }),
);
