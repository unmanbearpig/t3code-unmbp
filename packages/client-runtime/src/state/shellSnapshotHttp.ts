import { EnvironmentHttpCommonError, type EnvironmentId } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import { HttpClient, HttpClientError, type HttpClientResponse } from "effect/http";

import * as RemoteEnvironmentAuthorization from "../authorization/service.ts";
import type { PreparedConnection } from "../connection/model.ts";
import * as ManagedRelay from "../relay/managedRelay.ts";
import {
  executeAuthenticatedEnvironmentHttpRequest,
  withOrchestrationProtocolHeader,
} from "./environmentHttpAuth.ts";
import {
  decodeShellSnapshotDeferringPullRequests,
  type DeferredShellSnapshot,
} from "./shellPullRequests.ts";

// Long enough for a slow but alive server to finish. On timeout the socket asks
// the same server for the same full snapshot, so a short deadline only throws
// the first build away. The socket fallback is for setups where /api fails but
// /ws works, such as a proxy that blocks /api. A dead server is caught by the
// socket ping, which drops the session and interrupts this load. The cached
// shell renders while this runs.
const DEFAULT_SHELL_SNAPSHOT_TIMEOUT_MS = 20_000;

/**
 * Load the environment shell snapshot (projects + thread shells) over HTTP
 * instead of as the WebSocket subscription's first frame. The response is
 * gzip-compressible by the transport and keeps the (potentially large) list off
 * the socket.
 */
export const fetchEnvironmentShellSnapshot = Effect.fn(
  "clientRuntime.state.fetchEnvironmentShellSnapshot",
)(function* (input: {
  readonly prepared: PreparedConnection;
  readonly signer: Option.Option<ManagedRelay.ManagedRelayDpopSigner["Service"]>;
  readonly remoteAuthorization?: Option.Option<
    RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization["Service"]
  >;
  readonly timeoutMs?: number;
}) {
  return yield* executeAuthenticatedEnvironmentHttpRequest({
    ...input,
    group: "orchestration",
    method: "GET",
    url: (urls) => urls.shellSnapshot(),
    timeoutMs: input.timeoutMs ?? DEFAULT_SHELL_SNAPSHOT_TIMEOUT_MS,
    // The body is decoded here so each thread's pull request links can decode after its
    // rows. Every declared error is an `EnvironmentHttpCommonError`, so decoding one keeps
    // the error the credential retry checks for.
    request: ({ client, headers }) =>
      client
        .shellSnapshot({
          headers: withOrchestrationProtocolHeader(headers),
          responseMode: "response-only",
        })
        .pipe(Effect.flatMap(decodeShellSnapshotResponse)),
  });
});

const decodeEnvironmentHttpCommonError = Schema.decodeUnknownEffect(
  Schema.toCodecJson(EnvironmentHttpCommonError),
);

const decodeShellSnapshotResponse = (
  response: HttpClientResponse.HttpClientResponse,
): Effect.Effect<
  DeferredShellSnapshot,
  HttpClientError.HttpClientError | Schema.SchemaError | EnvironmentHttpCommonError
> => {
  if (response.status === 200) {
    return response.json.pipe(Effect.flatMap(decodeShellSnapshotDeferringPullRequests));
  }
  // A body that is not a declared error keeps its status, as the generated decoder reported.
  const statusError = new HttpClientError.HttpClientError({
    reason: new HttpClientError.StatusCodeError({ request: response.request, response }),
  });
  return response.json.pipe(
    Effect.flatMap(decodeEnvironmentHttpCommonError),
    Effect.matchEffect({
      onFailure: () => Effect.fail(statusError),
      onSuccess: Effect.fail,
    }),
  );
};

/**
 * Loads the environment shell snapshot over HTTP, returning `Option.none()` when
 * it cannot be loaded (so the caller falls back to the socket-embedded snapshot).
 * Decouples the shell state machine from the underlying HTTP + DPoP details and
 * keeps them out of test contexts.
 */
export class ShellSnapshotLoader extends Context.Service<
  ShellSnapshotLoader,
  {
    readonly load: (
      prepared: PreparedConnection,
    ) => Effect.Effect<Option.Option<DeferredShellSnapshot>>;
    /**
     * Snapshot prefetch integration: register the runtime's reuse candidate.
     * Check shouldPrefetch during socket setup, before publishing its session.
     * A false result skips prefetch; load remains available if config rules out
     * reuse. Missing hooks mean prefetch is allowed for alternative loaders.
     * Registration ends with the shell runtime, so disk caches never opt out.
     */
    readonly registerWarmBase?: (
      environmentId: EnvironmentId,
      isWarm: Effect.Effect<boolean>,
    ) => Effect.Effect<void, never, Scope.Scope>;
    readonly shouldPrefetch?: (environmentId: EnvironmentId) => Effect.Effect<boolean>;
    /** Cancel an already-started prefetch if the base became warm during setup. */
    readonly cancelPrefetch?: (environmentId: EnvironmentId) => Effect.Effect<void>;
  }
>()("@t3tools/client-runtime/state/shellSnapshotHttp/ShellSnapshotLoader") {}

/** @public Service construction is part of the canonical Effect module API. */
export const make = Effect.gen(function* () {
  const httpClient = yield* HttpClient.HttpClient;
  // Resolve the DPoP signer optionally: it is only needed for relay/DPoP
  // connections, so the loader must not hard-require it.
  const signer = yield* Effect.serviceOption(ManagedRelay.ManagedRelayDpopSigner);
  const remoteAuthorization = yield* Effect.serviceOption(
    RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization,
  );
  const warmBases = new Map<EnvironmentId, Effect.Effect<boolean>>();
  return ShellSnapshotLoader.of({
    registerWarmBase: (environmentId, isWarm) =>
      Effect.acquireRelease(
        Effect.sync(() => warmBases.set(environmentId, isWarm)),
        () =>
          Effect.sync(() => {
            if (warmBases.get(environmentId) === isWarm) warmBases.delete(environmentId);
          }),
      ).pipe(Effect.asVoid),
    shouldPrefetch: (environmentId) =>
      Effect.suspend(() => warmBases.get(environmentId) ?? Effect.succeed(false)).pipe(
        Effect.map((isWarm) => !isWarm),
      ),
    load: (prepared: PreparedConnection) =>
      fetchEnvironmentShellSnapshot({ prepared, signer, remoteAuthorization }).pipe(
        Effect.map(Option.some<DeferredShellSnapshot>),
        Effect.provideService(HttpClient.HttpClient, httpClient),
        Effect.catchCause((cause) =>
          Effect.logWarning(
            "Could not load the environment shell snapshot over HTTP; using the socket snapshot instead.",
          ).pipe(
            Effect.annotateLogs({ cause: Cause.pretty(cause) }),
            Effect.as(Option.none<DeferredShellSnapshot>()),
          ),
        ),
      ),
  });
});

export const layer: Layer.Layer<ShellSnapshotLoader, never, HttpClient.HttpClient> = Layer.effect(
  ShellSnapshotLoader,
  make,
);
