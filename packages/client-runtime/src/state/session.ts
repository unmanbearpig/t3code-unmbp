import type { AuthSessionState, EnvironmentId, ServerConfig } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { HttpClient } from "effect/http";
import { AsyncResult, Atom } from "effect/reactivity";

import * as RemoteEnvironmentAuthorization from "../authorization/service.ts";
import * as EnvironmentRegistry from "../connection/registry.ts";
import type { PreparedConnection } from "../connection/model.ts";
import * as EnvironmentSupervisor from "../connection/supervisor.ts";
import * as ManagedRelay from "../relay/managedRelay.ts";
import type { RemoteEnvironmentRequestError } from "../rpc/http.ts";
import { safeErrorLogAttributes } from "../errors/safeLog.ts";
import { executeAuthenticatedEnvironmentHttpRequest } from "./environmentHttpAuth.ts";
import { followStreamInEnvironment } from "./environmentStreams.ts";

/** @public Required to name the error in consumers' inferred session results. */
export class SessionHttpClientUnavailable extends Data.TaggedError(
  "SessionHttpClientUnavailable",
) {}

function initialConfigOption<E>(
  initialConfig: Effect.Effect<ServerConfig, E>,
): Effect.Effect<Option.Option<ServerConfig>> {
  return initialConfig.pipe(
    Effect.asSome,
    Effect.catch((error) =>
      Effect.logWarning("Could not load the initial environment configuration.").pipe(
        Effect.annotateLogs({ ...safeErrorLogAttributes(error) }),
        Effect.as(Option.none<ServerConfig>()),
      ),
    ),
  );
}

// Bounded so a wedged environment cannot pin the permissions check (and with it
// the settings UI) in a loading state for long.
const DEFAULT_SESSION_STATE_TIMEOUT_MS = 6_000;
const SESSION_STATE_RETRY_SCHEDULE = Schedule.exponential("1 second").pipe(
  Schedule.modifyDelay(({ duration }) =>
    Effect.succeed(Duration.min(duration, Duration.seconds(30))),
  ),
);

function isRetryableSessionError(
  error: SessionHttpClientUnavailable | RemoteEnvironmentRequestError,
): boolean {
  switch (error._tag) {
    case "RemoteEnvironmentAuthFetchError":
    case "RemoteEnvironmentAuthTimeoutError":
    case "RemoteEnvironmentAuthInvalidJsonError":
    case "EnvironmentInternalError":
      return true;
    case "RemoteEnvironmentAuthUndeclaredStatusError":
      return error.status === 408 || error.status === 429 || error.status >= 500;
    default:
      return false;
  }
}

/**
 * Read the granted scopes of this client's session on one environment via its
 * `/api/auth/session` endpoint, using the connection's authentication method
 * and refreshing relay credentials when needed.
 */
export const fetchEnvironmentSessionState = Effect.fn(
  "clientRuntime.state.fetchEnvironmentSessionState",
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
    group: "auth",
    method: "GET",
    url: (urls) => urls.session(),
    timeoutMs: input.timeoutMs ?? DEFAULT_SESSION_STATE_TIMEOUT_MS,
    request: ({ client, headers }) => client.session({ headers }),
    // This endpoint returns 200 with authenticated:false for expired credentials.
    isUnauthorizedResponse: (response) => !response.authenticated,
  });
});

function makeEnvironmentSessionAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry.EnvironmentRegistry | R, E>,
) {
  const initialConfigAtom = Atom.family((environmentId: EnvironmentId) =>
    runtime.atom(
      followStreamInEnvironment(
        environmentId,
        Stream.unwrap(
          EnvironmentSupervisor.EnvironmentSupervisor.pipe(
            Effect.map((supervisor) =>
              SubscriptionRef.changes(supervisor.session).pipe(
                Stream.mapEffect(
                  Option.match({
                    onNone: () => Effect.succeed(Option.none<ServerConfig>()),
                    onSome: (session) => initialConfigOption(session.initialConfig),
                  }),
                ),
              ),
            ),
          ),
        ),
      ),
      { initialValue: Option.none() },
    ),
  );

  // This is only the bootstrap config captured when a transport session is
  // established. Consumers that need current provider/settings state must use
  // createServerEnvironmentAtoms(...).configValueAtom instead.
  const initialConfigValueAtom = Atom.family((environmentId: EnvironmentId) =>
    Atom.make((get): ServerConfig | null =>
      Option.getOrNull(
        Option.getOrElse(AsyncResult.value(get(initialConfigAtom(environmentId))), () =>
          Option.none(),
        ),
      ),
    ).pipe(Atom.withLabel(`environment-config-value:${environmentId}`)),
  );

  const preparedConnectionAtom = Atom.family((environmentId: EnvironmentId) =>
    runtime.atom(
      followStreamInEnvironment(
        environmentId,
        Stream.unwrap(
          EnvironmentSupervisor.EnvironmentSupervisor.pipe(
            Effect.map((supervisor) => SubscriptionRef.changes(supervisor.prepared)),
          ),
        ),
      ),
      { initialValue: Option.none<PreparedConnection>() },
    ),
  );

  const preparedConnectionValueAtom = Atom.family((environmentId: EnvironmentId) =>
    Atom.make((get) =>
      Option.getOrElse(AsyncResult.value(get(preparedConnectionAtom(environmentId))), () =>
        Option.none<PreparedConnection>(),
      ),
    ).pipe(Atom.withLabel(`environment-prepared-connection:${environmentId}`)),
  );

  const connectedSessionAtom = Atom.family((environmentId: EnvironmentId) =>
    runtime.atom(
      followStreamInEnvironment(
        environmentId,
        Stream.unwrap(
          EnvironmentSupervisor.EnvironmentSupervisor.pipe(
            Effect.map((supervisor) => SubscriptionRef.changes(supervisor.session)),
          ),
        ),
      ),
      { initialValue: Option.none() },
    ),
  );

  const sessionStateAtom = Atom.family((environmentId: EnvironmentId) => {
    const refreshSignal = Atom.make(() => ({}));
    const preparedValue = preparedConnectionValueAtom(environmentId).pipe(
      Atom.map(Option.getOrNull),
    );
    const sessionValue = connectedSessionAtom(environmentId).pipe(
      Atom.map((result) => Option.getOrNull(Option.flatten(AsyncResult.value(result)))),
    );
    // Each connection owns its cache. Replacing credentials, endpoints or the
    // socket disposes its request and starts without a previous grant.
    const connectionQuery = Atom.make((get) => {
      const prepared = get(preparedValue);
      const session = get(sessionValue);
      if (prepared === null || session === null) return null;
      return runtime
        .atom((get) => {
          get(refreshSignal);
          const previous = get.self<AsyncResult.AsyncResult<AuthSessionState, unknown>>();
          const confirmed = Option.isSome(previous) && previous.value._tag === "Success";
          const fetchSession = Effect.gen(function* () {
            const signer = yield* Effect.serviceOption(ManagedRelay.ManagedRelayDpopSigner);
            const remoteAuthorization = yield* Effect.serviceOption(
              RemoteEnvironmentAuthorization.RemoteEnvironmentAuthorization,
            );
            const client = yield* Effect.serviceOption(HttpClient.HttpClient);
            if (Option.isNone(client)) return yield* new SessionHttpClientUnavailable();
            return yield* fetchEnvironmentSessionState({
              prepared,
              signer,
              remoteAuthorization,
            }).pipe(Effect.provideService(HttpClient.HttpClient, client.value));
          });
          return fetchSession.pipe(
            Effect.tapError((error) =>
              !confirmed && isRetryableSessionError(error)
                ? Effect.sync(() => get.setSelf(AsyncResult.failure(Cause.fail(error)))).pipe(
                    // Publish the failed initial lookup before continuing the retry.
                    Effect.andThen(Effect.promise(() => Promise.resolve())),
                  )
                : Effect.void,
            ),
            Effect.retry({
              while: isRetryableSessionError,
              schedule: SESSION_STATE_RETRY_SCHEDULE,
            }),
          );
        })
        .pipe(Atom.setIdleTTL(0));
    });
    return Atom.readable(
      (get) => {
        const query = get(connectionQuery);
        return query === null ? AsyncResult.initial<AuthSessionState>(true) : get(query);
      },
      (refresh) => refresh(refreshSignal),
    ).pipe(
      // Connection changes must cancel obsolete work even during the idle TTL.
      Atom.setLazy(false),
      Atom.setIdleTTL(5 * 60_000),
      Atom.withLabel(`environment-session-state:${environmentId}`),
    );
  });

  const sessionStateValueAtom = Atom.family((environmentId: EnvironmentId) =>
    Atom.make(
      (get): AuthSessionState | null =>
        Option.getOrNull(AsyncResult.value(get(sessionStateAtom(environmentId)))) ?? null,
    ).pipe(Atom.withLabel(`environment-session-state-value:${environmentId}`)),
  );

  return {
    initialConfigAtom,
    initialConfigValueAtom,
    preparedConnectionAtom,
    preparedConnectionValueAtom,
    sessionStateAtom,
    sessionStateValueAtom,
  };
}

const sessionAtomsByRuntime = new WeakMap<object, ReturnType<typeof makeEnvironmentSessionAtoms>>();

/** Commands and UI share one session fetch and the same reconnect invalidation. */
export function createEnvironmentSessionAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry.EnvironmentRegistry | R, E>,
): ReturnType<typeof makeEnvironmentSessionAtoms<R, E>> {
  const existing = sessionAtomsByRuntime.get(runtime);
  if (existing) return existing as ReturnType<typeof makeEnvironmentSessionAtoms<R, E>>;
  const atoms = makeEnvironmentSessionAtoms(runtime);
  sessionAtomsByRuntime.set(runtime, atoms);
  return atoms;
}
