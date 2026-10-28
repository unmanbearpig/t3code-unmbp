import { AttemptReconnectArm } from "./reconnectExperiment.ts";
import * as Cause from "effect/Cause";
import * as Diagnostics from "./diagnostics.ts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";

import type { ConnectionCatalogEntry, ConnectionRoute } from "./catalog.ts";
import type {
  ConnectionAttemptError,
  ConnectionAttemptStage,
  PreparedConnection,
} from "./model.ts";
import { ConnectionTransientError } from "./model.ts";
import * as ConnectionResolver from "./resolver.ts";
import {
  connectionRoutes,
  routeEntry,
  routeHttpBaseUrl,
  sendsTokenInClear,
  usableRoutes,
} from "./routes.ts";
import * as ConnectionDescriptors from "./descriptors.ts";
import * as RpcSession from "../rpc/session.ts";
import { fetchRemoteEnvironmentDescriptor } from "../environment/descriptor.ts";

export type ConnectionDriverProgress =
  | {
      readonly stage: "preparing";
    }
  | {
      readonly stage: Exclude<ConnectionAttemptStage, "preparing">;
      readonly prepared: PreparedConnection;
    };

export interface EnvironmentConnectionLease {
  readonly prepared: PreparedConnection;
  readonly session: RpcSession.RpcSession;
}

export type RouteCheck = "answered" | "silent" | "unchecked";

const ROUTE_CHECK_TIMEOUT_MS = 2_500;
// Preferred direct routes share one head start, so a relay connection does not
// wait the full probe timeout for a LAN the device has already left. A route
// that misses it connects over relay, then upgrades with a second reconnect.
const ROUTE_CHECK_HEAD_START_MS = 500;

export class ConnectionDriver extends Context.Service<
  ConnectionDriver,
  {
    readonly connect: (
      entry: ConnectionCatalogEntry,
      reportProgress: (progress: ConnectionDriverProgress) => Effect.Effect<void>,
    ) => Effect.Effect<EnvironmentConnectionLease, ConnectionAttemptError, Scope.Scope>;
    readonly preflight: (
      entry: ConnectionCatalogEntry,
      route: ConnectionRoute,
    ) => Effect.Effect<boolean>;
    /** Asks the host behind a live session to answer a fresh request on the session's own origin. */
    readonly checkHost: (prepared: PreparedConnection) => Effect.Effect<RouteCheck>;
  }
>()("@t3tools/client-runtime/connection/driver/ConnectionDriver") {}

export const connectOverRoutes = Effect.fn("ConnectionDriver.connectOverRoutes")(function* <R>(
  entry: ConnectionCatalogEntry,
  checkRoute: (route: ConnectionRoute) => Effect.Effect<RouteCheck>,
  connectRoute: (
    route: ConnectionRoute,
  ) => Effect.Effect<EnvironmentConnectionLease, ConnectionAttemptError, R | Scope.Scope>,
) {
  const routes = usableRoutes(entry);
  const checks =
    routes.length === 1
      ? []
      : yield* Effect.forEach(routes, (route) => Effect.forkChild(checkRoute(route)));
  const headStart = yield* Effect.forkChild(Effect.sleep(ROUTE_CHECK_HEAD_START_MS));
  const attemptScope = yield* Scope.Scope;
  let transient: ConnectionAttemptError | undefined;
  let blocked: ConnectionAttemptError | undefined;
  // Each route gets its own scope so a half-open session closes before the next try.
  const attempt = Effect.fnUntraced(function* (route: ConnectionRoute) {
    const routeScope = yield* Scope.fork(attemptScope);
    const result = yield* connectRoute(route).pipe(
      Scope.provide(routeScope),
      Effect.onExit((exit) => (Exit.isSuccess(exit) ? Effect.void : Scope.close(routeScope, exit))),
      Effect.result,
    );
    if (result._tag === "Failure") {
      if (result.failure._tag === "ConnectionTransientError") transient ??= result.failure;
      else blocked ??= result.failure;
    }
    return result;
  });
  const silent: Array<{ readonly route: ConnectionRoute; readonly index: number }> = [];
  for (const [index, route] of routes.entries()) {
    const check =
      checks.length === 0
        ? "unchecked"
        : yield* Effect.raceFirst(
            Fiber.join(checks[index]!),
            Fiber.join(headStart).pipe(Effect.as<RouteCheck>("silent")),
          );
    if (check === "silent") {
      silent.push({ route, index });
      continue;
    }
    const result = yield* attempt(route);
    if (result._tag === "Success") return result.success;
    // An incompatible server is the same server on every route.
    if (result.failure.reason === "unsupported") return yield* result.failure;
  }
  // A slow route that answered after the head start goes before one that is
  // still dead, which can hold an attempt open far longer than its probe.
  const late = yield* Effect.forEach(silent, ({ index }) => Fiber.join(checks[index]!), {
    concurrency: "unbounded",
  });
  const retry = [
    ...silent.filter((_, position) => late[position] !== "silent"),
    ...silent.filter((_, position) => late[position] === "silent"),
  ];
  for (const { route } of retry) {
    const result = yield* attempt(route);
    if (result._tag === "Success") return result.success;
    if (result.failure.reason === "unsupported") return yield* result.failure;
  }
  return yield* (
    transient ??
      blocked ??
      new ConnectionTransientError({
        reason: "endpoint-unavailable",
        detail: `${entry.target.label} did not answer on any saved route.`,
      })
  );
});

/** @public */
export const make = Effect.gen(function* () {
  const resolver = yield* ConnectionResolver.ConnectionResolver;
  const sessions = yield* RpcSession.RpcSessionFactory;
  const httpClient = yield* HttpClient.HttpClient;
  const diagnostics = yield* Diagnostics.ConnectionDiagnostics;

  const checkDescriptor = (httpBaseUrl: string, environmentId: string) =>
    fetchRemoteEnvironmentDescriptor({ httpBaseUrl, timeoutMs: ROUTE_CHECK_TIMEOUT_MS }).pipe(
      Effect.map((descriptor): RouteCheck =>
        descriptor.environmentId === environmentId ? "answered" : "silent",
      ),
      Effect.orElseSucceed((): RouteCheck => "silent"),
      Effect.provideService(FetchHttpClient.RequestInit, { cache: "no-store" }),
      Effect.provideService(HttpClient.HttpClient, httpClient),
    );

  const checkRoute = (
    entry: ConnectionCatalogEntry,
    route: ConnectionRoute,
    descriptorScope?: Scope.Scope,
  ) => {
    const httpBaseUrl = routeHttpBaseUrl(route);
    if (httpBaseUrl === null) return Effect.succeed<RouteCheck>("unchecked");
    const descriptor =
      descriptorScope === undefined
        ? checkDescriptor(httpBaseUrl, entry.target.environmentId)
        : ConnectionDescriptors.fetch(httpBaseUrl).pipe(
            Effect.forkIn(descriptorScope),
            Effect.flatMap(Fiber.join),
            Effect.timeoutOption(ROUTE_CHECK_TIMEOUT_MS),
            Effect.map((descriptor): RouteCheck =>
              Option.isSome(descriptor) &&
              descriptor.value.environmentId === entry.target.environmentId
                ? "answered"
                : "silent",
            ),
            Effect.orElseSucceed((): RouteCheck => "silent"),
          );
    return descriptor.pipe(
      Effect.provideService(HttpClient.HttpClient, httpClient),
      Effect.withSpan("ConnectionDriver.checkRoute", {
        attributes: { "connection.target.kind": route.target._tag },
      }),
    );
  };

  const checkHost = (prepared: PreparedConnection) =>
    checkDescriptor(prepared.httpBaseUrl, prepared.environmentId).pipe(
      Effect.withSpan("ConnectionDriver.checkHost", {
        attributes: { "connection.target.kind": prepared.target._tag },
      }),
    );

  const connect = Effect.fn("ConnectionDriver.connect")(function* (
    entry: ConnectionCatalogEntry,
    reportProgress: (progress: ConnectionDriverProgress) => Effect.Effect<void>,
  ) {
    const target = entry.target;
    yield* Effect.annotateCurrentSpan({
      "connection.environment.id": target.environmentId,
      "connection.target.kind": target._tag,
      "connection.route.count": connectionRoutes(entry).length,
    });
    diagnostics?.record({ kind: "attempt-start", environmentId: target.environmentId });
    diagnostics?.record({ kind: "stage", environmentId: target.environmentId, stage: "preparing" });
    yield* reportProgress({ stage: "preparing" });
    const descriptorScope = yield* Scope.make();
    const oldArm = (yield* AttemptReconnectArm) === "old";
    return yield* connectOverRoutes(
      entry,
      (route) => checkRoute(entry, route, oldArm ? undefined : descriptorScope),
      Effect.fnUntraced(function* (route) {
        diagnostics?.record({
          kind: "stage",
          environmentId: target.environmentId,
          stage: "preparing",
          route: Diagnostics.diagnosticRoute(route.target._tag, routeHttpBaseUrl(route) ?? ""),
        });
        const prepared = yield* resolver.prepare(routeEntry(entry, route));
        diagnostics?.record({
          kind: "stage",
          environmentId: target.environmentId,
          stage: "opening",
          route: Diagnostics.diagnosticRoute(prepared.target._tag, prepared.httpBaseUrl),
        });
        yield* reportProgress({ stage: "opening", prepared });
        const session = yield* sessions.connect(prepared);
        diagnostics?.record({
          kind: "stage",
          environmentId: target.environmentId,
          stage: "synchronizing",
        });
        yield* reportProgress({ stage: "synchronizing", prepared });
        yield* session.ready;
        return { prepared, session } satisfies EnvironmentConnectionLease;
      }),
    ).pipe(
      Effect.ensuring(Scope.close(descriptorScope, Exit.void)),
      Effect.provideService(
        Diagnostics.PreparationDiagnosticContext,
        diagnostics === null
          ? null
          : { environmentId: target.environmentId, record: diagnostics.record },
      ),
      Effect.onExit((exit) =>
        Effect.sync(() => {
          const failure = Exit.isFailure(exit)
            ? exit.cause.reasons.find(Cause.isFailReason)?.error
            : undefined;
          diagnostics?.record({
            kind: "attempt-end",
            environmentId: target.environmentId,
            outcome: Exit.isSuccess(exit)
              ? "connected"
              : Cause.hasInterruptsOnly(exit.cause)
                ? "interrupted"
                : "failed",
            error: failure?.reason ?? null,
          });
        }),
      ),
    );
  });

  const preflight = (entry: ConnectionCatalogEntry, route: ConnectionRoute) =>
    Effect.scoped(
      Effect.gen(function* () {
        const descriptorScope = yield* Scope.Scope;
        return yield* (
          sendsTokenInClear(connectionRoutes(entry), route)
            ? Effect.succeed("silent" as const)
            : checkRoute(entry, route, descriptorScope)
        ).pipe(
          Effect.flatMap((check) =>
            check === "answered"
              ? resolver.prepare(routeEntry(entry, route), "preflight").pipe(
                  Effect.as(true),
                  Effect.orElseSucceed(() => false),
                )
              : Effect.succeed(false),
          ),
        );
      }),
    ).pipe(
      ConnectionDescriptors.withConnectionDescriptors,
      Effect.provideService(HttpClient.HttpClient, httpClient),
      Effect.withSpan("ConnectionDriver.preflight"),
    );

  return ConnectionDriver.of({
    connect: Effect.fnUntraced(function* (entry, reportProgress) {
      return yield* connect(entry, reportProgress).pipe(
        ConnectionDescriptors.withConnectionDescriptors,
        Effect.provideService(HttpClient.HttpClient, httpClient),
      );
    }),
    checkHost,
    preflight,
  });
});

export const layer = Layer.effect(ConnectionDriver, make);
