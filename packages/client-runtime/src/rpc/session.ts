import {
  type ServerConfig,
  type ServerConfigStreamEvent,
  WsSubscribeServerConfigRpc,
  WS_METHODS,
} from "@t3tools/contracts";
import {
  RPC_WEBTRANSPORT_LIMITS,
  RpcWebTransportError,
  type ReliableSession,
} from "@t3tools/shared/rpcWebTransport";
import * as Clock from "effect/Clock";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Redacted from "effect/Redacted";
import * as Result from "effect/Result";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as HttpClient from "effect/http/HttpClient";
import type * as Rpc from "effect/rpc/Rpc";
import * as RpcClient from "effect/rpc/RpcClient";
import * as RpcClientError from "effect/rpc/RpcClientError";
import * as RpcSerialization from "effect/rpc/RpcSerialization";
import * as Socket from "effect/socket/Socket";

import { makeWsRpcProtocolClient, PING_TIMEOUT, type WsRpcProtocolClient } from "./protocol.ts";
import { makeWebSocketLayer } from "./webSocket.ts";
import { appendClientConnectionParams } from "../authorization/remote.ts";
import { RpcWebTransportConnector } from "../platform/rpcWebTransport.ts";
import { makeRpcWebTransportCooldown } from "./webTransportCooldown.ts";
import {
  prepareRpcWebTransportClient,
  makeRpcWebTransportClientProtocol,
  withRpcWebTransportProbe,
} from "./webTransportProtocol.ts";
import { issueRemoteWebSocketTicket } from "../authorization/remote.ts";
import * as ClientCapabilities from "../platform/capabilities.ts";
import { NETWORK_BLOCKING_HINT } from "../errors/network.ts";
import type {
  ConnectionAttemptError,
  ConnectionTransientError,
  PreparedConnection,
} from "../connection/model.ts";
import {
  ConnectionBlockedError,
  ConnectionTransientError as ConnectionTransientErrorClass,
} from "../connection/model.ts";
import {
  applyServerConfigProjection,
  type ServerConfigProjection,
  withoutEnvironmentThemes,
} from "../state/serverConfigProjection.ts";
import { environmentMismatchError, mapRemoteEnvironmentError } from "../connection/errors.ts";
import { orchestrationProtocolCompatibilityError } from "../connection/compatibility.ts";

export interface RpcSession {
  /** Selected protocol, for runtime diagnostics without exposing admission URLs. */
  readonly transport?: "websocket" | "webtransport";
  readonly client: WsRpcProtocolClient;
  readonly initialConfig: Effect.Effect<ServerConfig, ConnectionAttemptError>;
  readonly subscribeServerConfig: (
    input: ServerConfigSubscriptionInput,
  ) => ServerConfigSubscription;
  readonly ready: Effect.Effect<void, ConnectionAttemptError>;
  readonly probe: Effect.Effect<void, ConnectionAttemptError>;
  readonly closed: Effect.Effect<never, ConnectionAttemptError>;
}

export interface RpcSessionOptions {
  readonly environmentThemes?: boolean;
  readonly usageLimitSources?: boolean;
  /** This client answers /usage-limits itself, so the server may advertise it. */
  readonly usageLimitsCommand?: boolean;
}

export class RpcSessionFactory extends Context.Service<
  RpcSessionFactory,
  {
    readonly connect: (
      connection: PreparedConnection,
    ) => Effect.Effect<RpcSession, ConnectionAttemptError, Scope.Scope>;
  }
>()("@t3tools/client-runtime/rpc/session/RpcSessionFactory") {}

type InitialConfigError = Effect.Error<
  ReturnType<WsRpcProtocolClient[typeof WS_METHODS.serverGetConfig]>
>;
type ProbeError = Effect.Error<ReturnType<WsRpcProtocolClient[typeof WS_METHODS.serverProbe]>>;
type ServerConfigSubscriptionError =
  | Rpc.ErrorExit<typeof WsSubscribeServerConfigRpc>
  | RpcClientError.RpcClientError;
type ServerConfigSubscription = Stream.Stream<
  ServerConfigStreamEvent,
  ServerConfigSubscriptionError
>;
type ServerConfigSubscriptionInput = Parameters<
  WsRpcProtocolClient[typeof WS_METHODS.subscribeServerConfig]
>[0];
type EnvironmentThemesUpdatedEvent = Extract<
  ServerConfigStreamEvent,
  { readonly type: "environmentThemesUpdated" }
>;
type UsageLimitSourcesUpdatedEvent = Extract<
  ServerConfigStreamEvent,
  { readonly type: "usageLimitSourcesUpdated" }
>;

interface ServerConfigReplayState {
  readonly projection: ServerConfigProjection;
  readonly revision: number;
  readonly themesEvent: EnvironmentThemesUpdatedEvent | undefined;
  readonly sourcesEvent: UsageLimitSourcesUpdatedEvent | undefined;
}

interface BufferedServerConfigEvent {
  readonly event: ServerConfigStreamEvent;
  readonly replay: ServerConfigReplayState;
  readonly revision: number;
}

function serverConfigReplayEvents(
  state: ServerConfigReplayState,
): ReadonlyArray<ServerConfigStreamEvent> {
  const snapshot = {
    version: 1 as const,
    type: "snapshot" as const,
    config: withoutEnvironmentThemes(state.projection.config),
  };
  return [
    snapshot,
    ...(state.themesEvent === undefined ? [] : [state.themesEvent]),
    ...(state.sourcesEvent === undefined ? [] : [state.sourcesEvent]),
  ];
}

const isSocketErrorReason = Schema.is(Socket.SocketErrorReason);

function mapSessionRpcError(
  error: InitialConfigError | ProbeError | ServerConfigSubscriptionError,
  networkHint: string,
): ConnectionAttemptError {
  switch (error._tag) {
    case "EnvironmentAuthorizationError":
      return new ConnectionBlockedError({
        reason: "permission",
        detail: error.message,
      });
    case "KeybindingsConfigParseError":
    case "ServerSettingsError":
      return new ConnectionTransientErrorClass({
        reason: "remote-unavailable",
        detail: error.message,
      });
    case "RpcClientError":
      return new ConnectionTransientErrorClass({
        reason: "transport",
        detail: `${error.message}${isSocketErrorReason(error.reason) ? networkHint : ""}`,
      });
  }
}

function closeReliableSession(session: ReliableSession) {
  try {
    session.close(0);
  } catch {
    /* Logical cancellation does not await native close. */
  }
}

/** @public Service construction is part of the canonical Effect module API. */
export const make = Effect.fn("RpcSessionFactory.make")(function* (
  options: RpcSessionOptions = {},
) {
  const webSocketLayer = yield* makeWebSocketLayer;
  const webTransportConnector = yield* Effect.serviceOption(RpcWebTransportConnector);
  const cooldown = yield* makeRpcWebTransportCooldown;
  const httpClient = yield* HttpClient.HttpClient;
  const constructorWithHeaders = yield* Effect.serviceOption(
    ClientCapabilities.WebSocketConstructorWithHeaders,
  );
  const serverConfigInput: ServerConfigSubscriptionInput = {
    ...(options.environmentThemes === true ? { environmentThemes: true } : {}),
    ...(options.usageLimitSources === true ? { usageLimitSources: true } : {}),
    ...(options.usageLimitsCommand === true ? { usageLimitsCommand: true } : {}),
  };

  const connect = Effect.fnUntraced(function* (connection: PreparedConnection) {
    const networkHint =
      connection.target._tag === "RelayConnectionTarget" ? ` ${NETWORK_BLOCKING_HINT}` : "";
    const mapRpcError = (error: Parameters<typeof mapSessionRpcError>[0]) =>
      mapSessionRpcError(error, networkHint);
    yield* Effect.annotateCurrentSpan({
      "connection.environment.id": connection.environmentId,
    });

    const connected = yield* Deferred.make<void>();
    const disconnected = yield* Deferred.make<never, ConnectionTransientError>();
    const sessionScope = yield* Effect.scope;
    let socketOpened = false;
    const authorization = connection.httpAuthorization;
    const usesBearerUpgradeHeaders =
      authorization?._tag === "Bearer" && Option.isSome(constructorWithHeaders);
    // Native WebSocket failures omit the HTTP rejection. Share one diagnostic
    // across readiness, config and closure consumers, only before an upgrade opens.
    // Its fiber belongs to the session so canceling a waiter cannot issue it twice.
    const diagnostic = yield* Effect.cached(
      (usesBearerUpgradeHeaders
        ? issueRemoteWebSocketTicket({
            httpBaseUrl: connection.httpBaseUrl,
            bearerToken: authorization.token,
            timeoutMs: 3_000,
          }).pipe(
            Effect.match({
              onFailure: (error) => Option.some(mapRemoteEnvironmentError(error)),
              onSuccess: (issued) =>
                issued.orchestrationProtocolVersion === undefined
                  ? Option.none<ConnectionAttemptError>()
                  : Option.fromNullishOr(
                      orchestrationProtocolCompatibilityError({
                        label: connection.label,
                        orchestrationProtocolVersion: issued.orchestrationProtocolVersion,
                      }),
                    ),
            }),
            Effect.provideService(HttpClient.HttpClient, httpClient),
          )
        : Effect.succeed(Option.none<ConnectionAttemptError>())
      ).pipe(Effect.forkIn(sessionScope)),
    );
    const diagnoseError = (error: ConnectionAttemptError) =>
      transport !== "websocket" ||
      !usesBearerUpgradeHeaders ||
      socketOpened ||
      error._tag === "ConnectionBlockedError"
        ? Effect.fail(error)
        : diagnostic.pipe(
            Effect.flatMap(Fiber.join),
            Effect.flatMap((diagnosed) => Effect.fail(Option.getOrElse(diagnosed, () => error))),
          );
    // This session never reopens its socket. Once it ends, waiting for the
    // socket writer's open latch cannot succeed, including an RPC interrupt
    // already waiting there when the transport drops.
    const transportClosed = yield* Deferred.make<void>();
    const stopTransportWrites = Deferred.succeed(transportClosed, undefined).pipe(Effect.asVoid);
    const writeWhileLive = (write: Effect.Effect<void, Socket.SocketError>) =>
      Deferred.isDone(transportClosed).pipe(
        Effect.flatMap((closed) =>
          closed ? Effect.void : Effect.raceFirst(write, Deferred.await(transportClosed)),
        ),
      );
    // Set when the socket closes because the server stopped answering, so the failure says so
    // instead of looking like the server closed the connection.
    const pingTimedOut = yield* Ref.make(false);
    let transport: "websocket" | "webtransport" = "websocket";
    let ownerDisposing = false;
    let disposeSelectedTransport: Effect.Effect<void> | undefined;
    let selectedAttempt: Effect.Success<ReturnType<typeof cooldown.begin>>;
    const recordTransportFailure = Effect.suspend(() =>
      selectedAttempt !== undefined && !ownerDisposing
        ? cooldown.failed(selectedAttempt)
        : Effect.void,
    );
    const hooks = RpcClient.ConnectionHooks.of({
      onConnect: Deferred.succeed(connected, undefined).pipe(Effect.asVoid),
      onPingTimeout: Ref.set(pingTimedOut, true),
      onDisconnect: stopTransportWrites.pipe(
        Effect.andThen(recordTransportFailure),
        Effect.andThen(Effect.all([Deferred.isDone(connected), Ref.get(pingTimedOut)])),
        Effect.flatMap(([wasConnected, timedOut]) =>
          Deferred.fail(
            disconnected,
            new ConnectionTransientErrorClass({
              reason: "transport",
              detail: `${
                !wasConnected
                  ? `${connection.label} could not establish a ${transport === "websocket" ? "WebSocket" : "WebTransport"} connection.`
                  : timedOut
                    ? `${connection.label} stopped responding.`
                    : `${connection.label} disconnected.`
              }${networkHint}`,
            }),
          ),
        ),
        Effect.asVoid,
      ),
    });
    const candidate = connection.rpcWebTransport;
    const ticket = connection.rpcTicket;
    const attempt =
      candidate !== undefined &&
      ticket !== undefined &&
      Option.isSome(webTransportConnector) &&
      ticket.expiresAtEpochMs > (yield* Clock.currentTimeMillis)
        ? yield* cooldown.begin(connection.environmentId, candidate)
        : undefined;
    const selected =
      attempt === undefined ||
      candidate === undefined ||
      ticket === undefined ||
      Option.isNone(webTransportConnector)
        ? undefined
        : yield* Effect.gen(function* () {
            const candidateScope = yield* Scope.make();
            yield* Effect.addFinalizer((exit) =>
              Effect.sync(() => {
                ownerDisposing = true;
              }).pipe(Effect.andThen(Scope.close(candidateScope, exit))),
            );
            const url = new URL(candidate.url);
            url.searchParams.set("wsTicket", Redacted.value(ticket.value));
            appendClientConnectionParams(url, candidate.clientMetadata, "direct");
            const prepared = yield* Effect.gen(function* () {
              const session = yield* Effect.acquireRelease(
                Effect.tryPromise({
                  try: (signal) =>
                    webTransportConnector.value
                      .connect({ url: url.toString(), signal })
                      .then((session) => {
                        if (signal.aborted) {
                          closeReliableSession(session);
                          throw new RpcWebTransportError("aborted", "RPC connection cancelled");
                        }
                        return session;
                      }),
                  catch: (cause) =>
                    new RpcWebTransportError("network", "RPC transport connection failed", {
                      cause,
                    }),
                }).pipe(Effect.interruptible),
                (session) => Effect.sync(() => closeReliableSession(session)),
              );
              return yield* prepareRpcWebTransportClient(session, {
                environmentId: connection.environmentId,
                orchestrationProtocolVersion: candidate.orchestrationProtocolVersion,
              });
            }).pipe(
              Effect.provideService(Scope.Scope, candidateScope),
              Effect.timeoutOrElse({
                duration: RPC_WEBTRANSPORT_LIMITS.candidateTimeoutMs,
                orElse: () =>
                  Effect.fail(
                    new RpcWebTransportError("network", "RPC transport establishment timed out"),
                  ),
              }),
              Effect.onInterrupt(() => Scope.close(candidateScope, Exit.void)),
              Effect.result,
            );
            if (Result.isFailure(prepared)) {
              yield* Scope.close(candidateScope, Exit.void);
              yield* cooldown.failed(attempt);
              return undefined;
            }
            yield* cooldown.succeeded(attempt);
            selectedAttempt = attempt;
            transport = "webtransport";
            disposeSelectedTransport = Effect.sync(() => {
              ownerDisposing = true;
            }).pipe(Effect.andThen(Scope.close(candidateScope, Exit.void)));
            const protocol = yield* makeRpcWebTransportClientProtocol(prepared.success).pipe(
              Effect.provideService(Scope.Scope, candidateScope),
              Effect.provideService(RpcClient.ConnectionHooks, hooks),
              Effect.mapError(
                () =>
                  new ConnectionTransientErrorClass({
                    reason: "transport",
                    detail: `${connection.label} disconnected.`,
                  }),
              ),
            );
            return Context.make(RpcClient.Protocol, protocol);
          });
    const protocolContext =
      selected ??
      (yield* Effect.gen(function* () {
        const layerNativeSocket = webSocketLayer(
          connection,
          usesBearerUpgradeHeaders
            ? () => {
                socketOpened = true;
              }
            : undefined,
        );
        const layerSocket = Layer.effect(
          Socket.Socket,
          Effect.map(Socket.Socket, (socket) =>
            Socket.make({
              reader: socket.reader,
              writer: Effect.map(socket.writer, (writer) => ({
                write: (chunk) => writeWhileLive(writer.write(chunk)),
                writeAll: (chunks) => writeWhileLive(writer.writeAll(chunks)),
              })),
            }),
          ),
        ).pipe(Layer.provide(layerNativeSocket));
        const layerProtocol = Layer.effect(
          RpcClient.Protocol,
          RpcClient.makeProtocolSocket({
            pingTimeout: PING_TIMEOUT,
            retryTransientErrors: false,
            retryPolicy: Schedule.recurs(0),
          }),
        ).pipe(
          Layer.provide(
            Layer.mergeAll(
              layerSocket,
              RpcSerialization.layerJson,
              Layer.succeed(RpcClient.ConnectionHooks, hooks),
            ),
          ),
        );
        return yield* Layer.build(layerProtocol).pipe(
          Effect.withSpan("environment.websocket.connect"),
        );
      }));
    yield* Effect.annotateCurrentSpan("connection.rpc.transport", transport);
    const protocolClient = yield* makeWsRpcProtocolClient.pipe(Effect.provide(protocolContext));
    const initialConfigDeferred = yield* Deferred.make<ServerConfig>();
    const serverConfigExit = yield* Deferred.make<void, ServerConfigSubscriptionError>();
    const configSubscriptionClosed = yield* Deferred.make<never, ConnectionAttemptError>();
    const serverConfigState = yield* Ref.make(Option.none<ServerConfigReplayState>());
    const serverConfigUpdates = yield* PubSub.sliding<BufferedServerConfigEvent>(64);
    const configSubscriptionEndedError = new ConnectionTransientErrorClass({
      reason: "remote-unavailable",
      detail: `${connection.label} config subscription ended.`,
    });
    const serverConfigSource = protocolClient[WS_METHODS.subscribeServerConfig](
      serverConfigInput,
    ).pipe(
      Stream.runForEach((event) =>
        Effect.gen(function* () {
          const buffered = yield* Ref.modify(serverConfigState, (current) => {
            const projection = applyServerConfigProjection(
              Option.map(current, (state) => state.projection),
              event,
            );
            if (Option.isNone(projection)) {
              return [Option.none<BufferedServerConfigEvent>(), current] as const;
            }
            const next = {
              projection: projection.value,
              revision: Option.match(current, {
                onNone: () => 1,
                onSome: (state) => state.revision + 1,
              }),
              themesEvent:
                event.type === "environmentThemesUpdated"
                  ? event
                  : event.type === "snapshot" &&
                      event.config.environment.capabilities.environmentThemes !== true
                    ? undefined
                    : Option.getOrUndefined(current)?.themesEvent,
              sourcesEvent:
                event.type === "usageLimitSourcesUpdated"
                  ? event
                  : event.type === "snapshot" &&
                      event.config.environment.capabilities.usageLimitSources !== true
                    ? undefined
                    : Option.getOrUndefined(current)?.sourcesEvent,
            } satisfies ServerConfigReplayState;
            return [
              Option.some({ event, replay: next, revision: next.revision }),
              Option.some(next),
            ] as const;
          });
          if (Option.isSome(buffered)) {
            yield* PubSub.publish(serverConfigUpdates, buffered.value);
          }
          if (event.type === "snapshot") {
            yield* Deferred.succeed(initialConfigDeferred, event.config);
          }
        }),
      ),
      Effect.onExit((exit) => {
        if (Exit.isSuccess(exit)) {
          return recordTransportFailure.pipe(
            Effect.andThen(
              Effect.all([
                Deferred.succeed(serverConfigExit, undefined),
                Deferred.fail(configSubscriptionClosed, configSubscriptionEndedError),
              ]),
            ),
            Effect.asVoid,
          );
        }
        if (Cause.hasInterruptsOnly(exit.cause)) {
          return Effect.void;
        }
        return recordTransportFailure.pipe(
          Effect.andThen(
            Effect.all([
              Deferred.failCause(serverConfigExit, exit.cause),
              Deferred.failCause(configSubscriptionClosed, Cause.map(exit.cause, mapRpcError)),
            ]),
          ),
          Effect.asVoid,
        );
      }),
    );
    // LIFO scope release must retire writes before canceling the config
    // stream or RPC client. Register both atomically so interruption during
    // construction cannot leave an unopened socket waiting on an interrupt.
    // Teardown remains synchronous: the native close runs before replacement.
    yield* Effect.uninterruptible(
      serverConfigSource.pipe(
        Effect.forkScoped,
        Effect.andThen(Effect.addFinalizer(() => stopTransportWrites)),
      ),
    );
    const initialConfig = Effect.raceFirst(
      Deferred.await(initialConfigDeferred),
      Deferred.await(serverConfigExit).pipe(
        Effect.mapError(mapRpcError),
        Effect.flatMap(() => Effect.fail(configSubscriptionEndedError)),
      ),
    ).pipe(
      Effect.filterOrElse(
        (config) => config.environment.environmentId === connection.environmentId,
        (config) =>
          environmentMismatchError({
            expected: connection.environmentId,
            actual: config.environment.environmentId,
          }),
      ),
      Effect.withSpan("environment.initialSync"),
      Effect.catch(diagnoseError),
    );
    const serverConfigEvents = Stream.unwrap(
      Effect.gen(function* () {
        const subscription = yield* PubSub.subscribe(serverConfigUpdates);
        const snapshot = yield* Ref.get(serverConfigState);
        if (Option.isNone(snapshot)) {
          return Stream.empty;
        }
        const updates = Stream.fromSubscription(subscription).pipe(
          Stream.filter((buffered) => buffered.revision > snapshot.value.revision),
          Stream.mapAccum(
            () => snapshot.value.revision,
            (revision, buffered) => [
              buffered.revision,
              buffered.revision === revision + 1
                ? [buffered.event]
                : serverConfigReplayEvents(buffered.replay),
            ],
          ),
        );
        const terminal = Stream.fromEffect(Deferred.await(serverConfigExit)).pipe(Stream.drain);
        return Stream.concat(
          Stream.fromIterable(serverConfigReplayEvents(snapshot.value)),
          Stream.merge(updates, terminal, { haltStrategy: "either" }),
        );
      }),
    ).pipe(
      Stream.catchCause((cause) => {
        if (Cause.hasInterruptsOnly(cause)) {
          return Stream.failCause(cause);
        }
        // The supervisor keeps the original cause. Shared durable consumers
        // need a transport-shaped failure so they wait for its replacement.
        return Stream.fail(
          new RpcClientError.RpcClientError({
            reason: new RpcClientError.RpcClientDefect({
              message: `${connection.label} config subscription failed.`,
              cause,
            }),
          }),
        );
      }),
    );
    const validatedInitialConfig = initialConfig.pipe(
      Effect.mapError(
        (cause) =>
          new RpcClientError.RpcClientError({
            reason: new RpcClientError.RpcClientDefect({
              message: `${connection.label} config subscription failed.`,
              cause,
            }),
          }),
      ),
    );
    const subscribeServerConfig = (input: ServerConfigSubscriptionInput) =>
      Stream.unwrap(
        validatedInitialConfig.pipe(
          Effect.as(
            Equal.equals(input, serverConfigInput)
              ? serverConfigEvents
              : protocolClient[WS_METHODS.subscribeServerConfig](input),
          ),
        ),
      );
    const probe = initialConfig.pipe(
      Effect.flatMap((config) =>
        (config.environment.capabilities.connectionProbe === true
          ? protocolClient[WS_METHODS.serverProbe]({})
          : protocolClient[WS_METHODS.serverGetConfig]({}).pipe(withRpcWebTransportProbe)
        ).pipe(Effect.mapError(mapRpcError)),
      ),
      Effect.asVoid,
      Effect.withSpan("clientRuntime.connection.rpcSession.probe"),
    );

    // End transport ownership before RpcClient's subscription finalizers send cancellation.
    if (disposeSelectedTransport !== undefined) {
      yield* Effect.addFinalizer(() => disposeSelectedTransport!);
    }
    return {
      transport,
      client: protocolClient,
      initialConfig,
      subscribeServerConfig,
      ready: Deferred.await(connected).pipe(
        Effect.andThen(initialConfig),
        Effect.asVoid,
        Effect.raceFirst(Deferred.await(disconnected)),
        Effect.catch(diagnoseError),
      ),
      probe,
      closed: Effect.raceFirst(
        Deferred.await(disconnected),
        Deferred.await(configSubscriptionClosed),
      ).pipe(Effect.catch(diagnoseError)),
    } satisfies RpcSession;
  });

  return RpcSessionFactory.of({ connect });
});

export const layer = (options: RpcSessionOptions) => Layer.effect(RpcSessionFactory, make(options));
