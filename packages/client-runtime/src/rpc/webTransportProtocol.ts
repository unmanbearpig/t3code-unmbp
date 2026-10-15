import {
  RPC_WEBTRANSPORT_LIMITS,
  RPC_WEBTRANSPORT_WIRE_VERSION,
  RpcWebTransportError,
  ReadySchema,
  ServerControlEnvelopeSchema,
  ServerRequestEnvelopeSchema,
  createTransportByteBudget,
  decodeTransportEnvelope,
  lowerRpcWebTransportLimits,
  makeRpcWebTransportFrameChannel,
  makeRpcWebTransportLifetime,
  resolveRpcWebTransportLimits,
  transportTry,
  type ReliableByteStream,
  type ReliableSession,
  type RpcWebTransportLimits,
} from "@t3tools/shared/rpcWebTransport";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Semaphore from "effect/Semaphore";
import * as RpcClient from "effect/rpc/RpcClient";
import { RpcClientDefect, RpcClientError } from "effect/rpc/RpcClientError";
import type * as RpcMessage from "effect/rpc/RpcMessage";
import * as RpcSerialization from "effect/rpc/RpcSerialization";

interface Expected {
  readonly environmentId: string;
  readonly orchestrationProtocolVersion: number;
  readonly limits?: Partial<RpcWebTransportLimits>;
}

const preparedBrand: unique symbol = Symbol("PreparedRpcWebTransportClient");
class ProbeAdmission extends Context.Service<ProbeAdmission, true>()(
  "@t3tools/client-runtime/rpc/webTransportProtocol/ProbeAdmission",
) {}

/** Marks only the supervisor's legacy server.getConfig call as a reserved health probe. */
export const withRpcWebTransportProbe = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.provideService(effect, ProbeAdmission, true);
export interface PreparedRpcWebTransportClient {
  readonly [preparedBrand]: Effect.Success<ReturnType<typeof prepare>>;
}

const toRpcError = (cause: RpcWebTransportError) =>
  new RpcClientError({
    reason: new RpcClientDefect({ message: cause.message, cause }),
  });

const prepare = Effect.fnUntraced(function* (session: ReliableSession, expected: Expected) {
  const lifetime = yield* makeRpcWebTransportLifetime(session);
  const limits = yield* resolveRpcWebTransportLimits(expected.limits);
  const input = createTransportByteBudget(RPC_WEBTRANSPORT_LIMITS.sessionInputBytes);
  const output = createTransportByteBudget(RPC_WEBTRANSPORT_LIMITS.sessionOutputBytes);
  const controlInput = createTransportByteBudget(RPC_WEBTRANSPORT_LIMITS.controlReserveBytes);
  const controlOutput = createTransportByteBudget(RPC_WEBTRANSPORT_LIMITS.controlReserveBytes);
  const stream = lifetime.own(
    yield* lifetime.race(transportTry((signal) => session.openBidirectionalStream(signal))),
  );
  const control = makeRpcWebTransportFrameChannel(stream, controlInput, controlOutput, limits);
  yield* lifetime.race(
    Effect.gen(function* () {
      yield* control.writePreface("control");
      yield* control.write(
        {
          _tag: "Hello",
          wireVersion: RPC_WEBTRANSPORT_WIRE_VERSION,
          orchestrationProtocolVersion: expected.orchestrationProtocolVersion,
          encodings: ["identity", "deflate"],
          limits,
        },
        true,
      );
    }),
  );
  const ready = yield* lifetime.race(
    control
      .read({ control: true })
      .pipe(Effect.flatMap((value) => decodeTransportEnvelope(ReadySchema, value))),
  );
  if (
    ready.environmentId !== expected.environmentId ||
    ready.orchestrationProtocolVersion !== expected.orchestrationProtocolVersion ||
    ready.wireVersion !== RPC_WEBTRANSPORT_WIRE_VERSION ||
    ready.encoding !== "deflate"
  ) {
    return yield* Effect.fail(
      new RpcWebTransportError("protocol", "RPC transport identity or protocol mismatch"),
    );
  }
  const negotiated = lowerRpcWebTransportLimits(limits, ready.limits);
  const clock = yield* Clock.Clock;
  let lastFrame = clock.currentTimeMillisUnsafe();
  let deliverControl: ((message: RpcMessage.FromServerEncoded) => Effect.Effect<void>) | undefined;
  let attached = false;
  yield* Effect.gen(function* () {
    while (true) {
      const message = yield* control
        .read({ control: true })
        .pipe(
          Effect.flatMap((value) => decodeTransportEnvelope(ServerControlEnvelopeSchema, value)),
        );
      lastFrame = clock.currentTimeMillisUnsafe();
      if (message._tag === "Eof")
        return yield* lifetime.close(new RpcWebTransportError("network", "RPC session ended"));
      if (message._tag === "Defect")
        return yield* lifetime.close(
          new RpcWebTransportError("protocol", "Server reported an RPC session defect"),
        );
      if (message._tag !== "Pong") {
        if (!deliverControl)
          return yield* Effect.fail(
            new RpcWebTransportError("protocol", "Unsolicited message before client registration"),
          );
        yield* deliverControl(message);
      }
    }
  }).pipe(lifetime.race, Effect.catch(lifetime.close), Effect.forkScoped);
  return {
    session,
    lifetime,
    control,
    input,
    output,
    limits: negotiated,
    clock,
    onFrame() {
      lastFrame = clock.currentTimeMillisUnsafe();
    },
    get lastFrame() {
      return lastFrame;
    },
    attach(deliver: (message: RpcMessage.FromServerEncoded) => Effect.Effect<void>) {
      if (attached) throw new RpcWebTransportError("protocol", "RPC transport already attached");
      attached = true;
      deliverControl = deliver;
    },
  };
});

/** Completes discovery-bound control negotiation without winner connection hooks. */
export const prepareRpcWebTransportClient = Effect.fn("prepareRpcWebTransportClient")(function* (
  session: ReliableSession,
  expected: Expected,
) {
  const state = yield* prepare(session, expected);
  return { [preparedBrand]: state } satisfies PreparedRpcWebTransportClient;
});

/** Creates the installed Effect RPC protocol for one selected authenticated session. */
export const makeRpcWebTransportClientProtocol = Effect.fn("makeRpcWebTransportClientProtocol")(
  function* (prepared: PreparedRpcWebTransportClient) {
    const state = prepared[preparedBrand];
    const { lifetime } = state;
    const scope = yield* Effect.scope;
    const hooks = yield* Effect.serviceOption(RpcClient.ConnectionHooks);
    const clients = new Map<
      number,
      (message: RpcMessage.FromServerEncoded) => Effect.Effect<void>
    >();
    interface RequestEntry {
      readonly clientId: number;
      readonly ready: Deferred.Deferred<
        ReturnType<typeof makeRpcWebTransportFrameChannel>,
        RpcWebTransportError
      >;
      fiber?: Fiber.Fiber<void>;
      stream?: ReliableByteStream;
      completed?: boolean;
    }
    const requests = new Map<string | number, RequestEntry>();
    const ordinary = Semaphore.makeUnsafe(state.limits.ordinaryRequestStreams);
    const probe = Semaphore.makeUnsafe(1);
    const nativeOpen = Semaphore.makeUnsafe(1);
    let ordinaryOutstanding = 0;
    let probeOutstanding = 0;
    let disconnected = false;
    const disconnect = Effect.suspend(() => {
      if (disconnected) return Effect.void;
      disconnected = true;
      return Option.isSome(hooks) ? hooks.value.onDisconnect : Effect.void;
    });
    const broadcast = (message: RpcMessage.FromServerEncoded) =>
      Effect.forEach(clients.values(), (f) => f(message)).pipe(Effect.asVoid);
    yield* Effect.try({
      try: () => state.attach(broadcast),
      catch: (cause) =>
        cause instanceof RpcWebTransportError
          ? cause
          : new RpcWebTransportError("protocol", "Could not attach RPC client", { cause }),
    });
    const fail = (cause: RpcWebTransportError) => lifetime.close(cause);
    yield* lifetime.closed.pipe(
      Effect.andThen(
        Effect.suspend(() =>
          broadcast({ _tag: "ClientProtocolError", error: toRpcError(lifetime.error!) }),
        ),
      ),
      Effect.andThen(disconnect),
      Effect.forkScoped,
    );
    yield* Effect.addFinalizer(() => lifetime.close().pipe(Effect.andThen(disconnect)));
    if (!lifetime.error && Option.isSome(hooks)) yield* hooks.value.onConnect;
    yield* Effect.gen(function* () {
      while (true) {
        yield* Effect.sleep(RPC_WEBTRANSPORT_LIMITS.pingIntervalMs);
        if (
          state.clock.currentTimeMillisUnsafe() - state.lastFrame >=
          RPC_WEBTRANSPORT_LIMITS.pingTimeoutMs
        ) {
          if (Option.isSome(hooks) && hooks.value.onPingTimeout) yield* hooks.value.onPingTimeout;
          return yield* fail(new RpcWebTransportError("network", "RPC transport ping timeout"));
        }
      }
    }).pipe(lifetime.race, Effect.catch(fail), Effect.forkScoped);
    yield* Effect.gen(function* () {
      while (true) {
        yield* Effect.sleep(RPC_WEBTRANSPORT_LIMITS.pingIntervalMs);
        yield* state.control.write({ _tag: "Ping" }, true);
      }
    }).pipe(lifetime.race, Effect.catch(fail), Effect.forkScoped);

    const sendRequest = Effect.fnUntraced(function* (
      clientId: number,
      request: RpcMessage.RequestEncoded,
    ) {
      const probeAdmission = yield* Effect.serviceOption(ProbeAdmission);
      const isProbe =
        request.tag === "server.probe" ||
        (request.tag === "server.getConfig" && Option.isSome(probeAdmission));
      const wireRequest = {
        ...request,
        headers: [
          ...request.headers.filter(([key]) => key !== "t3-rpc-probe"),
          ...(request.tag === "server.getConfig" && isProbe
            ? [["t3-rpc-probe", "1"] as [string, string]]
            : []),
        ],
      };
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          if (requests.has(request.id))
            return yield* Effect.fail(
              new RpcWebTransportError("protocol", "Duplicate active request ID"),
            );
          if (
            isProbe
              ? probeOutstanding >= 2
              : ordinaryOutstanding >=
                state.limits.ordinaryRequestStreams + state.limits.pendingOrdinaryRequests
          )
            return yield* Effect.fail(
              new RpcWebTransportError("capacity", "RPC stream admission queue is full"),
            );
          if (isProbe) probeOutstanding++;
          else ordinaryOutstanding++;
          const entry: RequestEntry = {
            clientId,
            ready: yield* Deferred.make<
              ReturnType<typeof makeRpcWebTransportFrameChannel>,
              RpcWebTransportError
            >(),
          };
          requests.set(request.id, entry);
          const task = (isProbe ? probe : ordinary)
            .withPermits(1)(
              Effect.gen(function* () {
                const stream = lifetime.own(
                  yield* Effect.acquireRelease(
                    nativeOpen.withPermits(1)(
                      transportTry((signal) => state.session.openBidirectionalStream(signal)),
                    ),
                    (owned) =>
                      Effect.sync(() => {
                        if (!entry.completed) {
                          owned.reset(0);
                          owned.stop(0);
                        }
                        lifetime.release(owned);
                      }),
                    { interruptible: true },
                  ),
                );
                entry.stream = stream;
                const channel = makeRpcWebTransportFrameChannel(
                  stream,
                  state.input,
                  state.output,
                  state.limits,
                );
                yield* channel.writePreface("request");
                yield* channel.write(wireRequest);
                yield* Deferred.succeed(entry.ready, channel);
                while (true) {
                  const message = yield* channel
                    .read()
                    .pipe(
                      Effect.flatMap((value) =>
                        decodeTransportEnvelope(ServerRequestEnvelopeSchema, value),
                      ),
                    );
                  if (message.requestId !== request.id)
                    return yield* Effect.fail(
                      new RpcWebTransportError("protocol", "Cross-stream response ID"),
                    );
                  state.onFrame();
                  const deliver = clients.get(clientId);
                  if (deliver)
                    yield* deliver(
                      message._tag === "Chunk"
                        ? message
                        : {
                            ...message,
                            exit:
                              message.exit._tag === "Success"
                                ? message.exit
                                : {
                                    _tag: "Failure",
                                    cause: message.exit.cause.map((cause) =>
                                      cause._tag === "Interrupt"
                                        ? { ...cause, fiberId: cause.fiberId }
                                        : cause,
                                    ),
                                  },
                          },
                    );
                  if (message._tag === "Exit") {
                    // Publish the terminal response before FIN, which can be flow-controlled.
                    yield* channel.finish.pipe(
                      Effect.timeoutOrElse({
                        duration: RPC_WEBTRANSPORT_LIMITS.assemblyIdleTimeoutMs,
                        orElse: () =>
                          Effect.fail(new RpcWebTransportError("protocol", "Request FIN stalled")),
                      }),
                    );
                    const trailing = yield* channel.read().pipe(
                      Effect.timeoutOrElse({
                        duration: RPC_WEBTRANSPORT_LIMITS.assemblyIdleTimeoutMs,
                        orElse: () =>
                          Effect.fail(new RpcWebTransportError("protocol", "Response FIN stalled")),
                      }),
                    );
                    if (trailing !== null)
                      return yield* Effect.fail(
                        new RpcWebTransportError("protocol", "Response after Exit"),
                      );
                    lifetime.release(stream);
                    entry.completed = true;
                    return;
                  }
                }
              }),
            )
            .pipe(
              Effect.scoped,
              lifetime.race,
              Effect.catch((cause) =>
                Deferred.fail(entry.ready, cause).pipe(Effect.andThen(fail(cause))),
              ),
              Effect.onInterrupt(() =>
                Deferred.fail(
                  entry.ready,
                  new RpcWebTransportError("aborted", "RPC request cancelled"),
                ).pipe(Effect.asVoid),
              ),
              Effect.ensuring(
                Effect.sync(() => {
                  requests.delete(request.id);
                  if (isProbe) probeOutstanding--;
                  else ordinaryOutstanding--;
                }),
              ),
            );
          entry.fiber = yield* task.pipe(Effect.interruptible, Effect.forkIn(scope));
          yield* restore(Deferred.await(entry.ready)).pipe(
            Effect.onInterrupt(() => (entry.fiber ? Fiber.interrupt(entry.fiber) : Effect.void)),
          );
        }),
      );
    });
    const protocol: RpcClient.Protocol["Service"] = {
      run(clientId, receive) {
        return Effect.gen(function* () {
          clients.set(clientId, receive);
          if (lifetime.error)
            yield* receive({ _tag: "ClientProtocolError", error: toRpcError(lifetime.error) });
          return yield* Effect.never;
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              clients.delete(clientId);
            }),
          ),
        );
      },
      send(clientId, message) {
        return lifetime
          .race(
            Effect.suspend(() => {
              if (message._tag === "Request") return sendRequest(clientId, message);
              if (message._tag === "Eof" || message._tag === "Ping")
                return state.control.write(message, true);
              const entry = requests.get(message.requestId);
              if (!entry) return Effect.void;
              if (entry.clientId !== clientId)
                return Effect.fail(new RpcWebTransportError("protocol", "Cross-client request ID"));
              if (message._tag === "Interrupt" && !entry.stream)
                return entry.fiber ? Fiber.interrupt(entry.fiber) : Effect.void;
              return Deferred.await(entry.ready).pipe(
                Effect.flatMap((channel) => channel.write(message)),
              );
            }),
          )
          .pipe(Effect.mapError(toRpcError));
      },
      supportsAck: true,
      supportsTransferables: false,
      codecFor: RpcSerialization.json.codecFor,
    };
    return protocol;
  },
);
