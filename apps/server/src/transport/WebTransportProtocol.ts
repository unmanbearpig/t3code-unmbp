import {
  RPC_WEBTRANSPORT_LIMITS,
  RPC_WEBTRANSPORT_WIRE_VERSION,
  ClientControlEnvelopeSchema,
  ClientRequestEnvelopeSchema,
  HelloSchema,
  RpcWebTransportError,
  decodeTransportEnvelope,
  lowerRpcWebTransportLimits,
  makeRpcWebTransportFrameChannel,
  makeRpcWebTransportLifetime,
  resolveRpcWebTransportLimits,
  transportTry,
  type ReliableByteStream,
  type RpcWebTransportLimits,
  type ServerReliableSession,
  type TransportByteBudget,
} from "@t3tools/shared/rpcWebTransport";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Semaphore from "effect/Semaphore";
import type * as RpcMessage from "effect/rpc/RpcMessage";
import * as RpcSerialization from "effect/rpc/RpcSerialization";
import * as RpcServer from "effect/rpc/RpcServer";

interface Expected {
  readonly environmentId: string;
  readonly orchestrationProtocolVersion: number;
  /** One listener-owned pair, reused for every authenticated session. */
  readonly budgets: { readonly input: TransportByteBudget; readonly output: TransportByteBudget };
  readonly limits?: Partial<RpcWebTransportLimits>;
}

/** Validates Hello; Ready waits for RpcServer.make to install the receive callback. */
export const prepareRpcWebTransportServer = Effect.fn("prepareRpcWebTransportServer")(function* (
  session: ServerReliableSession,
  expected: Expected,
) {
  const lifetime = yield* makeRpcWebTransportLifetime(session);
  return yield* Effect.gen(function* () {
    const localLimits = yield* resolveRpcWebTransportLimits(expected.limits);
    const input = expected.budgets.input.child(RPC_WEBTRANSPORT_LIMITS.sessionInputBytes);
    const output = expected.budgets.output.child(RPC_WEBTRANSPORT_LIMITS.sessionOutputBytes);
    const { controlInput, controlOutput } = yield* lifetime.reserveControlBudgets(input, output);
    const controlStream = lifetime.own(
      yield* lifetime.race(transportTry((signal) => session.acceptBidirectionalStream(signal))),
    );
    const control = makeRpcWebTransportFrameChannel(
      controlStream,
      controlInput,
      controlOutput,
      localLimits,
    );
    const role = yield* lifetime.race(control.readPreface());
    if (role !== "control")
      return yield* Effect.fail(
        new RpcWebTransportError("protocol", "First stream must be control"),
      );
    const hello = yield* lifetime.race(
      control
        .read({ first: true, control: true })
        .pipe(Effect.flatMap((value) => decodeTransportEnvelope(HelloSchema, value))),
    );
    if (
      hello.orchestrationProtocolVersion !== expected.orchestrationProtocolVersion ||
      !hello.encodings.includes("deflate") ||
      !hello.encodings.includes("identity")
    )
      return yield* Effect.fail(
        new RpcWebTransportError("protocol", "Unsupported RPC transport negotiation"),
      );
    const limits = lowerRpcWebTransportLimits(localLimits, hello.limits);
    const disconnects = yield* Queue.make<number>();
    const scope = yield* Effect.scope;
    let running = false;
    let ordinary = 0;
    let probe = 0;
    interface RequestEntry {
      readonly stream: ReliableByteStream;
      readonly channel: ReturnType<typeof makeRpcWebTransportFrameChannel>;
      readonly writes: Semaphore.Semaphore;
      readonly finished: Deferred.Deferred<void>;
      readonly releaseCredit: () => void;
      terminal?: Deferred.Deferred<void>;
    }
    const requests = new Map<string | number, RequestEntry>();
    const streams = Semaphore.makeUnsafe(limits.ordinaryRequestStreams + 1);
    yield* lifetime.closed.pipe(Effect.andThen(Queue.offer(disconnects, 0)), Effect.forkScoped);
    const serveStream = (
      stream: ReliableByteStream,
      receive: (clientId: number, message: RpcMessage.FromClientEncoded) => Effect.Effect<void>,
    ) =>
      Effect.suspend(() => {
        const channel = makeRpcWebTransportFrameChannel(stream, input, output, limits);
        let requestId: string | number | undefined;
        let isProbe = false;
        let counted = false;
        let completed = false;
        const finished = Deferred.makeUnsafe<void>();
        const releaseCredit = () => {
          if (!counted) return;
          counted = false;
          if (isProbe) probe--;
          else ordinary--;
        };
        return Effect.gen(function* () {
          const role = yield* channel.readPreface();
          if (role !== "request")
            return yield* Effect.fail(
              new RpcWebTransportError("protocol", "Duplicate control stream"),
            );
          const first = yield* channel
            .read({ first: true })
            .pipe(
              Effect.flatMap((value) =>
                decodeTransportEnvelope(ClientRequestEnvelopeSchema, value),
              ),
            );
          if (first._tag !== "Request" || first.isNotification)
            return yield* Effect.fail(
              new RpcWebTransportError("protocol", "Request stream must start with a Request"),
            );
          requestId = first.id;
          if (requests.has(requestId))
            return yield* Effect.fail(
              new RpcWebTransportError("protocol", "Duplicate active request ID"),
            );
          isProbe =
            first.tag === "server.probe" ||
            (first.tag === "server.getConfig" &&
              first.headers.some(([key, value]) => key === "t3-rpc-probe" && value === "1"));
          if (isProbe ? probe >= 1 : ordinary >= limits.ordinaryRequestStreams)
            return yield* Effect.fail(
              new RpcWebTransportError("capacity", "RPC request stream capacity exceeded"),
            );
          if (isProbe) probe++;
          else ordinary++;
          counted = true;
          const entry: RequestEntry = {
            stream,
            channel,
            writes: Semaphore.makeUnsafe(1),
            finished,
            releaseCredit,
          };
          requests.set(requestId, entry);
          yield* receive(0, {
            ...first,
            headers: first.headers.filter(([key]) => key !== "t3-rpc-probe"),
          });
          while (true) {
            const value = yield* channel.read();
            if (value === null) {
              if (!entry.terminal)
                return yield* Effect.fail(
                  new RpcWebTransportError("protocol", "Request FIN before Exit"),
                );
              // Exit can reach the client before our write Promise settles. Keep
              // this request's ownership until the terminal send and local FIN finish.
              yield* Deferred.await(entry.terminal);
              completed = true;
              lifetime.release(stream);
              return;
            }
            const message = yield* decodeTransportEnvelope(ClientRequestEnvelopeSchema, value);
            if (message._tag === "Request" || message.requestId !== requestId)
              return yield* Effect.fail(
                new RpcWebTransportError("protocol", "Cross-stream request ID"),
              );
            // An Ack already in flight when Exit was sent remains legal. No further Request
            // is legal on this stream, and its FIN deadline is enforced by send below.
            if (!entry.terminal) yield* receive(0, message);
          }
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              if (requestId !== undefined) requests.delete(requestId);
              releaseCredit();
              if (!completed) {
                stream.reset(1);
                stream.stop(1);
                lifetime.release(stream);
              }
            }).pipe(Effect.andThen(Deferred.succeed(finished, undefined))),
          ),
        );
      });
    const finishRequest = (
      entry: RequestEntry,
      message: RpcMessage.ResponseExitEncoded,
      terminal: Deferred.Deferred<void>,
    ) =>
      entry.channel.write(message).pipe(
        // FIN can reach the client before its local Promise settles. Return class
        // credit first; the map entry and aggregate stream permit still own cleanup.
        Effect.andThen(Effect.sync(entry.releaseCredit)),
        Effect.andThen(
          entry.channel.finish.pipe(
            Effect.andThen(Deferred.succeed(terminal, undefined)),
            Effect.andThen(Deferred.await(entry.finished)),
            Effect.timeoutOrElse({
              duration: RPC_WEBTRANSPORT_LIMITS.assemblyIdleTimeoutMs,
              orElse: () =>
                Effect.fail(new RpcWebTransportError("protocol", "Request FIN stalled")),
            }),
          ),
        ),
        lifetime.race,
        Effect.catch((cause) => lifetime.close(cause).pipe(Effect.uninterruptible)),
        Effect.interruptible,
      );
    const protocol: RpcServer.Protocol["Service"] = {
      run(receive) {
        return Effect.gen(function* () {
          if (running)
            return yield* Effect.die(
              new RpcWebTransportError("protocol", "RPC server already registered"),
            );
          running = true;
          // RpcServer.make invokes run with its real receive function only after building
          // the authenticated handlers. Publishing Ready here is the registration milestone.
          yield* control.write(
            {
              _tag: "Ready",
              wireVersion: RPC_WEBTRANSPORT_WIRE_VERSION,
              orchestrationProtocolVersion: expected.orchestrationProtocolVersion,
              environmentId: expected.environmentId,
              encoding: "deflate",
              limits,
            },
            true,
          );
          yield* Effect.gen(function* () {
            while (true) {
              const value = yield* control.read({ control: true });
              const message = yield* decodeTransportEnvelope(ClientControlEnvelopeSchema, value);
              if (message._tag === "Ping") yield* control.write({ _tag: "Pong" }, true);
              else {
                yield* receive(0, message).pipe(Effect.forkIn(scope));
                return yield* lifetime.close(
                  new RpcWebTransportError("network", "RPC client ended session"),
                );
              }
            }
          }).pipe(lifetime.race, Effect.catch(lifetime.close), Effect.forkIn(scope));
          while (true) {
            // Acquire before accepting, so unprefaced streams also consume application credit.
            yield* streams.take(1);
            const stream = lifetime.own(
              yield* transportTry((signal) => session.acceptBidirectionalStream(signal)),
            );
            yield* serveStream(stream, receive).pipe(
              lifetime.race,
              Effect.catch(lifetime.close),
              Effect.ensuring(streams.release(1)),
              Effect.forkIn(scope),
            );
          }
        }).pipe(lifetime.race, Effect.catch(lifetime.close), Effect.andThen(Effect.never));
      },
      disconnects,
      send(clientId, message) {
        return Effect.suspend(() => {
          if (clientId !== 0 || lifetime.error) return Effect.void;
          if (message._tag === "ClientProtocolError")
            return lifetime.close(
              new RpcWebTransportError("protocol", "Local RPC server protocol failure"),
            );
          if (message._tag === "Chunk" || message._tag === "Exit") {
            const entry = requests.get(message.requestId);
            if (!entry)
              return lifetime.close(
                new RpcWebTransportError("protocol", "Response for unknown request"),
              );
            return entry.writes.withPermits(1)(
              Effect.suspend(() => {
                if (entry.terminal)
                  return lifetime.close(
                    new RpcWebTransportError("protocol", "Response after Exit"),
                  );
                if (message._tag === "Exit")
                  return Effect.uninterruptibleMask(() => {
                    // Publish terminal intent before any bytes can reach the peer.
                    // RpcServer's finalizer only schedules this session-owned send.
                    const terminal = (entry.terminal = Deferred.makeUnsafe<void>());
                    return finishRequest(entry, message, terminal).pipe(
                      Effect.forkIn(scope),
                      Effect.asVoid,
                    );
                  });
                return entry.channel
                  .write(message)
                  .pipe(lifetime.race, Effect.catch(lifetime.close));
              }),
            );
          }
          return control.write(message, true).pipe(lifetime.race, Effect.catch(lifetime.close));
        });
      },
      end: () => lifetime.close(),
      clientIds: Effect.sync(() => (lifetime.error ? new Set<number>() : new Set([0]))),
      initialMessage: Effect.succeed(Option.none()),
      supportsAck: true,
      supportsTransferables: false,
      supportsSpanPropagation: true,
      supportsNotifications: false,
      codecFor: RpcSerialization.json.codecFor,
    };
    return { protocol, closed: lifetime.closed, close: lifetime.close() };
  }).pipe(
    Effect.tapError(lifetime.close),
    Effect.onInterrupt(() => lifetime.close()),
  );
});
