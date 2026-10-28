import { describe, expect, it } from "@effect/vitest";
import {
  RPC_WEBTRANSPORT_LIMITS,
  createTransportByteBudget,
  makeRpcWebTransportFrameChannel,
  rpcWebTransportDefaultLimits,
  transportTry,
  type TransportByteBudget,
  type RpcWebTransportLimits,
} from "@t3tools/shared/rpcWebTransport";
import { memorySessionPair } from "../../../../packages/shared/src/rpcWebTransport.test-support.ts";
import {
  prepareRpcWebTransportClient,
  makeRpcWebTransportClientProtocol,
  withRpcWebTransportProbe,
} from "../../../../packages/client-runtime/src/rpc/webTransportProtocol.ts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Scope from "effect/Scope";
import * as TestClock from "effect/testing/TestClock";
import { Rpc, RpcClient, RpcGroup, RpcMessage, RpcServer } from "effect/rpc";
import { prepareRpcWebTransportServer } from "./WebTransportProtocol.ts";

const expected = { environmentId: "env-test", orchestrationProtocolVersion: 1 };
const budgets = () => ({
  input: createTransportByteBudget(RPC_WEBTRANSPORT_LIMITS.listenerInputBytes),
  output: createTransportByteBudget(RPC_WEBTRANSPORT_LIMITS.listenerOutputBytes),
});

const setup = Effect.fnUntraced(function* (
  options: {
    readonly limits?: Partial<RpcWebTransportLimits>;
    readonly neverAcknowledgeClose?: boolean;
    readonly pair?: ReturnType<typeof memorySessionPair>;
  } = {},
) {
  const pair = options.pair ?? memorySessionPair(options);
  const shared = budgets();
  const prepareServer = yield* prepareRpcWebTransportServer(pair.server, {
    ...expected,
    budgets: shared,
    ...(options.limits ? { limits: options.limits } : {}),
  }).pipe(Effect.forkScoped);
  const prepareClient = yield* prepareRpcWebTransportClient(pair.client, expected).pipe(
    Effect.forkScoped,
  );
  const server = yield* Fiber.join(prepareServer);
  return { ...pair, serverProtocol: server, prepareClient, shared };
});

const Group = RpcGroup.make(
  Rpc.make("echo", { payload: Schema.String, success: Schema.String }),
  Rpc.make("listen", { success: Schema.String, stream: true }),
  Rpc.make("server.probe", { success: Schema.String }),
);

describe("WebTransport Effect protocols", () => {
  describe.each(["ordinary", "probe"] as const)("%s FIN settlement", (kind) => {
    it.effect.each(["release", "dispose", "reject", "capacity"] as const)(
      "preserves class credit while local FIN remains pending: %s",
      (mode) =>
        Effect.gen(function* () {
          const owner = yield* Scope.make();
          yield* Effect.addFinalizer((exit) => Scope.close(owner, exit));
          const gate = Promise.withResolvers<void>();
          yield* Effect.addFinalizer(() => Effect.sync(() => gate.resolve()));
          yield* Effect.gen(function* () {
            const pair = memorySessionPair({ neverAcknowledgeClose: true });
            const published = yield* Deferred.make<void>();
            const clientEof = yield* Deferred.make<void>();
            const settled = yield* Deferred.make<void>();
            const sessionClosed = yield* Deferred.make<void>();
            const subscriptionStopped = yield* Deferred.make<void>();
            const secondStarted = yield* Deferred.make<void>();
            const holdSecond = yield* Deferred.make<void>();
            const source = yield* Queue.make<string>();
            const seen = yield* Queue.make<string>();
            const target = kind === "ordinary" ? 1 : 2;
            let accepted = 0;
            let opened = 0;
            let acceptCalls = 0;
            let classCalls = 0;
            let abortedFin = 0;
            let resets = 0;
            let stops = 0;
            let finishSettled = false;
            const fixture = yield* setup({
              ...(kind === "ordinary"
                ? { limits: { ordinaryRequestStreams: 1, pendingOrdinaryRequests: 1 } }
                : {}),
              pair: {
                ...pair,
                client: {
                  ...pair.client,
                  async openBidirectionalStream(signal) {
                    const stream = await pair.client.openBidirectionalStream(signal);
                    if (opened++ !== target) return stream;
                    return {
                      ...stream,
                      async read(max, signal) {
                        const bytes = await stream.read(max, signal);
                        if (bytes === null) Deferred.doneUnsafe(clientEof, Effect.void);
                        return bytes;
                      },
                    };
                  },
                },
                server: {
                  ...pair.server,
                  async acceptBidirectionalStream(signal) {
                    acceptCalls++;
                    const stream = await pair.server.acceptBidirectionalStream(signal);
                    if (accepted++ !== target) return stream;
                    return {
                      ...stream,
                      async finish(signal) {
                        await stream.finish(signal);
                        Deferred.doneUnsafe(published, Effect.void);
                        const abort = () => {
                          abortedFin++;
                          gate.reject(new Error("Local FIN aborted"));
                        };
                        signal.addEventListener("abort", abort, { once: true });
                        try {
                          if (signal.aborted) abort();
                          await gate.promise;
                          finishSettled = true;
                          Deferred.doneUnsafe(settled, Effect.void);
                        } finally {
                          signal.removeEventListener("abort", abort);
                        }
                      },
                      reset(code) {
                        resets++;
                        stream.reset(code);
                      },
                      stop(code) {
                        stops++;
                        stream.stop(code);
                      },
                    };
                  },
                },
              },
            });
            const classResponse = (value: string) =>
              Effect.gen(function* () {
                classCalls++;
                if (mode === "capacity" && classCalls === 2) {
                  yield* Deferred.succeed(secondStarted, undefined);
                  yield* Deferred.await(holdSecond);
                }
                return value;
              });
            yield* fixture.serverProtocol.closed.pipe(
              Effect.andThen(Deferred.succeed(sessionClosed, undefined)),
              Effect.forkScoped,
            );
            yield* RpcServer.make(Group).pipe(
              Effect.provide(
                Group.toLayer({
                  echo: (text) =>
                    kind === "ordinary" ? classResponse(text) : Effect.succeed(text),
                  "server.probe": () =>
                    kind === "probe" ? classResponse("alive") : Effect.succeed("alive"),
                  listen: () =>
                    Stream.fromQueue(source).pipe(
                      Stream.ensuring(Deferred.succeed(subscriptionStopped, undefined)),
                    ),
                }),
              ),
              Effect.provideService(RpcServer.Protocol, fixture.serverProtocol.protocol),
              Effect.forkScoped,
            );
            const protocol = yield* makeRpcWebTransportClientProtocol(
              yield* Fiber.join(fixture.prepareClient),
            );
            const client = yield* RpcClient.make(Group).pipe(
              Effect.provideService(RpcClient.Protocol, protocol),
            );
            const call = (text: string) =>
              kind === "ordinary" ? client.echo(text) : client["server.probe"]();
            if (kind === "probe") {
              yield* client.listen().pipe(
                Stream.runForEach((value) => Queue.offer(seen, value)),
                Effect.forkScoped,
              );
              yield* Queue.offer(source, "before");
              expect(yield* Queue.take(seen)).toBe("before");
            }
            expect(yield* call("first")).toBe(kind === "ordinary" ? "first" : "alive");
            yield* Deferred.await(published);
            yield* Deferred.await(clientEof);
            yield* TestClock.adjust(0);
            if (mode === "capacity") {
              const second = yield* call("second").pipe(Effect.exit, Effect.forkScoped);
              yield* Deferred.await(secondStarted);
              const probe =
                kind === "ordinary"
                  ? yield* client["server.probe"]().pipe(Effect.forkScoped)
                  : undefined;
              yield* TestClock.adjust(0);
              if (probe) {
                // Both aggregate permits remain owned while the first local FIN is pending.
                expect(acceptCalls).toBe(3);
                expect(probe.pollUnsafe()).toBeUndefined();
              }
              expect(resets).toBe(0);
              expect(stops).toBe(0);
              gate.resolve();
              yield* Deferred.await(settled);
              yield* TestClock.adjust(0);
              if (probe) expect(yield* Fiber.join(probe)).toBe("alive");
              // Cleanup of the first request must not return the second request's credit.
              const stream = yield* transportTry((signal) =>
                fixture.client.openBidirectionalStream(signal),
              );
              const extra = makeRpcWebTransportFrameChannel(
                stream,
                createTransportByteBudget(1024 * 1024),
                createTransportByteBudget(1024 * 1024),
                rpcWebTransportDefaultLimits,
              );
              yield* extra.writePreface("request");
              yield* extra.write({
                _tag: "Request",
                id: "over-capacity",
                tag: kind === "ordinary" ? "echo" : "server.probe",
                payload: kind === "ordinary" ? "extra" : undefined,
                headers: [],
              });
              yield* TestClock.adjust(0);
              expect(classCalls).toBe(2);
              expect(yield* Deferred.isDone(sessionClosed)).toBe(true);
              expect(Exit.isFailure(yield* Fiber.join(second))).toBe(true);
            } else {
              const second = yield* call("second").pipe(Effect.exit);
              expect(second).toMatchObject({
                _tag: "Success",
                value: kind === "ordinary" ? "second" : "alive",
              });
              expect(finishSettled).toBe(false);
              expect(resets).toBe(0);
              expect(stops).toBe(0);
              expect(yield* client["server.probe"]()).toBe("alive");
              if (kind === "probe") {
                yield* Queue.offer(source, "during");
                expect(yield* Queue.take(seen)).toBe("during");
                expect(yield* Deferred.isDone(subscriptionStopped)).toBe(false);
              }
              expect(yield* Deferred.isDone(sessionClosed)).toBe(false);
              if (mode === "release") {
                gate.resolve();
                yield* Deferred.await(settled);
                expect(yield* call("after")).toBe(kind === "ordinary" ? "after" : "alive");
                if (kind === "probe") {
                  yield* Queue.offer(source, "after");
                  expect(yield* Queue.take(seen)).toBe("after");
                  expect(yield* Deferred.isDone(subscriptionStopped)).toBe(false);
                }
                expect(yield* Deferred.isDone(sessionClosed)).toBe(false);
              } else if (mode === "reject") {
                gate.reject(new Error("Local FIN failed"));
                yield* Deferred.await(sessionClosed);
                if (kind === "probe") yield* Deferred.await(subscriptionStopped);
              }
            }
            yield* Scope.close(owner, Exit.void);
            if (mode === "dispose") expect(abortedFin).toBe(1);
            yield* Effect.promise(() => fixture.server.closed);
            expect(fixture.shared.input.usedBytes).toBe(0);
            expect(fixture.shared.output.usedBytes).toBe(0);
          }).pipe(Effect.provideService(Scope.Scope, owner));
        }).pipe(Effect.scoped),
    );
  });

  it.effect.each(["release", "dispose", "reject"] as const)(
    "joins client FIN with the server terminal write before releasing admission: %s",
    (mode) =>
      Effect.gen(function* () {
        const owner = yield* Scope.make();
        yield* Effect.addFinalizer((exit) => Scope.close(owner, exit));
        const gate = Promise.withResolvers<void>();
        yield* Effect.addFinalizer(() => Effect.sync(() => gate.resolve()));
        yield* Effect.gen(function* () {
          const pair = memorySessionPair({ neverAcknowledgeClose: true });
          const exitEnqueued = yield* Deferred.make<void>();
          const clientFin = yield* Deferred.make<void>();
          const serverFin = yield* Deferred.make<void>();
          const probeStarted = yield* Deferred.make<void>();
          const releaseProbe = yield* Deferred.make<void>();
          const subscriptionStopped = yield* Deferred.make<void>();
          const sessionClosed = yield* Deferred.make<void>();
          let accepted = 0;
          let acceptCalls = 0;
          let terminalWrites = 0;
          let abortedWrites = 0;
          let writeSettled = false;
          const fixture = yield* setup({
            limits: { ordinaryRequestStreams: 2, pendingOrdinaryRequests: 1 },
            pair: {
              ...pair,
              server: {
                ...pair.server,
                async acceptBidirectionalStream(signal) {
                  acceptCalls++;
                  const stream = await pair.server.acceptBidirectionalStream(signal);
                  if (accepted++ !== 2) return stream;
                  let writes = 0;
                  return {
                    ...stream,
                    async read(max, signal) {
                      const bytes = await stream.read(max, signal);
                      if (bytes === null) Deferred.doneUnsafe(clientFin, Effect.void);
                      return bytes;
                    },
                    async write(bytes, signal) {
                      await stream.write(bytes, signal);
                      if (++writes !== 2) return;
                      terminalWrites++;
                      Deferred.doneUnsafe(exitEnqueued, Effect.void);
                      const abort = () => {
                        abortedWrites++;
                        gate.reject(new Error("Terminal write aborted"));
                      };
                      signal.addEventListener("abort", abort, { once: true });
                      try {
                        if (signal.aborted) abort();
                        await gate.promise;
                        writeSettled = true;
                      } finally {
                        signal.removeEventListener("abort", abort);
                      }
                    },
                    async finish(signal) {
                      await stream.finish(signal);
                      Deferred.doneUnsafe(serverFin, Effect.void);
                    },
                  };
                },
              },
            },
          });
          const source = yield* Queue.make<string>();
          const seen = yield* Queue.make<string>();
          yield* fixture.serverProtocol.closed.pipe(
            Effect.andThen(Deferred.succeed(sessionClosed, undefined)),
            Effect.forkScoped,
          );
          yield* RpcServer.make(Group).pipe(
            Effect.provide(
              Group.toLayer({
                echo: (text) => Effect.succeed(text),
                listen: () =>
                  Stream.fromQueue(source).pipe(
                    Stream.ensuring(Deferred.succeed(subscriptionStopped, undefined)),
                  ),
                "server.probe": () =>
                  Deferred.succeed(probeStarted, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseProbe)),
                    Effect.as("alive"),
                  ),
              }),
            ),
            Effect.provideService(RpcServer.Protocol, fixture.serverProtocol.protocol),
            Effect.forkScoped,
          );
          const protocol = yield* makeRpcWebTransportClientProtocol(
            yield* Fiber.join(fixture.prepareClient),
          );
          const client = yield* RpcClient.make(Group).pipe(
            Effect.provideService(RpcClient.Protocol, protocol),
          );
          yield* client.listen().pipe(
            Stream.runForEach((value) => Queue.offer(seen, value)),
            Effect.forkScoped,
          );
          yield* Queue.offer(source, "before");
          expect(yield* Queue.take(seen)).toBe("before");
          expect(yield* client.echo("completed")).toBe("completed");
          yield* Deferred.await(exitEnqueued);
          yield* Deferred.await(clientFin);
          expect(writeSettled).toBe(false);
          yield* TestClock.adjust(0);
          expect(yield* Deferred.isDone(sessionClosed)).toBe(false);
          const probe = yield* client["server.probe"]().pipe(Effect.forkScoped);
          yield* Deferred.await(probeStarted);
          // All three request permits remain owned: subscription, terminal write and probe.
          yield* TestClock.adjust(0);
          expect(acceptCalls).toBe(4);
          expect(yield* Deferred.isDone(sessionClosed)).toBe(false);
          expect(yield* Deferred.isDone(subscriptionStopped)).toBe(false);
          yield* Queue.offer(source, "during");
          expect(yield* Queue.take(seen)).toBe("during");
          yield* Deferred.succeed(releaseProbe, undefined);
          expect(yield* Fiber.join(probe)).toBe("alive");
          expect(writeSettled).toBe(false);
          if (mode === "release") {
            gate.resolve();
            yield* Deferred.await(serverFin);
            expect(yield* client.echo("after")).toBe("after");
            expect(yield* client["server.probe"]()).toBe("alive");
            yield* Queue.offer(source, "after");
            expect(yield* Queue.take(seen)).toBe("after");
            expect(yield* Deferred.isDone(sessionClosed)).toBe(false);
            expect(yield* Deferred.isDone(subscriptionStopped)).toBe(false);
          } else if (mode === "reject") {
            gate.reject(new Error("Terminal write failed"));
            yield* Deferred.await(sessionClosed);
            yield* Deferred.await(subscriptionStopped);
          }
          expect(terminalWrites).toBe(1);
          yield* Scope.close(owner, Exit.void);
          if (mode === "dispose") expect(abortedWrites).toBe(1);
          yield* Effect.promise(() => fixture.server.closed);
        }).pipe(Effect.provideService(Scope.Scope, owner));
      }).pipe(Effect.scoped),
  );

  it.effect("rejects a genuinely premature client FIN before any terminal response", () =>
    Effect.gen(function* () {
      const fixture = yield* setup();
      const requests = yield* Queue.make<RpcMessage.FromClientEncoded>();
      yield* fixture.serverProtocol.protocol
        .run((_id, message) => Queue.offer(requests, message).pipe(Effect.asVoid))
        .pipe(Effect.forkScoped);
      yield* Fiber.join(fixture.prepareClient);
      const stream = yield* transportTry((signal) =>
        fixture.client.openBidirectionalStream(signal),
      );
      const channel = makeRpcWebTransportFrameChannel(
        stream,
        createTransportByteBudget(1024 * 1024),
        createTransportByteBudget(1024 * 1024),
        rpcWebTransportDefaultLimits,
      );
      yield* channel.writePreface("request");
      yield* channel.write({
        _tag: "Request",
        id: "early",
        tag: "echo",
        payload: "x",
        headers: [],
      });
      expect((yield* Queue.take(requests))._tag).toBe("Request");
      yield* channel.finish;
      yield* fixture.serverProtocol.closed;
    }).pipe(Effect.scoped),
  );

  it.effect("keeps queued real-RpcClient cancellation local and releases its admission", () =>
    Effect.gen(function* () {
      const fixture = yield* setup({
        limits: { ordinaryRequestStreams: 1, pendingOrdinaryRequests: 1 },
      });
      const listening = yield* Deferred.make<void>();
      const stopped = yield* Deferred.make<void>();
      const echoes: string[] = [];
      yield* RpcServer.make(Group).pipe(
        Effect.provide(
          Group.toLayer({
            echo: (text) =>
              Effect.sync(() => {
                echoes.push(text);
                return text;
              }),
            listen: () =>
              Stream.unwrap(
                Deferred.succeed(listening, undefined).pipe(Effect.as(Stream.never)),
              ).pipe(Stream.ensuring(Deferred.succeed(stopped, undefined))),
            "server.probe": () => Effect.succeed("alive"),
          }),
        ),
        Effect.provideService(RpcServer.Protocol, fixture.serverProtocol.protocol),
        Effect.forkScoped,
      );
      const protocol = yield* makeRpcWebTransportClientProtocol(
        yield* Fiber.join(fixture.prepareClient),
      );
      const client = yield* RpcClient.make(Group).pipe(
        Effect.provideService(RpcClient.Protocol, protocol),
      );
      const subscription = yield* client.listen().pipe(Stream.runDrain, Effect.forkScoped);
      yield* Deferred.await(listening);
      const queued = yield* client.echo("cancelled").pipe(Effect.forkScoped);
      yield* TestClock.adjust(0);
      expect(queued.pollUnsafe()).toBeUndefined();
      expect(fixture.opened()).toBe(2);
      yield* Fiber.interrupt(queued);
      expect(fixture.opened()).toBe(2);
      expect(echoes).toEqual([]);
      expect(yield* client["server.probe"]()).toBe("alive");
      yield* Fiber.interrupt(subscription);
      yield* Deferred.await(stopped);
      expect(yield* client.echo("after")).toBe("after");
      expect(echoes).toEqual(["after"]);
    }).pipe(Effect.scoped),
  );

  it.effect("still closes the session on an unexpected peer reset during Request assembly", () =>
    Effect.gen(function* () {
      const fixture = yield* setup();
      yield* fixture.serverProtocol.protocol.run(() => Effect.void).pipe(Effect.forkScoped);
      yield* Fiber.join(fixture.prepareClient);
      const stream = yield* transportTry((signal) =>
        fixture.client.openBidirectionalStream(signal),
      );
      const channel = makeRpcWebTransportFrameChannel(
        stream,
        createTransportByteBudget(1024 * 1024),
        createTransportByteBudget(1024 * 1024),
        rpcWebTransportDefaultLimits,
      );
      yield* channel.writePreface("request");
      stream.reset(1);
      yield* fixture.serverProtocol.closed;
    }).pipe(Effect.scoped),
  );

  it.effect.each([
    { write: 1, dispose: false },
    { write: 2, dispose: false },
    { write: 3, dispose: false },
    { write: 3, dispose: true },
  ])("keeps cancellation during initial Request write session-owned: %j", (test) =>
    Effect.gen(function* () {
      const owner = yield* Scope.make();
      yield* Effect.addFinalizer((exit) => Scope.close(owner, exit));
      yield* Effect.gen(function* () {
        const pair = memorySessionPair({ neverAcknowledgeClose: true });
        const blocked = yield* Deferred.make<void>();
        const release = Promise.withResolvers<void>();
        let opened = 0;
        let resets = 0;
        let abortedWrites = 0;
        const fixture = yield* setup({
          pair: {
            ...pair,
            client: {
              ...pair.client,
              async openBidirectionalStream(signal) {
                const stream = await pair.client.openBidirectionalStream(signal);
                if (opened++ !== 2) return stream;
                let writes = 0;
                return {
                  ...stream,
                  async write(bytes, signal) {
                    if (++writes !== test.write) return stream.write(bytes, signal);
                    Deferred.doneUnsafe(blocked, Effect.void);
                    const abort = () => {
                      abortedWrites++;
                      release.reject(new Error("Gated write aborted"));
                    };
                    signal.addEventListener("abort", abort, { once: true });
                    try {
                      if (signal.aborted) abort();
                      await release.promise;
                      await stream.write(bytes, signal);
                    } finally {
                      signal.removeEventListener("abort", abort);
                    }
                  },
                  reset(code) {
                    resets++;
                    stream.reset(code);
                  },
                };
              },
            },
          },
        });
        const group = RpcGroup.make(
          Rpc.make("listen", { success: Schema.String, stream: true }),
          Rpc.make("upload", { payload: Schema.String, success: Schema.String }),
          Rpc.make("server.probe", { success: Schema.String }),
        );
        const source = yield* Queue.make<string>();
        const seen = yield* Queue.make<string>();
        const interrupts = yield* Queue.make<RpcMessage.FromClientEncoded>();
        let unrelatedInterrupted = false;
        let sessionClosed = false;
        yield* fixture.serverProtocol.closed.pipe(
          Effect.andThen(
            Effect.sync(() => {
              sessionClosed = true;
            }),
          ),
          Effect.forkScoped,
        );
        yield* RpcServer.make(group).pipe(
          Effect.provide(
            group.toLayer({
              listen: () =>
                Stream.fromQueue(source).pipe(
                  Stream.ensuring(
                    Effect.sync(() => {
                      unrelatedInterrupted = true;
                    }),
                  ),
                ),
              upload: () => Effect.never,
              "server.probe": () => Effect.succeed("alive"),
            }),
          ),
          Effect.provideService(RpcServer.Protocol, {
            ...fixture.serverProtocol.protocol,
            run(receive) {
              return fixture.serverProtocol.protocol.run((id, message) =>
                (message._tag === "Interrupt"
                  ? Queue.offer(interrupts, message)
                  : Effect.void
                ).pipe(Effect.andThen(receive(id, message))),
              );
            },
          }),
          Effect.forkScoped,
        );
        const protocol = yield* makeRpcWebTransportClientProtocol(
          yield* Fiber.join(fixture.prepareClient),
        );
        const client = yield* RpcClient.make(group).pipe(
          Effect.provideService(RpcClient.Protocol, protocol),
        );
        yield* client.listen().pipe(
          Stream.runForEach((value) => Queue.offer(seen, value)),
          Effect.forkScoped,
        );
        yield* Queue.offer(source, "before");
        expect(yield* Queue.take(seen)).toBe("before");
        const upload = yield* client.upload("cancelled").pipe(Effect.forkScoped);
        yield* Deferred.await(blocked);
        yield* Fiber.interrupt(upload);
        expect(resets).toBe(0);
        expect(abortedWrites).toBe(0);
        expect(yield* client["server.probe"]()).toBe("alive");
        if (!test.dispose) {
          release.resolve();
          expect((yield* Queue.take(interrupts))._tag).toBe("Interrupt");
          expect(Queue.sizeUnsafe(interrupts)).toBe(0);
          expect(yield* client["server.probe"]()).toBe("alive");
          yield* Queue.offer(source, "after");
          expect(yield* Queue.take(seen)).toBe("after");
          expect(resets).toBe(0);
          expect(sessionClosed).toBe(false);
          expect(unrelatedInterrupted).toBe(false);
        }
        yield* Scope.close(owner, Exit.void);
        if (test.dispose) expect(abortedWrites).toBe(1);
        expect((yield* Effect.promise(() => fixture.client.closed)).category).toBe("closed");
      }).pipe(Effect.provideService(Scope.Scope, owner));
    }).pipe(Effect.scoped),
  );

  it.effect("publishes Ready only after the server receive callback is registered", () =>
    Effect.gen(function* () {
      const fixture = yield* setup();
      expect(fixture.prepareClient.pollUnsafe()).toBeUndefined();
      const requests = yield* Queue.make<RpcMessage.FromClientEncoded>();
      yield* fixture.serverProtocol.protocol
        .run((_id, message) => Queue.offer(requests, message).pipe(Effect.asVoid))
        .pipe(Effect.forkScoped);
      const prepared = yield* Fiber.join(fixture.prepareClient);
      const protocol = yield* makeRpcWebTransportClientProtocol(prepared);
      const responses = yield* Queue.make<RpcMessage.FromServerEncoded>();
      yield* protocol
        .run(7, (message) => Queue.offer(responses, message).pipe(Effect.asVoid))
        .pipe(Effect.forkScoped);
      yield* protocol.send(7, {
        _tag: "Request",
        id: "one",
        tag: "echo",
        payload: "first",
        headers: [],
      });
      expect(yield* Queue.take(requests)).toMatchObject({
        _tag: "Request",
        id: "one",
        payload: "first",
      });
      yield* fixture.serverProtocol.protocol.send(0, {
        _tag: "Exit",
        requestId: "one",
        exit: { _tag: "Success", value: "done" },
      });
      expect(yield* Queue.take(responses)).toMatchObject({ _tag: "Exit", requestId: "one" });
    }).pipe(Effect.scoped),
  );

  it.effect("runs the real RpcClient/RpcServer schema codecs, stream Ack and Interrupt", () =>
    Effect.gen(function* () {
      const fixture = yield* setup();
      const interrupted = yield* Deferred.make<void>();
      const source = yield* Queue.make<string>();
      yield* RpcServer.make(Group).pipe(
        Effect.provide(
          Group.toLayer({
            echo: (text) => Effect.succeed(text),
            listen: () =>
              Stream.fromQueue(source).pipe(
                Stream.ensuring(Deferred.succeed(interrupted, undefined)),
              ),
            "server.probe": () => Effect.succeed("alive"),
          }),
        ),
        Effect.provideService(RpcServer.Protocol, fixture.serverProtocol.protocol),
        Effect.forkScoped,
      );
      const protocol = yield* makeRpcWebTransportClientProtocol(
        yield* Fiber.join(fixture.prepareClient),
      );
      const client = yield* RpcClient.make(Group).pipe(
        Effect.provideService(RpcClient.Protocol, protocol),
      );
      expect(yield* client.echo("hello €")).toBe("hello €");
      const seen = yield* Queue.make<string>();
      const consumer = yield* client.listen().pipe(
        Stream.runForEach((value) => Queue.offer(seen, value)),
        Effect.forkScoped,
      );
      yield* Queue.offer(source, "first");
      expect(yield* Queue.take(seen)).toBe("first");
      yield* Queue.offer(source, "second");
      expect(yield* Queue.take(seen)).toBe("second");
      yield* Fiber.interrupt(consumer);
      yield* Deferred.await(interrupted);
      expect(yield* client["server.probe"]()).toBe("alive");
    }).pipe(Effect.scoped),
  );

  it.effect(
    "keeps a real probe moving with 63 subscriptions and releases a queued request after interruption",
    () =>
      Effect.gen(function* () {
        const fixture = yield* setup();
        const arrived = yield* Queue.make<RpcMessage.RequestEncoded>();
        yield* fixture.serverProtocol.protocol
          .run((_id, message) => {
            if (message._tag === "Request") {
              if (message.tag === "server.probe")
                return fixture.serverProtocol.protocol.send(0, {
                  _tag: "Exit",
                  requestId: message.id,
                  exit: { _tag: "Success", value: "alive" },
                });
              return Queue.offer(arrived, message).pipe(Effect.asVoid);
            }
            if (message._tag === "Interrupt")
              return fixture.serverProtocol.protocol.send(0, {
                _tag: "Exit",
                requestId: message.requestId,
                exit: { _tag: "Failure", cause: [{ _tag: "Interrupt", fiberId: null }] },
              });
            return Effect.void;
          })
          .pipe(Effect.forkScoped);
        const protocol = yield* makeRpcWebTransportClientProtocol(
          yield* Fiber.join(fixture.prepareClient),
        );
        const replies = yield* Queue.make<RpcMessage.FromServerEncoded>();
        yield* protocol
          .run(1, (message) => Queue.offer(replies, message).pipe(Effect.asVoid))
          .pipe(Effect.forkScoped);
        for (let i = 0; i < 63; i++) {
          yield* protocol.send(1, {
            _tag: "Request",
            id: i,
            tag: "listen",
            payload: null,
            headers: [],
          });
          expect((yield* Queue.take(arrived)).id).toBe(i);
        }
        const queued = yield* protocol
          .send(1, { _tag: "Request", id: 64, tag: "echo", payload: "queued", headers: [] })
          .pipe(Effect.forkScoped);
        yield* protocol.send(1, {
          _tag: "Request",
          id: "probe",
          tag: "server.probe",
          payload: null,
          headers: [],
        });
        expect(yield* Queue.take(replies)).toMatchObject({ _tag: "Exit", requestId: "probe" });
        expect(fixture.opened()).toBe(65);
        yield* protocol.send(1, { _tag: "Interrupt", requestId: 0 });
        yield* Fiber.join(queued);
        expect((yield* Queue.take(arrived)).id).toBe(64);
        expect(fixture.opened()).toBe(66);
      }).pipe(Effect.scoped),
  );

  it.effect(
    "retains a failure between Ready and publication for future clients and never waits for native close",
    () =>
      Effect.gen(function* () {
        const fixture = yield* setup({ neverAcknowledgeClose: true });
        yield* fixture.serverProtocol.protocol.run(() => Effect.void).pipe(Effect.forkScoped);
        const prepared = yield* Fiber.join(fixture.prepareClient);
        yield* fixture.serverProtocol.close;
        // The transport wrapper resolves its own logical closed promise independently.
        yield* Effect.promise(() => fixture.server.closed);
        yield* TestClock.adjust(0);
        const protocol = yield* makeRpcWebTransportClientProtocol(prepared);
        const responses = yield* Queue.make<RpcMessage.FromServerEncoded>();
        yield* protocol
          .run(33, (message) => Queue.offer(responses, message).pipe(Effect.asVoid))
          .pipe(Effect.forkScoped);
        expect((yield* Queue.take(responses))._tag).toBe("ClientProtocolError");
        expect(
          Exit.isFailure(
            yield* protocol
              .send(33, { _tag: "Request", id: 1, tag: "echo", payload: null, headers: [] })
              .pipe(Effect.exit),
          ),
        ).toBe(true);
        yield* fixture.serverProtocol.closed;
      }).pipe(Effect.scoped),
  );

  it.effect("bounds and cancels queued opens and marks only a deliberate legacy config probe", () =>
    Effect.gen(function* () {
      const fixture = yield* setup({
        limits: { ordinaryRequestStreams: 1, pendingOrdinaryRequests: 1 },
      });
      const requests = yield* Queue.make<RpcMessage.RequestEncoded>();
      yield* fixture.serverProtocol.protocol
        .run((_id, message) =>
          message._tag === "Request"
            ? Queue.offer(requests, message).pipe(Effect.asVoid)
            : Effect.void,
        )
        .pipe(Effect.forkScoped);
      const client = yield* makeRpcWebTransportClientProtocol(
        yield* Fiber.join(fixture.prepareClient),
      );
      yield* client.run(1, () => Effect.void).pipe(Effect.forkScoped);
      yield* client.send(1, {
        _tag: "Request",
        id: "active",
        tag: "listen",
        payload: null,
        headers: [],
      });
      expect((yield* Queue.take(requests)).id).toBe("active");
      const queued = yield* client
        .send(1, {
          _tag: "Request",
          id: "queued",
          tag: "server.getConfig",
          payload: null,
          headers: [],
        })
        .pipe(Effect.exit, Effect.forkScoped);
      yield* TestClock.adjust(0);
      expect(queued.pollUnsafe()).toBeUndefined();
      const excess = yield* client
        .send(1, { _tag: "Request", id: "excess", tag: "listen", payload: null, headers: [] })
        .pipe(Effect.exit);
      expect(Exit.isFailure(excess)).toBe(true);
      yield* client
        .send(1, {
          _tag: "Request",
          id: "probe",
          tag: "server.getConfig",
          payload: null,
          headers: [],
        })
        .pipe(withRpcWebTransportProbe);
      const probe = yield* Queue.take(requests);
      expect(probe.id).toBe("probe");
      expect(probe.headers).toEqual([]);
      yield* client.send(1, { _tag: "Interrupt", requestId: "queued" });
      expect(Exit.isFailure(yield* Fiber.join(queued))).toBe(true);
      const next = yield* client
        .send(1, { _tag: "Request", id: "next", tag: "listen", payload: null, headers: [] })
        .pipe(Effect.exit, Effect.forkScoped);
      yield* TestClock.adjust(0);
      expect(next.pollUnsafe()).toBeUndefined();
      yield* client.send(1, { _tag: "Interrupt", requestId: "next" });
      expect(Exit.isFailure(yield* Fiber.join(next))).toBe(true);
    }).pipe(Effect.scoped),
  );
  it.effect("carries the legacy probe marker through the installed RpcClient call context", () =>
    Effect.gen(function* () {
      const fixture = yield* setup({
        limits: { ordinaryRequestStreams: 1, pendingOrdinaryRequests: 1 },
      });
      const group = RpcGroup.make(
        Rpc.make("listen", { success: Schema.String, stream: true }),
        Rpc.make("server.getConfig", { success: Schema.String }),
      );
      const started = yield* Deferred.make<void>();
      yield* RpcServer.make(group).pipe(
        Effect.provide(
          group.toLayer({
            listen: () =>
              Stream.unwrap(Deferred.succeed(started, undefined).pipe(Effect.as(Stream.never))),
            "server.getConfig": () => Effect.succeed("legacy-alive"),
          }),
        ),
        Effect.provideService(RpcServer.Protocol, fixture.serverProtocol.protocol),
        Effect.forkScoped,
      );
      const protocol = yield* makeRpcWebTransportClientProtocol(
        yield* Fiber.join(fixture.prepareClient),
      );
      const client = yield* RpcClient.make(group).pipe(
        Effect.provideService(RpcClient.Protocol, protocol),
      );
      const subscription = yield* client.listen().pipe(Stream.runDrain, Effect.forkScoped);
      yield* Deferred.await(started);
      const ordinary = yield* client["server.getConfig"]().pipe(Effect.exit, Effect.forkScoped);
      yield* TestClock.adjust(0);
      expect(ordinary.pollUnsafe()).toBeUndefined();
      expect(yield* client["server.getConfig"]().pipe(withRpcWebTransportProbe)).toBe(
        "legacy-alive",
      );
      yield* Fiber.interrupt(ordinary);
      yield* Fiber.interrupt(subscription);
    }).pipe(Effect.scoped),
  );
  it.effect(
    "preserves control progress while two sessions exhaust the shared listener ceilings",
    () =>
      Effect.gen(function* () {
        const shared = budgets();
        const inputSessions: TransportByteBudget[] = [];
        const outputSessions: TransportByteBudget[] = [];
        const instrument = (
          budget: TransportByteBudget,
          sessions: TransportByteBudget[],
        ): TransportByteBudget => ({
          ...budget,
          get usedBytes() {
            return budget.usedBytes;
          },
          child(max) {
            const child = budget.child(max);
            sessions.push(child);
            return child;
          },
        });
        const sharedBudgets = {
          input: instrument(shared.input, inputSessions),
          output: instrument(shared.output, outputSessions),
        };
        const open = Effect.fnUntraced(function* () {
          const owner = yield* Scope.make();
          yield* Effect.addFinalizer((exit) => Scope.close(owner, exit));
          const pair = memorySessionPair({ neverAcknowledgeClose: true });
          yield* Effect.addFinalizer(() => Effect.sync(() => pair.client.close(0)));
          const preparing = yield* prepareRpcWebTransportServer(pair.server, {
            ...expected,
            budgets: sharedBudgets,
          }).pipe(Effect.provideService(Scope.Scope, owner), Effect.forkIn(owner));
          const stream = yield* transportTry((signal) =>
            pair.client.openBidirectionalStream(signal),
          );
          const control = makeRpcWebTransportFrameChannel(
            stream,
            createTransportByteBudget(65536),
            createTransportByteBudget(65536),
            rpcWebTransportDefaultLimits,
          );
          yield* control.writePreface("control");
          yield* control.write(
            {
              _tag: "Hello",
              wireVersion: 1,
              orchestrationProtocolVersion: 1,
              encodings: ["identity", "deflate"],
              limits: rpcWebTransportDefaultLimits,
            },
            true,
          );
          const server = yield* Fiber.join(preparing);
          yield* server.protocol.run(() => Effect.void).pipe(Effect.forkIn(owner));
          expect(yield* control.read({ control: true })).toMatchObject({ _tag: "Ready" });
          return { control, owner, server };
        });
        const first = yield* open();
        const second = yield* open();
        expect(shared.input.usedBytes).toBe(2 * RPC_WEBTRANSPORT_LIMITS.controlReserveBytes);
        expect(shared.output.usedBytes).toBe(2 * RPC_WEBTRANSPORT_LIMITS.controlReserveBytes);
        const leases = [...inputSessions, ...outputSessions].map((budget) =>
          budget.acquire(budget.maxBytes - RPC_WEBTRANSPORT_LIMITS.controlReserveBytes),
        );
        expect(shared.input.usedBytes).toBe(shared.input.maxBytes);
        expect(shared.output.usedBytes).toBe(shared.output.maxBytes);
        expect(() => inputSessions[0]!.acquire(1)).toThrow("budget exhausted");
        for (const session of [first, second]) {
          yield* session.control.write({ _tag: "Ping" }, true);
          expect(yield* session.control.read({ control: true })).toEqual({ _tag: "Pong" });
        }
        for (const lease of leases) lease.release();
        yield* first.server.close;
        yield* Scope.close(first.owner, Exit.void);
        expect(shared.input.usedBytes).toBe(RPC_WEBTRANSPORT_LIMITS.controlReserveBytes);
        expect(shared.output.usedBytes).toBe(RPC_WEBTRANSPORT_LIMITS.controlReserveBytes);
        yield* second.server.close;
        yield* Scope.close(second.owner, Exit.void);
        expect(shared.input.usedBytes).toBe(0);
        expect(shared.output.usedBytes).toBe(0);
      }).pipe(Effect.scoped),
  );

  it.effect(
    "releases control reservations on rejected Hello and a partial reservation failure",
    () =>
      Effect.gen(function* () {
        const shared = budgets();
        const pair = memorySessionPair({ neverAcknowledgeClose: true });
        const preparing = yield* prepareRpcWebTransportServer(pair.server, {
          ...expected,
          budgets: shared,
        }).pipe(Effect.exit, Effect.forkScoped);
        const stream = yield* transportTry((signal) => pair.client.openBidirectionalStream(signal));
        const control = makeRpcWebTransportFrameChannel(
          stream,
          createTransportByteBudget(65536),
          createTransportByteBudget(65536),
          rpcWebTransportDefaultLimits,
        );
        yield* control.writePreface("control");
        yield* control.write(
          {
            _tag: "Hello",
            wireVersion: 1,
            orchestrationProtocolVersion: 99,
            encodings: ["identity", "deflate"],
            limits: rpcWebTransportDefaultLimits,
          },
          true,
        );
        expect(Exit.isFailure(yield* Fiber.join(preparing))).toBe(true);
        expect(shared.input.usedBytes).toBe(0);
        expect(shared.output.usedBytes).toBe(0);
        const input = createTransportByteBudget(65536);
        const output = createTransportByteBudget(65535);
        const failed = yield* prepareRpcWebTransportServer(memorySessionPair().server, {
          ...expected,
          budgets: { input, output },
        }).pipe(Effect.exit);
        expect(Exit.isFailure(failed)).toBe(true);
        expect(input.usedBytes).toBe(0);
        expect(output.usedBytes).toBe(0);
      }).pipe(Effect.scoped),
  );
  it.effect(
    "releases the reserved capacity when control admission is interrupted without native acknowledgement",
    () =>
      Effect.gen(function* () {
        const shared = budgets();
        const pair = memorySessionPair({ neverAcknowledgeClose: true });
        let signal!: () => void;
        const accepting = new Promise<void>((resolve) => {
          signal = resolve;
        });
        const server = {
          ...pair.server,
          acceptBidirectionalStream(abort: AbortSignal) {
            signal();
            return pair.server.acceptBidirectionalStream(abort);
          },
        };
        const pending = yield* prepareRpcWebTransportServer(server, {
          ...expected,
          budgets: shared,
        }).pipe(Effect.forkScoped);
        yield* Effect.promise(() => accepting);
        expect(shared.input.usedBytes).toBe(RPC_WEBTRANSPORT_LIMITS.controlReserveBytes);
        expect(shared.output.usedBytes).toBe(RPC_WEBTRANSPORT_LIMITS.controlReserveBytes);
        yield* Fiber.interrupt(pending);
        expect(shared.input.usedBytes).toBe(0);
        expect(shared.output.usedBytes).toBe(0);
      }).pipe(Effect.scoped),
  );
});
