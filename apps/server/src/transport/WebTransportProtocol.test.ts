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
  } = {},
) {
  const pair = memorySessionPair(options);
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
