import { describe, expect, it } from "@effect/vitest";
import {
  RPC_WEBTRANSPORT_LIMITS,
  createTransportByteBudget,
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
});
