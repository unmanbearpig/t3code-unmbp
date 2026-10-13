// Level 2 workloads: the level 1 shapes, driven through RpcClient calls.
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { RpcClientError } from "effect/rpc";
import * as Stream from "effect/Stream";

import type { TlsMaterial } from "./client.ts";
import { wallClockMs } from "./protocol.ts";
import { makeRpcClient, type RpcTransportName } from "./rpcClient.ts";
import type { Recorder } from "./workloads.ts";

export interface RpcWorkload {
  readonly name: string;
  readonly description: string;
  /** Interrupted at the deadline; closing the client's scope closes its connection. */
  readonly run: (
    transport: RpcTransportName,
    tls: TlsMaterial,
    record: Recorder,
  ) => Effect.Effect<void, RpcClientError.RpcClientError>;
}

const pingText = "x".repeat(256);

const collectSubscriptions = (
  client: Effect.Success<ReturnType<typeof makeRpcClient>>,
  record: Recorder,
  subscriptions: number,
  count: number,
  intervalMs: number,
) =>
  Effect.forEach(
    Array.from({ length: subscriptions }),
    () => {
      let lastAt = 0;
      return client.subscribe({ count, intervalMs, bytes: 512 }).pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            const receivedAt = wallClockMs();
            record.sample("latency", receivedAt - event.sentAt);
            if (lastAt !== 0) record.sample("stall", Math.max(0, receivedAt - lastAt - intervalMs));
            lastAt = receivedAt;
          }),
        ),
      );
    },
    { concurrency: "unbounded", discard: true },
  );

export const rpcWorkloads: ReadonlyArray<RpcWorkload> = [
  {
    name: "connect",
    description: "5 cold connections, each until its first ping RPC returns",
    run: (transport, tls, record) =>
      Effect.gen(function* () {
        for (let i = 0; i < 5; i++) {
          const startedAt = wallClockMs();
          yield* Effect.scoped(
            Effect.flatMap(makeRpcClient(transport, tls), (client) =>
              client.ping({ text: pingText }),
            ),
          );
          record.sample("connectAndPing", wallClockMs() - startedAt);
        }
      }),
  },
  {
    name: "rpc",
    description: "200 sequential ping RPCs",
    run: (transport, tls, record) =>
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* makeRpcClient(transport, tls);
          for (let i = 0; i < 200; i++) {
            const startedAt = wallClockMs();
            yield* client.ping({ text: pingText });
            record.sample("rtt", wallClockMs() - startedAt);
          }
        }),
      ),
  },
  {
    name: "stream",
    description: "one subscription: 500 events of 512B every 10ms",
    run: (transport, tls, record) =>
      Effect.scoped(
        Effect.flatMap(makeRpcClient(transport, tls), (client) =>
          collectSubscriptions(client, record, 1, 500, 10),
        ),
      ),
  },
  {
    name: "fanout",
    description: "8 subscriptions at once: 200 events of 512B every 10ms each",
    run: (transport, tls, record) =>
      Effect.scoped(
        Effect.flatMap(makeRpcClient(transport, tls), (client) =>
          collectSubscriptions(client, record, 8, 200, 10),
        ),
      ),
  },
  {
    name: "hol",
    description: "2000-message (~4MB JSON) thread snapshot while pinging every 20ms",
    run: (transport, tls, record) =>
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* makeRpcClient(transport, tls);
          // Warm the connection so the snapshot does not pay for the handshake.
          yield* client.ping({ text: pingText });
          const startedAt = wallClockMs();
          const snapshot = yield* Effect.forkChild(
            Effect.map(client.snapshot({ messages: 2000, bytes: 2000 }), () =>
              record.set("snapshotMs", wallClockMs() - startedAt),
            ),
          );
          const pings: Array<Fiber.Fiber<void, RpcClientError.RpcClientError>> = [];
          while (!snapshot.pollUnsafe()) {
            const sentAt = wallClockMs();
            pings.push(
              yield* Effect.forkChild(
                Effect.map(client.ping({ text: pingText }), () =>
                  record.sample("pingDuringSnapshot", wallClockMs() - sentAt),
                ),
              ),
            );
            yield* Effect.sleep("20 millis");
          }
          yield* Fiber.join(snapshot);
          yield* Fiber.joinAll(pings);
        }),
      ),
  },
];
