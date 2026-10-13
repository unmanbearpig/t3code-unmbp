// Level 2 workloads: the level 1 shapes, driven through RpcClient calls.
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { RpcClientError } from "effect/rpc";
import * as Stream from "effect/Stream";

import type { TlsMaterial } from "./client.ts";
import { wallClockMs } from "./protocol.ts";
import { makeRpcClient, type RpcTransportName } from "./rpcClient.ts";
import { type Metrics, summarize } from "./workloads.ts";

export interface RpcWorkload {
  readonly name: string;
  readonly description: string;
  readonly run: (
    transport: RpcTransportName,
    tls: TlsMaterial,
  ) => Effect.Effect<Metrics, RpcClientError.RpcClientError>;
}

const pingText = "x".repeat(256);

const collectSubscriptions = (
  client: Effect.Success<ReturnType<typeof makeRpcClient>>,
  subscriptions: number,
  count: number,
  intervalMs: number,
) =>
  Effect.gen(function* () {
    const latencies: number[] = [];
    const stalls: number[] = [];
    yield* Effect.forEach(
      Array.from({ length: subscriptions }),
      () => {
        let lastAt = 0;
        return client.subscribe({ count, intervalMs, bytes: 512 }).pipe(
          Stream.runForEach((event) =>
            Effect.sync(() => {
              const receivedAt = wallClockMs();
              latencies.push(receivedAt - event.sentAt);
              if (lastAt !== 0) stalls.push(Math.max(0, receivedAt - lastAt - intervalMs));
              lastAt = receivedAt;
            }),
          ),
        );
      },
      { concurrency: "unbounded", discard: true },
    );
    return { ...summarize("latency.", latencies), ...summarize("stall.", stalls) };
  });

export const rpcWorkloads: ReadonlyArray<RpcWorkload> = [
  {
    name: "connect",
    description: "5 cold connections, each until its first ping RPC returns",
    run: (transport, tls) =>
      Effect.gen(function* () {
        const samples: number[] = [];
        for (let i = 0; i < 5; i++) {
          const startedAt = wallClockMs();
          yield* Effect.scoped(
            Effect.flatMap(makeRpcClient(transport, tls), (client) =>
              client.ping({ text: pingText }),
            ),
          );
          samples.push(wallClockMs() - startedAt);
        }
        return summarize("connectAndPing.", samples);
      }),
  },
  {
    name: "rpc",
    description: "200 sequential ping RPCs",
    run: (transport, tls) =>
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* makeRpcClient(transport, tls);
          const rtts: number[] = [];
          for (let i = 0; i < 200; i++) {
            const startedAt = wallClockMs();
            yield* client.ping({ text: pingText });
            rtts.push(wallClockMs() - startedAt);
          }
          return summarize("rtt.", rtts);
        }),
      ),
  },
  {
    name: "stream",
    description: "one subscription: 500 events of 512B every 10ms",
    run: (transport, tls) =>
      Effect.scoped(
        Effect.flatMap(makeRpcClient(transport, tls), (client) =>
          collectSubscriptions(client, 1, 500, 10),
        ),
      ),
  },
  {
    name: "fanout",
    description: "8 subscriptions at once: 200 events of 512B every 10ms each",
    run: (transport, tls) =>
      Effect.scoped(
        Effect.flatMap(makeRpcClient(transport, tls), (client) =>
          collectSubscriptions(client, 8, 200, 10),
        ),
      ),
  },
  {
    name: "hol",
    description: "2000-message (~4MB JSON) thread snapshot while pinging every 20ms",
    run: (transport, tls) =>
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* makeRpcClient(transport, tls);
          // Warm the connection so the snapshot does not pay for the handshake.
          yield* client.ping({ text: pingText });
          const startedAt = wallClockMs();
          const snapshot = yield* Effect.forkChild(
            Effect.map(
              client.snapshot({ messages: 2000, bytes: 2000 }),
              () => wallClockMs() - startedAt,
            ),
          );
          const rtts: number[] = [];
          const pings: Array<Fiber.Fiber<void, RpcClientError.RpcClientError>> = [];
          while (!snapshot.pollUnsafe()) {
            const sentAt = wallClockMs();
            pings.push(
              yield* Effect.forkChild(
                Effect.map(
                  client.ping({ text: pingText }),
                  () => void rtts.push(wallClockMs() - sentAt),
                ),
              ),
            );
            yield* Effect.sleep("20 millis");
          }
          const snapshotMs = yield* Fiber.join(snapshot);
          yield* Fiber.joinAll(pings);
          return { snapshotMs, ...summarize("pingDuringSnapshot.", rtts) };
        }),
      ),
  },
];
