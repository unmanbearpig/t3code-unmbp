// @effect-diagnostics effectFnOpportunity:off - Protocol benchmarks must not add tracing spans to measured operations.
// Level 2 workloads: the level 1 shapes, driven through RpcClient calls.
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { RpcClientError } from "effect/rpc";
import * as Stream from "effect/Stream";

import type { TlsMaterial } from "./client.ts";
import { wallClockMs } from "./protocol.ts";
import { makeRpcClient, type RpcTransportName } from "./rpcClient.ts";
import type { Recorder } from "./workloads.ts";
import { workloadSettings } from "./settings.ts";

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

const pingText = "x".repeat(workloadSettings.pingBytes);

const ping = (client: Effect.Success<ReturnType<typeof makeRpcClient>>, record: Recorder) =>
  client
    .ping({ text: pingText })
    .pipe(
      Effect.tap((response) =>
        Effect.sync(() => record.check(response === pingText, "Invalid ping echo")),
      ),
    );

const collectSubscriptions = (
  client: Effect.Success<ReturnType<typeof makeRpcClient>>,
  record: Recorder,
  subscriptions: number,
  count: number,
  bytes: number,
  intervalMs: number,
) => {
  record.expectSamples("latency", subscriptions * count);
  return Effect.forEach(
    Array.from({ length: subscriptions }),
    () => {
      let lastAt = 0;
      let received = 0;
      return client.subscribe({ count, intervalMs, bytes }).pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            const receivedAt = wallClockMs();
            record.check(
              event.sequence === received++ && event.text.length === bytes,
              "Missing, reordered, or truncated RPC event",
            );
            record.sample("latency", receivedAt - event.sentAt);
            if (lastAt !== 0) record.sample("stall", Math.max(0, receivedAt - lastAt - intervalMs));
            lastAt = receivedAt;
          }),
        ),
      );
    },
    { concurrency: "unbounded", discard: true },
  );
};

export const rpcWorkloads: ReadonlyArray<RpcWorkload> = [
  {
    name: "connect",
    description: `${workloadSettings.connect.count} cold connections, each until its first ping RPC returns`,
    run: Effect.fnUntraced(function* (transport, tls, record) {
      record.expectSamples("connectAndPing", workloadSettings.connect.count);
      for (let i = 0; i < workloadSettings.connect.count; i++) {
        const startedAt = wallClockMs();
        yield* Effect.scoped(
          Effect.gen(function* () {
            const client = yield* makeRpcClient(transport, tls);
            yield* ping(client, record);
            record.sample("connectAndPing", wallClockMs() - startedAt);
          }),
        );
      }
    }),
  },
  {
    name: "rpc",
    description: `${workloadSettings.rpc.count} sequential ping RPCs after a warmup ping`,
    run: (transport, tls, record) =>
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* makeRpcClient(transport, tls);
          record.expectSamples("rtt", workloadSettings.rpc.count);
          for (let i = 0; i < workloadSettings.rpc.warmupCount; i++) yield* ping(client, record);
          for (let i = 0; i < workloadSettings.rpc.count; i++) {
            const startedAt = wallClockMs();
            yield* ping(client, record);
            record.sample("rtt", wallClockMs() - startedAt);
          }
        }),
      ),
  },
  {
    name: "stream",
    description: `one subscription: ${workloadSettings.stream.count} events of ${workloadSettings.stream.bytes}B every ${workloadSettings.stream.intervalMs}ms`,
    run: (transport, tls, record) =>
      Effect.scoped(
        Effect.flatMap(makeRpcClient(transport, tls), (client) =>
          collectSubscriptions(
            client,
            record,
            1,
            workloadSettings.stream.count,
            workloadSettings.stream.bytes,
            workloadSettings.stream.intervalMs,
          ),
        ),
      ),
  },
  {
    name: "fanout",
    description: `${workloadSettings.fanout.subscriptions} subscriptions: ${workloadSettings.fanout.count} events of ${workloadSettings.fanout.bytes}B every ${workloadSettings.fanout.intervalMs}ms each`,
    run: (transport, tls, record) =>
      Effect.scoped(
        Effect.flatMap(makeRpcClient(transport, tls), (client) =>
          collectSubscriptions(
            client,
            record,
            workloadSettings.fanout.subscriptions,
            workloadSettings.fanout.count,
            workloadSettings.fanout.bytes,
            workloadSettings.fanout.intervalMs,
          ),
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
          for (let i = 0; i < workloadSettings.hol.warmupCount; i++) yield* ping(client, record);
          const startedAt = wallClockMs();
          const snapshot = yield* Effect.forkChild(
            Effect.map(
              client.snapshot({
                messages: workloadSettings.hol.messages,
                bytes: workloadSettings.hol.textBytes,
              }),
              (response) => {
                record.set("snapshotMs", wallClockMs() - startedAt);
                record.check(
                  response.messages.length === workloadSettings.hol.messages &&
                    response.messages.every(
                      (message) => message.text.length === workloadSettings.hol.textBytes,
                    ),
                  "Truncated RPC snapshot",
                );
              },
            ),
          );
          const pings: Array<Fiber.Fiber<void, RpcClientError.RpcClientError>> = [];
          while (!snapshot.pollUnsafe()) {
            const sentAt = wallClockMs();
            pings.push(
              yield* Effect.forkChild(
                Effect.map(ping(client, record), () =>
                  record.sample("pingDuringSnapshot", wallClockMs() - sentAt),
                ),
              ),
            );
            yield* Effect.sleep(`${workloadSettings.hol.pingIntervalMs} millis`);
          }
          yield* Fiber.join(snapshot);
          record.expectSamples("pingDuringSnapshot", pings.length);
          yield* Fiber.joinAll(pings);
        }),
      ),
  },
];
