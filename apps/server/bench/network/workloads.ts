// @effect-diagnostics globalTimers:off - Ping and stream timers sit in the measured path, so they stay plain timers.
// Workloads modelled on T3 traffic: interactive RPCs, a streaming agent turn, a large response
// sharing the connection with interactive RPCs, and several thread subscriptions at once.
import { connect, type Connection, type TlsMaterial, type TransportName } from "./client.ts";
import { decodeFrame, encodeFrame, type Frame, Kind, wallClockMs } from "./protocol.ts";

export type Metrics = Record<string, number | string>;

export interface Workload {
  readonly name: string;
  readonly description: string;
  readonly run: (transport: TransportName, tls: TlsMaterial) => Promise<Metrics>;
}

export function summarize(prefix: string, samples: ReadonlyArray<number>): Metrics {
  if (samples.length === 0) return { [`${prefix}n`]: 0 };
  const sorted = [...samples].sort((a, b) => a - b);
  const at = (q: number) => sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))]!;
  return {
    [`${prefix}p50`]: at(0.5),
    [`${prefix}p95`]: at(0.95),
    [`${prefix}p99`]: at(0.99),
    [`${prefix}max`]: sorted[sorted.length - 1]!,
  };
}

/** Routes frames to per-channel listeners so workloads can await specific replies. */
function route(connection: Connection) {
  const listeners = new Map<number, (frame: Frame, receivedAt: number) => void>();
  connection.onFrame = (bytes) => {
    const receivedAt = wallClockMs();
    const frame = decodeFrame(bytes);
    listeners.get(frame.channel)?.(frame, receivedAt);
  };
  return (channel: number, listener: (frame: Frame, receivedAt: number) => void) =>
    listeners.set(channel, listener);
}

const ping = (channel: number, seq: number) =>
  encodeFrame({ kind: Kind.ping, channel, seq, sentAt: wallClockMs(), arg: 0, payloadBytes: 256 });

function requestStream(
  connection: Connection,
  channel: number,
  count: number,
  bytes: number,
  intervalMs: number,
) {
  connection.send(
    channel,
    encodeFrame({
      kind: Kind.streamRequest,
      channel,
      seq: count,
      sentAt: wallClockMs(),
      arg: bytes,
      payload: new Uint8Array([intervalMs]),
    }),
  );
}

/** Collects stream latencies on `channels`; resolves once every channel delivered `count`. */
function collectStreams(
  on: ReturnType<typeof route>,
  channels: ReadonlyArray<number>,
  count: number,
  intervalMs: number,
) {
  const latencies: number[] = [];
  const stalls: number[] = [];
  let remaining = channels.length;
  return {
    latencies,
    stalls,
    done: new Promise<void>((resolve) => {
      for (const channel of channels) {
        let received = 0;
        let lastAt = 0;
        on(channel, (frame, receivedAt) => {
          latencies.push(receivedAt - frame.sentAt);
          // Extra wait beyond the send interval: how long the UI would sit frozen.
          if (lastAt !== 0) stalls.push(Math.max(0, receivedAt - lastAt - intervalMs));
          lastAt = receivedAt;
          received += 1;
          if (received === count && --remaining === 0) resolve();
        });
      }
    }),
  };
}

export const workloads: ReadonlyArray<Workload> = [
  {
    name: "connect",
    description: "5 cold connections, then one 256B ping each",
    run: async (transport, tls) => {
      const connectMs: number[] = [];
      const firstPingMs: number[] = [];
      for (let i = 0; i < 5; i++) {
        const startedAt = wallClockMs();
        const connection = await connect(transport, tls);
        connectMs.push(wallClockMs() - startedAt);
        const on = route(connection);
        await new Promise<void>((resolve) => {
          on(0, (frame, receivedAt) => {
            firstPingMs.push(receivedAt - frame.sentAt);
            resolve();
          });
          connection.send(0, ping(0, 0));
        });
        connection.close();
      }
      return { ...summarize("connect.", connectMs), ...summarize("firstPing.", firstPingMs) };
    },
  },
  {
    name: "rpc",
    description: "200 sequential 256B request/response round trips",
    run: async (transport, tls) => {
      const connection = await connect(transport, tls);
      const on = route(connection);
      const rtts: number[] = [];
      for (let seq = 0; seq < 200; seq++) {
        await new Promise<void>((resolve) => {
          on(0, (frame, receivedAt) => {
            rtts.push(receivedAt - frame.sentAt);
            resolve();
          });
          connection.send(0, ping(0, seq));
        });
      }
      connection.close();
      return summarize("rtt.", rtts);
    },
  },
  {
    name: "stream",
    description: "one subscription: 500 × 512B messages every 10ms (an agent turn streaming)",
    run: async (transport, tls) => {
      const connection = await connect(transport, tls);
      const on = route(connection);
      const collected = collectStreams(on, [1], 500, 10);
      requestStream(connection, 1, 500, 512, 10);
      await collected.done;
      connection.close();
      return {
        ...summarize("latency.", collected.latencies),
        ...summarize("stall.", collected.stalls),
      };
    },
  },
  {
    name: "fanout",
    description: "8 subscriptions at once: 200 × 512B every 10ms each",
    run: async (transport, tls) => {
      const connection = await connect(transport, tls);
      const on = route(connection);
      const channels = [1, 2, 3, 4, 5, 6, 7, 8];
      const collected = collectStreams(on, channels, 200, 10);
      for (const channel of channels) requestStream(connection, channel, 200, 512, 10);
      await collected.done;
      connection.close();
      return {
        ...summarize("latency.", collected.latencies),
        ...summarize("stall.", collected.stalls),
      };
    },
  },
  {
    name: "hol",
    description: "4MB response (long-thread snapshot) while pinging every 20ms on another channel",
    run: async (transport, tls) => {
      const connection = await connect(transport, tls);
      const on = route(connection);
      const rtts: number[] = [];
      let outstanding = 0;
      let allPongs: () => void = () => {};
      on(0, (frame, receivedAt) => {
        rtts.push(receivedAt - frame.sentAt);
        if (--outstanding === 0) allPongs();
      });
      const startedAt = wallClockMs();
      const bulk = new Promise<number>((resolve) =>
        on(1, (_frame, receivedAt) => resolve(receivedAt - startedAt)),
      );
      connection.send(
        1,
        encodeFrame({
          kind: Kind.bulkRequest,
          channel: 1,
          seq: 0,
          sentAt: startedAt,
          arg: 4 * 1024 * 1024,
        }),
      );
      let seq = 0;
      const timer = setInterval(() => {
        outstanding += 1;
        connection.send(0, ping(0, seq++));
      }, 20);
      const bulkMs = await bulk;
      clearInterval(timer);
      if (outstanding > 0) await new Promise<void>((resolve) => (allPongs = resolve));
      connection.close();
      return { bulkMs, ...summarize("pingDuringBulk.", rtts) };
    },
  },
];
