// @effect-diagnostics globalTimers:off - Ping and stream timers sit in the measured path, so they stay plain timers.
// Workloads modelled on T3 traffic: interactive RPCs, a streaming agent turn, a large response
// sharing the connection with interactive RPCs, and several thread subscriptions at once.
import { connect, type Connection, type TlsMaterial, type TransportName } from "./client.ts";
import { decodeFrame, encodeFrame, type Frame, Kind, wallClockMs } from "./protocol.ts";

export type Metrics = Record<string, number | string>;

/** Samples recorded as a workload runs, so a run cut off by its deadline still reports them. */
export class Recorder {
  private readonly series = new Map<string, number[]>();
  private readonly values = new Map<string, number>();
  private readonly cleanups: Array<() => void> = [];
  private readonly controller = new AbortController();

  get signal() {
    return this.controller.signal;
  }

  /** Runs when the deadline cuts the workload off, so its timers and sockets do not leak. */
  onAbort(cleanup: () => void) {
    if (this.signal.aborted) cleanup();
    else this.cleanups.push(cleanup);
  }

  abort() {
    this.controller.abort();
    for (const cleanup of this.cleanups.splice(0)) cleanup();
  }

  sample(name: string, value: number) {
    let samples = this.series.get(name);
    if (!samples) this.series.set(name, (samples = []));
    samples.push(value);
  }

  set(name: string, value: number) {
    this.values.set(name, value);
  }

  metrics(): Metrics {
    const metrics: Metrics = {};
    for (const [name, samples] of this.series) {
      const sorted = samples.toSorted((a, b) => a - b);
      const at = (q: number) => sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))]!;
      Object.assign(metrics, {
        [`${name}.n`]: sorted.length,
        [`${name}.p50`]: at(0.5),
        [`${name}.p95`]: at(0.95),
        [`${name}.p99`]: at(0.99),
        [`${name}.max`]: sorted[sorted.length - 1]!,
      });
    }
    for (const [name, value] of this.values) metrics[name] = value;
    return metrics;
  }
}

export interface Workload {
  readonly name: string;
  readonly description: string;
  readonly run: (transport: TransportName, tls: TlsMaterial, record: Recorder) => Promise<void>;
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

/** Records stream latencies on `channels`; resolves once every channel delivered `count`. */
function collectStreams(
  on: ReturnType<typeof route>,
  record: Recorder,
  channels: ReadonlyArray<number>,
  count: number,
  intervalMs: number,
) {
  let remaining = channels.length;
  return new Promise<void>((resolve) => {
    for (const channel of channels) {
      let received = 0;
      let lastAt = 0;
      on(channel, (frame, receivedAt) => {
        record.sample("latency", receivedAt - frame.sentAt);
        // Extra wait beyond the send interval: how long the UI would sit frozen.
        if (lastAt !== 0) record.sample("stall", Math.max(0, receivedAt - lastAt - intervalMs));
        lastAt = receivedAt;
        received += 1;
        if (received === count && --remaining === 0) resolve();
      });
    }
  });
}

export const workloads: ReadonlyArray<Workload> = [
  {
    name: "connect",
    description: "5 cold connections, then one 256B ping each",
    run: async (transport, tls, record) => {
      for (let i = 0; i < 5; i++) {
        const startedAt = wallClockMs();
        const connection = await connect(transport, tls, record.signal);
        record.onAbort(connection.close);
        record.sample("connect", wallClockMs() - startedAt);
        const on = route(connection);
        await new Promise<void>((resolve) => {
          on(0, (frame, receivedAt) => {
            record.sample("firstPing", receivedAt - frame.sentAt);
            resolve();
          });
          connection.send(0, ping(0, 0));
        });
        connection.close();
      }
    },
  },
  {
    name: "rpc",
    description: "200 sequential 256B request/response round trips",
    run: async (transport, tls, record) => {
      const connection = await connect(transport, tls, record.signal);
      record.onAbort(connection.close);
      const on = route(connection);
      for (let seq = 0; seq < 200; seq++) {
        await new Promise<void>((resolve) => {
          on(0, (frame, receivedAt) => {
            record.sample("rtt", receivedAt - frame.sentAt);
            resolve();
          });
          connection.send(0, ping(0, seq));
        });
      }
      connection.close();
    },
  },
  {
    name: "stream",
    description: "one subscription: 500 × 512B messages every 10ms (an agent turn streaming)",
    run: async (transport, tls, record) => {
      const connection = await connect(transport, tls, record.signal);
      record.onAbort(connection.close);
      const done = collectStreams(route(connection), record, [1], 500, 10);
      requestStream(connection, 1, 500, 512, 10);
      await done;
      connection.close();
    },
  },
  {
    name: "fanout",
    description: "8 subscriptions at once: 200 × 512B every 10ms each",
    run: async (transport, tls, record) => {
      const connection = await connect(transport, tls, record.signal);
      record.onAbort(connection.close);
      const channels = [1, 2, 3, 4, 5, 6, 7, 8];
      const done = collectStreams(route(connection), record, channels, 200, 10);
      for (const channel of channels) requestStream(connection, channel, 200, 512, 10);
      await done;
      connection.close();
    },
  },
  {
    name: "hol",
    description: "4MB response (long-thread snapshot) while pinging every 20ms on another channel",
    run: async (transport, tls, record) => {
      const connection = await connect(transport, tls, record.signal);
      record.onAbort(connection.close);
      const on = route(connection);
      let outstanding = 0;
      let allPongs: () => void = () => {};
      on(0, (frame, receivedAt) => {
        record.sample("pingDuringBulk", receivedAt - frame.sentAt);
        if (--outstanding === 0) allPongs();
      });
      const startedAt = wallClockMs();
      const bulk = new Promise<void>((resolve) =>
        on(1, (_frame, receivedAt) => {
          record.set("bulkMs", receivedAt - startedAt);
          resolve();
        }),
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
      record.onAbort(() => clearInterval(timer));
      await bulk;
      clearInterval(timer);
      if (outstanding > 0) await new Promise<void>((resolve) => (allPongs = resolve));
      connection.close();
    },
  },
];
