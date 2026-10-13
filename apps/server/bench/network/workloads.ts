// @effect-diagnostics globalTimers:off - Ping and stream timers sit in the measured path, so they stay plain timers.
// Workloads modelled on T3 traffic: interactive RPCs, a streaming agent turn, a large response
// sharing the connection with interactive RPCs, and several thread subscriptions at once.
import { connect, type Connection, type TlsMaterial, type TransportName } from "./client.ts";
import { decodeFrame, encodeFrame, type Frame, Kind, wallClockMs } from "./protocol.ts";
import { workloadSettings } from "./settings.ts";

export type Metrics = Record<string, number | string>;

/** Samples recorded as a workload runs, so a run cut off by its deadline still reports them. */
export class Recorder {
  private readonly series = new Map<string, number[]>();
  private readonly values = new Map<string, number>();
  private readonly cleanups: Array<() => void> = [];
  private readonly controller = new AbortController();
  private readonly expected = new Map<string, number>();
  private invalid: string | undefined;

  expectSamples(name: string, count: number) {
    this.expected.set(name, count);
  }

  check(condition: boolean, message: string) {
    if (!condition && this.invalid === undefined) this.invalid = message;
  }

  /** Partial samples are allowed on a timeout; completed work must contain every expected sample. */
  validationError(completed: boolean) {
    if (this.invalid) return this.invalid;
    if (completed) {
      for (const [name, count] of this.expected) {
        const actual = this.series.get(name)?.length ?? 0;
        if (actual !== count) return `${name}: expected ${count} samples, received ${actual}`;
      }
    }
    return undefined;
  }

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
  encodeFrame({
    kind: Kind.ping,
    channel,
    seq,
    sentAt: wallClockMs(),
    arg: 0,
    payloadBytes: workloadSettings.pingBytes,
  });

function exchangePing(
  connection: Connection,
  on: ReturnType<typeof route>,
  seq: number,
  record: Recorder,
  series?: string,
) {
  return new Promise<void>((resolve) => {
    on(0, (frame, receivedAt) => {
      record.check(
        frame.kind === Kind.pong &&
          frame.seq === seq &&
          frame.payload.byteLength === workloadSettings.pingBytes,
        "Invalid ping response",
      );
      if (series) record.sample(series, receivedAt - frame.sentAt);
      resolve();
    });
    connection.send(0, ping(0, seq));
  });
}

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
  bytes: number,
  intervalMs: number,
) {
  let remaining = channels.length;
  record.expectSamples("latency", channels.length * count);
  return new Promise<void>((resolve) => {
    for (const channel of channels) {
      let received = 0;
      let lastAt = 0;
      on(channel, (frame, receivedAt) => {
        record.check(
          frame.kind === Kind.streamMessage &&
            frame.seq === received &&
            frame.arg === count &&
            frame.payload.byteLength === bytes,
          "Missing, reordered, or truncated stream event",
        );
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
    description: `${workloadSettings.connect.count} cold connections, then one ${workloadSettings.pingBytes}B ping each`,
    run: async (transport, tls, record) => {
      record.expectSamples("connect", workloadSettings.connect.count);
      record.expectSamples("firstPing", workloadSettings.connect.count);
      record.expectSamples("connectAndPing", workloadSettings.connect.count);
      for (let i = 0; i < workloadSettings.connect.count; i++) {
        const startedAt = wallClockMs();
        const connection = await connect(transport, tls, record.signal);
        record.onAbort(connection.close);
        record.sample("connect", wallClockMs() - startedAt);
        const on = route(connection);
        await exchangePing(connection, on, 0, record, "firstPing");
        record.sample("connectAndPing", wallClockMs() - startedAt);
        connection.close();
      }
    },
  },
  {
    name: "rpc",
    description: `${workloadSettings.rpc.count} sequential ${workloadSettings.pingBytes}B request/response round trips after a warmup ping`,
    run: async (transport, tls, record) => {
      const connection = await connect(transport, tls, record.signal);
      record.onAbort(connection.close);
      const on = route(connection);
      record.expectSamples("rtt", workloadSettings.rpc.count);
      for (let seq = 0; seq < workloadSettings.rpc.warmupCount; seq++)
        await exchangePing(connection, on, seq, record);
      for (
        let seq = workloadSettings.rpc.warmupCount;
        seq < workloadSettings.rpc.warmupCount + workloadSettings.rpc.count;
        seq++
      ) {
        await exchangePing(connection, on, seq, record, "rtt");
      }
      connection.close();
    },
  },
  {
    name: "stream",
    description: `one subscription: ${workloadSettings.stream.count} events of ${workloadSettings.stream.bytes}B every ${workloadSettings.stream.intervalMs}ms`,
    run: async (transport, tls, record) => {
      const connection = await connect(transport, tls, record.signal);
      record.onAbort(connection.close);
      const { count, bytes, intervalMs } = workloadSettings.stream;
      const done = collectStreams(route(connection), record, [1], count, bytes, intervalMs);
      requestStream(connection, 1, count, bytes, intervalMs);
      await done;
      connection.close();
    },
  },
  {
    name: "fanout",
    description: `${workloadSettings.fanout.subscriptions} subscriptions: ${workloadSettings.fanout.count} events of ${workloadSettings.fanout.bytes}B every ${workloadSettings.fanout.intervalMs}ms each`,
    run: async (transport, tls, record) => {
      const connection = await connect(transport, tls, record.signal);
      record.onAbort(connection.close);
      const { subscriptions, count, bytes, intervalMs } = workloadSettings.fanout;
      const channels = Array.from({ length: subscriptions }, (_, i) => i + 1);
      const done = collectStreams(route(connection), record, channels, count, bytes, intervalMs);
      for (const channel of channels) requestStream(connection, channel, count, bytes, intervalMs);
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
      for (let seq = 0; seq < workloadSettings.hol.warmupCount; seq++)
        await exchangePing(connection, on, seq, record);
      const outstanding = new Set<number>();
      let allPongs: () => void = () => {};
      on(0, (frame, receivedAt) => {
        record.check(
          frame.kind === Kind.pong &&
            outstanding.has(frame.seq) &&
            frame.payload.byteLength === workloadSettings.pingBytes,
          "Invalid or duplicated ping during bulk response",
        );
        record.sample("pingDuringBulk", receivedAt - frame.sentAt);
        outstanding.delete(frame.seq);
        if (outstanding.size === 0) allPongs();
      });
      const startedAt = wallClockMs();
      const bulk = new Promise<void>((resolve) =>
        on(1, (frame, receivedAt) => {
          record.check(
            frame.kind === Kind.bulkMessage &&
              frame.payload.byteLength === workloadSettings.hol.bulkBytes,
            "Truncated bulk response",
          );
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
          arg: workloadSettings.hol.bulkBytes,
        }),
      );
      let seq = 0;
      const timer = setInterval(() => {
        outstanding.add(seq);
        connection.send(0, ping(0, seq++));
      }, workloadSettings.hol.pingIntervalMs);
      record.onAbort(() => clearInterval(timer));
      await bulk;
      clearInterval(timer);
      record.expectSamples("pingDuringBulk", seq);
      if (outstanding.size > 0) await new Promise<void>((resolve) => (allPongs = resolve));
      connection.close();
    },
  },
];
