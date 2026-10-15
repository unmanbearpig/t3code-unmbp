import {
  RpcWebTransportError,
  RPC_WEBTRANSPORT_LIMITS,
  type ReliableByteStream,
  type ServerReliableSession,
  type TransportClose,
} from "./rpcWebTransportCore.ts";
import type { TransportByteBudget, TransportByteLease } from "./rpcWebTransportBudget.ts";

const readBufferAccounts = new WeakMap<ReliableByteStream, (budget: TransportByteBudget) => void>();

/** Keeps the optional standard-stream carry buffer in the decoder's own accounting tree. */
export function accountStandardWebTransportReadBuffers(
  stream: ReliableByteStream,
  budget: TransportByteBudget,
) {
  readBufferAccounts.get(stream)?.(budget);
}

interface StandardReader<A> {
  readonly closed?: Promise<void>;
  read(): Promise<{ readonly done: boolean; readonly value?: A | undefined }>;
  cancel(reason?: unknown): Promise<void>;
  releaseLock(): void;
}

interface StandardReadable<A> {
  getReader(): StandardReader<A>;
}

interface StandardByteReader {
  readonly closed?: Promise<void>;
  read(
    view: Uint8Array<ArrayBuffer>,
  ): Promise<{ readonly done: boolean; readonly value?: Uint8Array | undefined }>;
  cancel(reason?: unknown): Promise<void>;
  releaseLock(): void;
}

interface StandardByteReadable extends StandardReadable<Uint8Array> {
  getReader(options: { readonly mode: "byob" }): StandardByteReader;
  getReader(): StandardReader<Uint8Array>;
}

export interface StandardWebTransportStream {
  readonly readable: StandardByteReadable;
  readonly writable: {
    getWriter(): {
      write(bytes: Uint8Array): Promise<void>;
      close(): Promise<void>;
      abort(reason?: unknown): Promise<void>;
      releaseLock(): void;
    };
  };
}

/** Structural Web Streams seam, usable without DOM constructors on native clients. */
export interface StandardWebTransportSession {
  readonly closed: Promise<{ readonly closeCode?: number } | void>;
  readonly incomingBidirectionalStreams: StandardReadable<StandardWebTransportStream>;
  createBidirectionalStream(): Promise<StandardWebTransportStream>;
  close(info?: { readonly closeCode: number }): void;
}

const aborted = () => new RpcWebTransportError("aborted", "Transport operation cancelled");

function bestEffort(f: () => void | Promise<void>) {
  try {
    void Promise.resolve(f()).catch(() => undefined);
  } catch {
    // Native shutdown is advisory after logical ownership has ended.
  }
}

/** Interrupts locally even when the backend never acknowledges cancellation. */
function cancellable<A>(
  operation: () => Promise<A>,
  signal: AbortSignal,
  lifetime: AbortSignal,
  cancel: () => void,
  late?: (value: A) => void,
): Promise<A> {
  return new Promise((resolve, reject) => {
    let settled = false;
    const cleanup = () => {
      signal.removeEventListener("abort", onAbort);
      lifetime.removeEventListener("abort", onAbort);
    };
    const onAbort = () => {
      if (settled) return;
      settled = true;
      cleanup();
      cancel();
      reject(aborted());
    };
    if (signal.aborted || lifetime.aborted) return onAbort();
    signal.addEventListener("abort", onAbort, { once: true });
    lifetime.addEventListener("abort", onAbort, { once: true });
    let pending: Promise<A>;
    try {
      pending = operation();
    } catch (cause) {
      cleanup();
      settled = true;
      reject(cause);
      return;
    }
    void pending.then(
      (value) => {
        if (settled) return late?.(value);
        settled = true;
        cleanup();
        resolve(value);
      },
      (cause: unknown) => {
        if (settled) return;
        settled = true;
        cleanup();
        reject(cause);
      },
    );
  });
}

export function wrapStandardWebTransportSession(
  raw: StandardWebTransportSession,
  options?: { readonly streamError: (applicationCode: number) => unknown },
): ServerReliableSession {
  const lifetime = new AbortController();
  const streams = new Set<ReliableByteStream>();
  let incoming: StandardReader<StandardWebTransportStream> | undefined;
  let accepting = false;
  let opening = false;
  let resolveClosed!: (close: TransportClose) => void;
  const streamError = (code: number): unknown => {
    try {
      return options?.streamError(code) ?? { code, streamErrorCode: code };
    } catch {
      return { code, streamErrorCode: code };
    }
  };
  const closed = new Promise<TransportClose>((resolve) => {
    resolveClosed = resolve;
  });
  function close(
    code: number,
    category: TransportClose["category"] = "closed",
    closeNative = true,
  ) {
    if (lifetime.signal.aborted) return;
    lifetime.abort();
    resolveClosed({ category, applicationCode: code });
    for (const stream of streams) {
      stream.reset(code);
      stream.stop(code);
    }
    streams.clear();
    // The session owns the incoming stream source. Cancelling it separately can race
    // native session shutdown; releasing its reader also rejects an outstanding read.
    if (incoming) bestEffort(() => incoming?.releaseLock());
    if (closeNative) bestEffort(() => raw.close({ closeCode: code }));
  }
  void raw.closed.then(
    (info) => close(info?.closeCode ?? 0, "closed", false),
    () => close(1, "network", false),
  );
  const wrap = (stream: StandardWebTransportStream): ReliableByteStream => {
    let byob: StandardByteReader | undefined;
    let defaultReader: StandardReader<Uint8Array> | undefined;
    try {
      byob = stream.readable.getReader({ mode: "byob" });
    } catch {
      defaultReader = stream.readable.getReader();
    }
    const reader = byob ?? defaultReader!;
    // closed can settle while the final queued bytes are still completing a read.
    // Observe errors here, but only read() decides when all bytes have reached us.
    void reader.closed?.catch(() => undefined);
    const writer = stream.writable.getWriter();
    const readLifetime = new AbortController();
    const writeLifetime = new AbortController();
    let reading = false;
    let writing = false;
    let readDone = false;
    let writeDone = false;
    let remainder: Uint8Array | undefined;
    let remainderLease: TransportByteLease | undefined;
    let readBufferBudget: TransportByteBudget | undefined;
    const dispose = () => {
      if (readDone && writeDone) streams.delete(result);
    };
    const result: ReliableByteStream = {
      async read(maxBytes, signal) {
        if (
          !Number.isSafeInteger(maxBytes) ||
          maxBytes < 1 ||
          maxBytes > RPC_WEBTRANSPORT_LIMITS.recordBytes
        ) {
          throw new RangeError("Invalid transport read size");
        }
        if (reading) throw new RpcWebTransportError("protocol", "Concurrent stream reads");
        if (readLifetime.signal.aborted || lifetime.signal.aborted) throw aborted();
        if (signal.aborted) throw aborted();
        if (readDone) return null;
        reading = true;
        try {
          if (!remainder) {
            const item = await cancellable(
              () => (byob ? byob.read(new Uint8Array(maxBytes)) : defaultReader!.read()),
              signal,
              readLifetime.signal,
              () => result.stop(1),
            );
            if (readLifetime.signal.aborted || lifetime.signal.aborted || signal.aborted)
              throw aborted();
            if (item.done) {
              readDone = true;
              bestEffort(() => reader.releaseLock());
              dispose();
              return null;
            }
            const bytes = "value" in item ? item.value : undefined;
            if (!bytes || bytes.byteLength === 0)
              throw new RpcWebTransportError("protocol", "Empty transport chunk");
            // A backend chunk is retained until drained. Reject unexpectedly large allocations
            // instead of hiding an unbounded backing buffer behind a small subarray.
            if (bytes.buffer.byteLength > RPC_WEBTRANSPORT_LIMITS.recordBytes) {
              result.stop(1);
              throw new RpcWebTransportError("capacity", "Backend read exceeds bounded chunk size");
            }
            try {
              remainderLease = readBufferBudget?.acquire(bytes.buffer.byteLength);
            } catch (cause) {
              result.stop(1);
              throw cause;
            }
            remainder = bytes;
          }
          const count = Math.min(maxBytes, remainder.byteLength);
          const output = remainder.slice(0, count);
          remainder = count === remainder.byteLength ? undefined : remainder.subarray(count);
          if (!remainder) {
            remainderLease?.release();
            remainderLease = undefined;
          }
          return output;
        } finally {
          reading = false;
        }
      },
      async write(bytes, signal) {
        if (writing || writeDone)
          throw new RpcWebTransportError("protocol", "Invalid concurrent stream write");
        if (bytes.byteLength > RPC_WEBTRANSPORT_LIMITS.recordBytes)
          throw new RangeError("Transport write exceeds chunk size");
        writing = true;
        try {
          await cancellable(
            () => writer.write(bytes),
            signal,
            writeLifetime.signal,
            () => result.reset(1),
          );
        } finally {
          writing = false;
        }
      },
      async finish(signal) {
        if (writeDone) return;
        if (writing) throw new RpcWebTransportError("protocol", "Finish during stream write");
        writing = true;
        try {
          await cancellable(
            () => writer.close(),
            signal,
            writeLifetime.signal,
            () => result.reset(1),
          );
          writeDone = true;
          bestEffort(() => writer.releaseLock());
          dispose();
        } finally {
          writing = false;
        }
      },
      reset(code) {
        if (writeLifetime.signal.aborted) return;
        writeLifetime.abort();
        writeDone = true;
        bestEffort(() => writer.abort(streamError(code)));
        bestEffort(() => writer.releaseLock());
        dispose();
      },
      stop(code) {
        if (readLifetime.signal.aborted) return;
        readLifetime.abort();
        readDone = true;
        remainder = undefined;
        remainderLease?.release();
        remainderLease = undefined;
        bestEffort(() => reader.cancel(streamError(code)));
        bestEffort(() => reader.releaseLock());
        dispose();
      },
    };
    readBufferAccounts.set(result, (budget) => {
      if (readBufferBudget && readBufferBudget !== budget)
        throw new RpcWebTransportError("protocol", "Stream decoder budget already installed");
      readBufferBudget = budget;
    });
    streams.add(result);
    if (lifetime.signal.aborted) {
      result.reset(1);
      result.stop(1);
    }
    return result;
  };
  const discard = (stream: StandardWebTransportStream) => {
    const owned = wrap(stream);
    owned.reset(1);
    owned.stop(1);
  };
  return {
    closed,
    close,
    async openBidirectionalStream(signal) {
      if (opening) throw new RpcWebTransportError("capacity", "Concurrent native stream opens");
      opening = true;
      try {
        return wrap(
          await cancellable(
            () => raw.createBidirectionalStream(),
            signal,
            lifetime.signal,
            () => close(1, "aborted"),
            discard,
          ),
        );
      } finally {
        opening = false;
      }
    },
    async acceptBidirectionalStream(signal) {
      if (accepting) throw new RpcWebTransportError("protocol", "Concurrent stream accepts");
      incoming ??= raw.incomingBidirectionalStreams.getReader();
      accepting = true;
      try {
        const item = await cancellable(
          () => incoming!.read(),
          signal,
          lifetime.signal,
          () => close(1, "aborted"),
          (item) => {
            if (item.value) discard(item.value);
          },
        );
        if (item.done || !item.value)
          throw new RpcWebTransportError("network", "Incoming streams ended");
        return wrap(item.value);
      } finally {
        accepting = false;
      }
    },
  };
}
