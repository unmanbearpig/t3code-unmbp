import { RpcWebTransportConnector } from "@t3tools/client-runtime/platform";
import {
  RpcWebTransportError,
  type ReliableByteStream,
  type ReliableConnector,
  type ReliableSession,
  type TransportClose,
} from "@t3tools/shared/rpcWebTransport";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { requireOptionalNativeModule } from "expo";

const chunkBytes = 64 * 1024;
const emptyBytes = new Uint8Array(0);
const kind = { connect: 1, open: 2, read: 3, write: 4, finish: 5, closed: 6 } as const;

export interface NativeQuicResult {
  readonly status: number;
  readonly handle: number;
  readonly data: Uint8Array;
}

/** The Expo bridge carries bounded binary chunks, with one operation ID per owner. */
export interface NativeQuicModule {
  isAvailable(): boolean;
  createOwner(): number;
  submit(
    owner: number,
    operation: number,
    kind: number,
    handle: number,
    data: Uint8Array,
    argument: number,
  ): Promise<NativeQuicResult>;
  cancel(owner: number, operation: number): void;
  close(owner: number, applicationCode: number): void;
  reset(owner: number, stream: number, code: number): void;
  stop(owner: number, stream: number, code: number): void;
}

function bestEffort(action: () => void): void {
  try {
    action();
  } catch {
    // Logical teardown must not wait for or depend on a working native bridge.
  }
}

function aborted(): RpcWebTransportError {
  return new RpcWebTransportError("aborted", "Native WebTransport operation aborted.");
}

function transportError(cause: unknown): RpcWebTransportError {
  return cause instanceof RpcWebTransportError
    ? cause
    : new RpcWebTransportError("network", "Native WebTransport operation failed.", { cause });
}

function statusError(status: number): RpcWebTransportError {
  switch (status) {
    case -1:
      return aborted();
    case -2:
      return new RpcWebTransportError("network", "Native WebTransport connection failed.");
    case -3:
      return new RpcWebTransportError("rejected", "Native WebTransport operation rejected.");
    case -5:
      return new RpcWebTransportError("capacity", "Native WebTransport capacity exceeded.");
    case -6:
      return new RpcWebTransportError("unavailable", "Native WebTransport is unavailable.");
    default:
      return new RpcWebTransportError("protocol", "Invalid native WebTransport result.");
  }
}

function isHandle(value: number): boolean {
  return Number.isSafeInteger(value) && value > 0;
}

function applicationCode(value: number): number {
  return Number.isInteger(value) && value >= 0 && value <= 0xffff_ffff ? value : 0;
}

function closeForError(error: RpcWebTransportError): TransportClose {
  return {
    category:
      error.category === "capacity" || error.category === "unavailable"
        ? "network"
        : error.category,
  };
}

interface Operation {
  readonly promise: Promise<NativeQuicResult>;
  cancel(error: RpcWebTransportError): void;
}

class NativeSession implements ReliableSession {
  readonly closed: Promise<TransportClose>;
  failure: RpcWebTransportError | undefined;
  private resolveClosed!: (close: TransportClose) => void;
  private nextOperation = 0;
  private readonly pending = new Map<number, Operation["cancel"]>();

  constructor(
    readonly native: NativeQuicModule,
    readonly owner: number,
  ) {
    this.closed = new Promise((resolve) => {
      this.resolveClosed = resolve;
    });
  }

  terminate(close: TransportClose, error: RpcWebTransportError): void {
    if (this.failure !== undefined) return;
    this.failure = error;
    this.resolveClosed(close);
    for (const cancel of this.pending.values()) cancel(error);
    bestEffort(() => this.native.close(this.owner, applicationCode(close.applicationCode ?? 0)));
  }

  fail(error: RpcWebTransportError): void {
    this.terminate(closeForError(error), error);
  }

  close(code: number): void {
    this.terminate(
      { category: "closed", applicationCode: applicationCode(code) },
      new RpcWebTransportError("aborted", "Native WebTransport session closed."),
    );
  }

  peerClosed(result: NativeQuicResult): RpcWebTransportError {
    if (result.handle > 0x1_0000_0000) {
      const error = statusError(-4);
      this.fail(error);
      return error;
    }
    const error = new RpcWebTransportError("aborted", "Native WebTransport session closed.");
    this.terminate(
      {
        category: "closed",
        ...(result.handle > 0 ? { applicationCode: result.handle - 1 } : {}),
      },
      error,
    );
    return this.failure ?? error;
  }

  run(input: {
    readonly kind: number;
    readonly handle?: number;
    readonly data?: Uint8Array;
    readonly argument?: number;
    readonly signal?: AbortSignal;
    readonly onCancel?: (error: RpcWebTransportError) => void;
    readonly onLate?: (result: NativeQuicResult) => void;
  }): Operation {
    let cancel: Operation["cancel"] = () => undefined;
    const promise = new Promise<NativeQuicResult>((resolve, reject) => {
      if (this.failure !== undefined) {
        reject(this.failure);
        return;
      }
      if (this.nextOperation === Number.MAX_SAFE_INTEGER) {
        const error = new RpcWebTransportError("capacity", "Native operation IDs exhausted.");
        this.fail(error);
        reject(error);
        return;
      }
      const operation = ++this.nextOperation;
      let active = true;
      const remove = () => {
        active = false;
        this.pending.delete(operation);
        input.signal?.removeEventListener("abort", onAbort);
      };
      cancel = (error) => {
        if (!active) return;
        remove();
        reject(error);
        bestEffort(() => this.native.cancel(this.owner, operation));
        input.onCancel?.(error);
      };
      const onAbort = () => cancel(aborted());
      this.pending.set(operation, cancel);
      input.signal?.addEventListener("abort", onAbort, { once: true });
      if (input.signal?.aborted) {
        onAbort();
        return;
      }
      const completed = (result: NativeQuicResult) => {
        if (!active) {
          input.onLate?.(result);
          return;
        }
        remove();
        if (
          !Number.isInteger(result?.status) ||
          !Number.isSafeInteger(result.handle) ||
          result.handle < 0 ||
          !(result.data instanceof Uint8Array) ||
          result.data.byteLength > chunkBytes ||
          result.data.buffer.byteLength > chunkBytes
        ) {
          reject(statusError(-4));
        } else if (result.status < 0) {
          reject(statusError(result.status));
        } else {
          resolve(result);
        }
      };
      const failed = (cause: unknown) => {
        if (!active) return;
        remove();
        reject(transportError(cause));
      };
      try {
        this.native
          .submit(
            this.owner,
            operation,
            input.kind,
            input.handle ?? 0,
            input.data ?? emptyBytes,
            input.argument ?? 0,
          )
          .then(completed, failed);
      } catch (cause) {
        failed(cause);
      }
    });
    return { promise, cancel: (error) => cancel(error) };
  }

  watchClosed(): void {
    void this.run({ kind: kind.closed }).promise.then(
      (result) => {
        if (result.status === 2) this.peerClosed(result);
        else this.fail(statusError(-4));
      },
      (cause: unknown) => this.fail(transportError(cause)),
    );
  }

  async openBidirectionalStream(signal: AbortSignal): Promise<ReliableByteStream> {
    try {
      const result = await this.run({
        kind: kind.open,
        signal,
        onLate: (late) => {
          if (late?.status === 0 && isHandle(late.handle)) {
            bestEffort(() => this.native.reset(this.owner, late.handle, 0));
            bestEffort(() => this.native.stop(this.owner, late.handle, 0));
          }
        },
      }).promise;
      if (result.status === 2) throw this.peerClosed(result);
      if (result.status !== 0 || !isHandle(result.handle)) throw statusError(-4);
      if (this.failure !== undefined || signal.aborted) {
        bestEffort(() => this.native.reset(this.owner, result.handle, 0));
        bestEffort(() => this.native.stop(this.owner, result.handle, 0));
        throw this.failure ?? aborted();
      }
      return new NativeByteStream(this, result.handle);
    } catch (cause) {
      const error = transportError(cause);
      if (error.category === "network" || error.category === "protocol") this.fail(error);
      throw error;
    }
  }
}

class NativeByteStream implements ReliableByteStream {
  private readFailure: RpcWebTransportError | undefined;
  private writeFailure: RpcWebTransportError | undefined;
  private readEnded = false;
  private writeEnded = false;
  private pendingRead: Operation | undefined;
  private pendingWrite: Operation | undefined;
  private writing = false;

  constructor(
    private readonly session: NativeSession,
    private readonly handle: number,
  ) {}

  private failRead(error: RpcWebTransportError, code = 0): void {
    if (this.readFailure !== undefined || this.readEnded) return;
    this.readFailure = error;
    this.pendingRead?.cancel(error);
    bestEffort(() =>
      this.session.native.stop(this.session.owner, this.handle, applicationCode(code)),
    );
    if (error.category === "network" || error.category === "protocol") this.session.fail(error);
  }

  private failWrite(error: RpcWebTransportError, code = 0): void {
    if (this.writeFailure !== undefined || this.writeEnded) return;
    this.writeFailure = error;
    this.pendingWrite?.cancel(error);
    bestEffort(() =>
      this.session.native.reset(this.session.owner, this.handle, applicationCode(code)),
    );
    if (error.category === "network" || error.category === "protocol") this.session.fail(error);
  }

  async read(maxBytes: number, signal: AbortSignal): Promise<Uint8Array | null> {
    const failure = this.session.failure ?? this.readFailure;
    if (failure !== undefined) throw failure;
    if (this.readEnded) return null;
    if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0) throw statusError(-4);
    if (this.pendingRead !== undefined) throw statusError(-5);
    const argument = Math.min(maxBytes, chunkBytes);
    const operation = this.session.run({
      kind: kind.read,
      handle: this.handle,
      argument,
      signal,
      onCancel: (error) => this.failRead(error),
    });
    this.pendingRead = operation;
    try {
      const result = await operation.promise;
      const failure = this.session.failure ?? this.readFailure;
      if (failure !== undefined) throw failure;
      if (result.status === 2) throw this.session.peerClosed(result);
      if (result.status === 1) {
        this.readEnded = true;
        return null;
      }
      if (
        result.status !== 0 ||
        result.data.byteLength === 0 ||
        result.data.byteLength > argument
      ) {
        throw statusError(-4);
      }
      return result.data;
    } catch (cause) {
      const error = transportError(cause);
      this.failRead(error);
      throw error;
    } finally {
      this.pendingRead = undefined;
    }
  }

  async write(bytes: Uint8Array, signal: AbortSignal): Promise<void> {
    await this.writeOperation(kind.write, bytes, signal);
  }

  async finish(signal: AbortSignal): Promise<void> {
    await this.writeOperation(kind.finish, emptyBytes, signal);
  }

  private async writeOperation(
    operationKind: number,
    bytes: Uint8Array,
    signal: AbortSignal,
  ): Promise<void> {
    const failure = this.session.failure ?? this.writeFailure;
    if (failure !== undefined) throw failure;
    if (this.writeEnded) {
      if (operationKind === kind.finish) return;
      throw new RpcWebTransportError("aborted", "Native WebTransport stream finished.");
    }
    if (this.writing) throw statusError(-5);
    this.writing = true;
    try {
      let offset = 0;
      do {
        const data = bytes.subarray(offset, offset + chunkBytes).slice();
        const operation = this.session.run({
          kind: operationKind,
          handle: this.handle,
          data,
          signal,
          onCancel: (error) => this.failWrite(error),
        });
        this.pendingWrite = operation;
        const result = await operation.promise;
        const failure = this.session.failure ?? this.writeFailure;
        if (failure !== undefined) throw failure;
        if (result.status === 2) throw this.session.peerClosed(result);
        if (result.status !== 0) throw statusError(-4);
        offset += data.byteLength;
      } while (offset < bytes.byteLength);
      if (operationKind === kind.finish) this.writeEnded = true;
    } catch (cause) {
      const error = transportError(cause);
      this.failWrite(error);
      throw error;
    } finally {
      this.pendingWrite = undefined;
      this.writing = false;
    }
  }

  reset(code: number): void {
    this.failWrite(new RpcWebTransportError("aborted", "Native WebTransport stream reset."), code);
  }

  stop(code: number): void {
    this.failRead(new RpcWebTransportError("aborted", "Native WebTransport stream stopped."), code);
  }
}

export function makeNativeWebTransportConnector(native: NativeQuicModule): ReliableConnector {
  return {
    async connect({ url, signal }) {
      if (signal.aborted) throw aborted();
      let owner: number;
      try {
        owner = native.createOwner();
      } catch (cause) {
        throw transportError(cause);
      }
      if (!isHandle(owner)) throw statusError(-6);
      const session = new NativeSession(native, owner);
      // Native releases an owner as soon as it closes, including during connect.
      session.watchClosed();
      try {
        const result = await session.run({
          kind: kind.connect,
          data: new TextEncoder().encode(url),
          signal,
          onCancel: (error) => session.fail(error),
          onLate: () => bestEffort(() => native.close(owner, 0)),
        }).promise;
        if (result.status !== 0) throw statusError(-4);
        if (session.failure !== undefined) throw session.failure;
        if (signal.aborted) throw aborted();
        return session;
      } catch (cause) {
        const error = transportError(cause);
        session.fail(error);
        throw error;
      }
    },
  };
}

/** Older native clients omit this service and keep the existing WebSocket path. */
export const layer = Layer.effectContext(
  Effect.sync(() => {
    try {
      const native = requireOptionalNativeModule<NativeQuicModule>("T3Quic");
      if (
        native !== null &&
        typeof native.isAvailable === "function" &&
        native.isAvailable() &&
        typeof native.createOwner === "function" &&
        typeof native.submit === "function" &&
        typeof native.cancel === "function" &&
        typeof native.close === "function" &&
        typeof native.reset === "function" &&
        typeof native.stop === "function"
      ) {
        return Context.make(RpcWebTransportConnector, makeNativeWebTransportConnector(native));
      }
    } catch {
      // Expo Go and native clients predating this module can still connect over WSS.
    }
    return Context.empty();
  }),
);
