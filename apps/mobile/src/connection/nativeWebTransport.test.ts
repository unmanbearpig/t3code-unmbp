import { RpcWebTransportConnector } from "@t3tools/client-runtime/platform";
import { RpcWebTransportError, type ReliableSession } from "@t3tools/shared/rpcWebTransport";
import { afterEach, describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { vi } from "vite-plus/test";

const mocks = vi.hoisted(() => ({ requireModule: vi.fn() }));
vi.mock("expo", () => ({ requireOptionalNativeModule: mocks.requireModule }));

import {
  layer,
  makeNativeWebTransportConnector,
  type NativeQuicModule,
  type NativeQuicResult,
} from "./nativeWebTransport";

const signal = () => new AbortController().signal;
const result = (status = 0, handle = 0, data = new Uint8Array(0)): NativeQuicResult => ({
  status,
  handle,
  data,
});

interface Submitted {
  readonly owner: number;
  readonly operation: number;
  readonly kind: number;
  readonly handle: number;
  readonly data: Uint8Array;
  readonly argument: number;
  readonly resolve: (result: NativeQuicResult) => void;
  readonly reject: (cause: unknown) => void;
}

function fakeNative() {
  const submitted: Submitted[] = [];
  const waiting: Array<{
    readonly kind: number;
    readonly handle: number | undefined;
    readonly resolve: (submitted: Submitted) => void;
  }> = [];
  let nextOwner = 100;
  const native: NativeQuicModule = {
    isAvailable: vi.fn(() => true),
    createOwner: vi.fn(() => ++nextOwner),
    submit: vi.fn(
      (owner, operation, kind, handle, data, argument) =>
        new Promise<NativeQuicResult>((resolve, reject) => {
          const entry = { owner, operation, kind, handle, data, argument, resolve, reject };
          const index = waiting.findIndex(
            (waiter) =>
              waiter.kind === kind && (waiter.handle === undefined || waiter.handle === handle),
          );
          if (index < 0) submitted.push(entry);
          else waiting.splice(index, 1)[0]?.resolve(entry);
        }),
    ),
    cancel: vi.fn(),
    close: vi.fn(),
    reset: vi.fn(),
    stop: vi.fn(),
  };
  function take(kind: number, handle?: number) {
    const index = submitted.findIndex(
      (entry) => entry.kind === kind && (handle === undefined || entry.handle === handle),
    );
    if (index < 0) throw new Error(`Missing native operation ${kind}.`);
    const entry = submitted.splice(index, 1)[0];
    if (entry === undefined) throw new Error(`Missing native operation ${kind}.`);
    return entry;
  }
  const connector = makeNativeWebTransportConnector(native);
  return {
    native,
    submitted,
    take,
    next(kind: number, handle?: number): Promise<Submitted> {
      if (
        submitted.some(
          (entry) => entry.kind === kind && (handle === undefined || entry.handle === handle),
        )
      ) {
        return Promise.resolve(take(kind, handle));
      }
      return new Promise((resolve) => waiting.push({ kind, handle, resolve }));
    },
    connector,
    async connect() {
      const connecting = connector.connect({ url: "https://host.example/rpc", signal: signal() });
      take(1).resolve(result());
      return connecting;
    },
    async open(session: ReliableSession, handle = 7) {
      const opening = session.openBidirectionalStream(signal());
      take(2).resolve(result(0, handle));
      return opening;
    },
  };
}

afterEach(() => mocks.requireModule.mockReset());

describe("native WebTransport connector", () => {
  it("aborts a pending connect without waiting for native, then closes a late connection", async () => {
    const fake = fakeNative();
    const controller = new AbortController();
    const connecting = fake.connector.connect({
      url: "https://host.example/rpc",
      signal: controller.signal,
    });
    const connect = fake.take(1);
    expect(new TextDecoder().decode(connect.data)).toBe("https://host.example/rpc");
    controller.abort();
    await expect(connecting).rejects.toMatchObject({ category: "aborted" });
    expect(fake.native.cancel).toHaveBeenCalledWith(connect.owner, connect.operation);
    expect(fake.native.close).toHaveBeenCalledWith(connect.owner, 0);

    connect.resolve(result());
    await Promise.resolve();
    expect(fake.native.close).toHaveBeenCalledTimes(2);
  });

  it("never allocates an owner for an already aborted connect", async () => {
    const fake = fakeNative();
    const controller = new AbortController();
    controller.abort();
    await expect(
      fake.connector.connect({ url: "https://host.example/rpc", signal: controller.signal }),
    ).rejects.toMatchObject({ category: "aborted" });
    expect(fake.native.createOwner).not.toHaveBeenCalled();
  });

  it("registers closure before connect and handles closure during connection establishment", async () => {
    const fake = fakeNative();
    const connecting = fake.connector.connect({
      url: "https://host.example/rpc",
      signal: signal(),
    });
    expect(fake.submitted.map((entry) => entry.kind)).toEqual([6, 1]);
    fake.take(6).resolve(result(-3));
    await expect(connecting).rejects.toMatchObject({ category: "rejected" });
    expect(fake.native.close).toHaveBeenCalledWith(101, 0);
    fake.take(1).resolve(result());
    await Promise.resolve();
    expect(fake.native.close).toHaveBeenCalledTimes(2);
  });

  it("cancels a pending open and destroys both halves when native completes late", async () => {
    const fake = fakeNative();
    const session = await fake.connect();
    const controller = new AbortController();
    const opening = session.openBidirectionalStream(controller.signal);
    const open = fake.take(2);
    controller.abort();
    await expect(opening).rejects.toMatchObject({ category: "aborted" });
    expect(fake.native.cancel).toHaveBeenCalledWith(open.owner, open.operation);
    expect(fake.native.close).not.toHaveBeenCalled();

    open.resolve(result(0, 12));
    await Promise.resolve();
    expect(fake.native.reset).toHaveBeenCalledWith(open.owner, 12, 0);
    expect(fake.native.stop).toHaveBeenCalledWith(open.owner, 12, 0);
    const stream = await fake.open(session);
    stream.stop(3);
    session.close(0);
  });

  it("cleans an open that completes just before cancellation", async () => {
    const fake = fakeNative();
    const session = await fake.connect();
    const controller = new AbortController();
    const opening = session.openBidirectionalStream(controller.signal);
    const open = fake.take(2);
    open.resolve(result(0, 12));
    await Promise.resolve();
    controller.abort();
    await expect(opening).rejects.toMatchObject({ category: "aborted" });
    expect(fake.native.reset).toHaveBeenCalledWith(open.owner, 12, 0);
    expect(fake.native.stop).toHaveBeenCalledWith(open.owner, 12, 0);
    session.close(0);
  });

  it("closes locally and rejects pending and future operations while native is stalled", async () => {
    const fake = fakeNative();
    const session = await fake.connect();
    const stream = await fake.open(session);
    const reading = stream.read(10, signal());
    const writing = stream.write(new Uint8Array([1]), signal());
    const opening = session.openBidirectionalStream(signal());
    session.close(42);

    expect(await session.closed).toEqual({ category: "closed", applicationCode: 42 });
    const failure = await reading.catch((cause: unknown) => cause);
    expect(failure).toMatchObject({ category: "aborted" });
    await expect(writing).rejects.toBe(failure);
    await expect(opening).rejects.toBe(failure);
    await expect(stream.read(10, signal())).rejects.toBe(failure);
    await expect(stream.write(new Uint8Array([2]), signal())).rejects.toBe(failure);
    await expect(stream.finish(signal())).rejects.toBe(failure);
    await expect(session.openBidirectionalStream(signal())).rejects.toBe(failure);
    session.close(1);
    expect(fake.native.close).toHaveBeenCalledExactlyOnceWith(101, 42);
  });

  it("retains a native connection failure and its cause for every later operation", async () => {
    const fake = fakeNative();
    const session = await fake.connect();
    const stream = await fake.open(session);
    const reading = stream.read(10, signal());
    const underlying = new Error("native connection failed");
    fake.take(6).reject(underlying);
    expect(await session.closed).toEqual({ category: "network" });
    const failure = await reading.catch((cause: unknown) => cause);
    expect(failure).toBeInstanceOf(RpcWebTransportError);
    expect(failure).toMatchObject({ category: "network", cause: underlying });
    await expect(session.openBidirectionalStream(signal())).rejects.toBe(failure);
    await expect(stream.read(10, signal())).rejects.toBe(failure);
  });

  it("reports peer application closure and rejects work already in progress", async () => {
    const fake = fakeNative();
    const session = await fake.connect();
    const stream = await fake.open(session);
    const reading = stream.read(10, signal());
    fake.take(6).resolve(result(2, 14));
    expect(await session.closed).toEqual({ category: "closed", applicationCode: 13 });
    await expect(reading).rejects.toMatchObject({ category: "aborted" });
  });

  it("caps reads at 64 KiB, preserves binary data, and remembers EOF", async () => {
    const fake = fakeNative();
    const session = await fake.connect();
    const stream = await fake.open(session);
    const reading = stream.read(1024 * 1024, signal());
    const read = fake.take(3);
    expect(read.argument).toBe(65_536);
    const bytes = new Uint8Array([0, 128, 255]);
    read.resolve(result(0, 0, bytes));
    expect(await reading).toBe(bytes);
    const eof = stream.read(1, signal());
    fake.take(3).resolve(result(1));
    expect(await eof).toBeNull();
    expect(await stream.read(1, signal())).toBeNull();
    expect(fake.submitted.filter((entry) => entry.kind === 3)).toHaveLength(0);
    const finishing = stream.finish(signal());
    fake.take(5).resolve(result());
    await finishing;
    await stream.finish(signal());
    await expect(stream.write(bytes, signal())).rejects.toMatchObject({ category: "aborted" });
    session.close(0);
  });

  it("splits a large write into 64 KiB native chunks and waits before submitting the next", async () => {
    const fake = fakeNative();
    const session = await fake.connect();
    const stream = await fake.open(session);
    const bytes = new Uint8Array(2 * 65_536 + 3);
    bytes.set([1, 128, 255], 2 * 65_536);
    const writing = stream.write(bytes, signal());
    const first = fake.take(4);
    expect(first.data.byteLength).toBe(65_536);
    expect(first.data.buffer.byteLength).toBe(65_536);
    expect(fake.submitted.filter((entry) => entry.kind === 4)).toHaveLength(0);
    first.resolve(result());
    const second = await fake.next(4);
    expect(second.data.byteLength).toBe(65_536);
    expect(second.data.buffer.byteLength).toBe(65_536);
    expect(fake.submitted.filter((entry) => entry.kind === 4)).toHaveLength(0);
    second.resolve(result());
    const last = await fake.next(4);
    expect(last.data).toEqual(new Uint8Array([1, 128, 255]));
    expect(last.data.buffer.byteLength).toBe(3);
    last.resolve(result());
    await writing;
    session.close(0);
  });

  it("rejects concurrent writes without queuing or blocking another stream", async () => {
    const fake = fakeNative();
    const session = await fake.connect();
    const first = await fake.open(session, 7);
    const second = await fake.open(session, 8);
    const backing = new Uint8Array(1024 * 1024);
    backing.set([0, 255, 128]);
    const writing = first.write(backing.subarray(0, 3), signal());
    const write = fake.take(4, 7);
    expect(write.data).toEqual(new Uint8Array([0, 255, 128]));
    expect(write.data.buffer.byteLength).toBe(3);
    await expect(first.write(new Uint8Array([3]), signal())).rejects.toMatchObject({
      category: "capacity",
    });
    await expect(first.finish(signal())).rejects.toMatchObject({ category: "capacity" });
    const otherWriting = second.write(new Uint8Array([4]), signal());
    fake.take(4, 8).resolve(result());
    await otherWriting;
    write.resolve(result());
    await writing;
    session.close(0);
  });

  it("aborts reads immediately and leaves the write half usable", async () => {
    const fake = fakeNative();
    const session = await fake.connect();
    const stream = await fake.open(session);
    const controller = new AbortController();
    const reading = stream.read(10, controller.signal);
    const read = fake.take(3);
    await expect(stream.read(10, signal())).rejects.toMatchObject({ category: "capacity" });
    controller.abort();
    const failure = await reading.catch((cause: unknown) => cause);
    expect(failure).toMatchObject({ category: "aborted" });
    expect(fake.native.cancel).toHaveBeenCalledWith(read.owner, read.operation);
    expect(fake.native.stop).toHaveBeenCalledWith(read.owner, 7, 0);
    await expect(stream.read(10, signal())).rejects.toBe(failure);
    const writing = stream.write(new Uint8Array([2]), signal());
    fake.take(4).resolve(result());
    await writing;
    session.close(0);
  });

  it("reset settles a blocked write and stop settles a blocked read without native callbacks", async () => {
    const fake = fakeNative();
    const session = await fake.connect();
    const stream = await fake.open(session);
    const writing = stream.write(new Uint8Array([2]), signal());
    const reading = stream.read(10, signal());
    stream.reset(18);
    stream.stop(19);
    await expect(writing).rejects.toMatchObject({ category: "aborted" });
    await expect(reading).rejects.toMatchObject({ category: "aborted" });
    expect(fake.native.reset).toHaveBeenCalledWith(101, 7, 18);
    expect(fake.native.stop).toHaveBeenCalledWith(101, 7, 19);
    await expect(stream.finish(signal())).rejects.toMatchObject({ category: "aborted" });
    session.close(0);
  });

  it.each(["Android", "iOS"])(
    "cancels %s opens queued before native registration without waiting for dispatch",
    async () => {
      const fake = fakeNative();
      const dispatch: Array<() => void> = [];
      const tasks = new Map<number, Submitted>();
      const native: NativeQuicModule = {
        ...fake.native,
        submit: (...args) =>
          new Promise((resolve) => {
            dispatch.push(() => {
              void fake.native.submit(...args).then(resolve);
              if (args[2] === 2) tasks.set(args[1], fake.take(2));
            });
          }),
        cancel: (_owner, operation) =>
          new Promise<void>((resolve) => {
            dispatch.push(() => {
              const task = tasks.get(operation);
              tasks.delete(operation);
              task?.resolve(result(-1));
              resolve();
            });
          }),
      };
      const flush = () => {
        while (dispatch.length > 0) dispatch.shift()?.();
      };
      const connecting = makeNativeWebTransportConnector(native).connect({
        url: "https://host.example/rpc",
        signal: signal(),
      });
      flush();
      fake.take(1).resolve(result());
      const session = await connecting;
      const controller = new AbortController();
      const opening = session.openBidirectionalStream(controller.signal);
      controller.abort();
      await expect(opening).rejects.toMatchObject({ category: "aborted" });
      expect(tasks.size).toBe(0);
      expect(dispatch).toHaveLength(2);
      flush();
      expect(tasks.size).toBe(0);
      await Promise.resolve();
      expect(fake.native.close).not.toHaveBeenCalled();
      session.close(0);
      flush();
    },
  );

  it("cleans both halves of late successful opens beyond the native stream-slot limit", async () => {
    const fake = fakeNative();
    const slots = new Map<number, { send: boolean; receive: boolean }>();
    const release = (handle: number, half: "send" | "receive") => {
      const slot = slots.get(handle);
      if (slot === undefined) return;
      slot[half] = false;
      if (!slot.send && !slot.receive) slots.delete(handle);
    };
    vi.mocked(fake.native.reset).mockImplementation((_owner, handle) => release(handle, "send"));
    vi.mocked(fake.native.stop).mockImplementation((_owner, handle) => release(handle, "receive"));
    const session = await fake.connect();
    for (let handle = 1; handle <= 70; handle++) {
      const controller = new AbortController();
      const opening = session.openBidirectionalStream(controller.signal);
      const nativeOpen = fake.take(2);
      slots.set(handle, { send: true, receive: true });
      controller.abort();
      await expect(opening).rejects.toMatchObject({ category: "aborted" });
      nativeOpen.resolve(result(0, handle));
      await Promise.resolve();
      expect(slots.size).toBe(0);
    }
    session.close(0);
  });

  it("publishes explicit peer codes before an immediately running generic cancellation", async () => {
    const fake = fakeNative();
    const session = await fake.connect();
    const stream = await fake.open(session);
    const observed: number[] = [];
    let resetCode = 0;
    let stopCode = 0;
    vi.mocked(fake.native.reset).mockImplementation((_owner, _handle, code) => {
      resetCode = code;
    });
    vi.mocked(fake.native.stop).mockImplementation((_owner, _handle, code) => {
      stopCode = code;
    });
    const writing = stream.write(new Uint8Array([2]), signal());
    const write = fake.take(4);
    const reading = stream.read(10, signal());
    const read = fake.take(3);
    vi.mocked(fake.native.cancel).mockImplementation((_owner, operation) => {
      if (operation === write.operation) {
        observed.push(resetCode);
        write.resolve(result(-1));
      }
      if (operation === read.operation) {
        observed.push(stopCode);
        read.resolve(result(-1));
      }
    });
    stream.reset(42);
    stream.stop(43);
    await expect(writing).rejects.toMatchObject({ category: "aborted" });
    await expect(reading).rejects.toMatchObject({ category: "aborted" });
    expect(observed).toEqual([42, 43]);
    session.close(0);
  });

  it("observes asynchronous cancellation rejection while aborting the caller immediately", async () => {
    const fake = fakeNative();
    const session = await fake.connect();
    let rejectCancellation: (cause: Error) => void = () => undefined;
    vi.mocked(fake.native.cancel).mockImplementation(
      () =>
        new Promise<void>((_resolve, reject) => {
          rejectCancellation = reject;
        }),
    );
    const controller = new AbortController();
    const opening = session.openBidirectionalStream(controller.signal);
    const open = fake.take(2);
    controller.abort();
    await expect(opening).rejects.toMatchObject({ category: "aborted" });
    rejectCancellation(new Error("native cancellation dispatch failed"));
    open.resolve(result(0, 7));
    await Promise.resolve();
    expect(fake.native.reset).toHaveBeenCalledWith(open.owner, 7, 0);
    expect(fake.native.stop).toHaveBeenCalledWith(open.owner, 7, 0);
    session.close(0);
  });

  it("aborts a blocked finish without waiting for native and retains the write failure", async () => {
    const fake = fakeNative();
    const session = await fake.connect();
    const stream = await fake.open(session);
    const controller = new AbortController();
    const finishing = stream.finish(controller.signal);
    const finish = fake.take(5);
    controller.abort();
    const failure = await finishing.catch((cause: unknown) => cause);
    expect(failure).toMatchObject({ category: "aborted" });
    expect(fake.native.cancel).toHaveBeenCalledWith(finish.owner, finish.operation);
    expect(fake.native.reset).toHaveBeenCalledWith(finish.owner, 7, 0);
    await expect(stream.finish(signal())).rejects.toBe(failure);
    await expect(stream.write(new Uint8Array([1]), signal())).rejects.toBe(failure);
    const reading = stream.read(1, signal());
    fake.take(3).resolve(result(0, 0, new Uint8Array([2])));
    expect(await reading).toEqual(new Uint8Array([2]));
    session.close(0);
  });

  it("retains a rejected write and cancels its half without closing unrelated streams", async () => {
    const fake = fakeNative();
    const session = await fake.connect();
    const stream = await fake.open(session);
    const writing = stream.write(new Uint8Array([2]), signal());
    fake.take(4).resolve(result(-3));
    const failure = await writing.catch((cause: unknown) => cause);
    expect(failure).toMatchObject({ category: "rejected" });
    await expect(stream.write(new Uint8Array([3]), signal())).rejects.toBe(failure);
    expect(fake.native.close).not.toHaveBeenCalled();
    expect(fake.native.reset).toHaveBeenCalledWith(101, 7, 0);
    session.close(0);
  });

  it("fails the session if native returns more bytes than requested", async () => {
    const fake = fakeNative();
    const session = await fake.connect();
    const stream = await fake.open(session);
    const reading = stream.read(1, signal());
    fake.take(3).resolve(result(0, 0, new Uint8Array([1, 2])));
    const failure = await reading.catch((cause: unknown) => cause);
    expect(failure).toMatchObject({ category: "protocol" });
    expect(await session.closed).toEqual({ category: "protocol" });
    await expect(session.openBidirectionalStream(signal())).rejects.toBe(failure);
  });
});

describe("optional native connector layer", () => {
  it.effect("omits the connector on binaries without the native module", () =>
    Effect.gen(function* () {
      mocks.requireModule.mockReturnValue(null);
      const connector = yield* Effect.serviceOption(RpcWebTransportConnector).pipe(
        Effect.provide(layer),
      );
      expect(Option.isNone(connector)).toBe(true);
    }),
  );

  it.effect("omits the connector when native reports an unsupported ABI", () =>
    Effect.gen(function* () {
      const fake = fakeNative();
      vi.mocked(fake.native.isAvailable).mockReturnValue(false);
      mocks.requireModule.mockReturnValue(fake.native);
      const connector = yield* Effect.serviceOption(RpcWebTransportConnector).pipe(
        Effect.provide(layer),
      );
      expect(Option.isNone(connector)).toBe(true);
    }),
  );

  it.effect("omits the connector when loading native throws or methods are missing", () =>
    Effect.gen(function* () {
      mocks.requireModule.mockImplementation(() => {
        throw new Error("Native module unavailable");
      });
      const missing = yield* Effect.serviceOption(RpcWebTransportConnector).pipe(
        Effect.provide(layer),
      );
      expect(Option.isNone(missing)).toBe(true);
      mocks.requireModule.mockReturnValue({ isAvailable: () => true });
      const outdated = yield* Effect.serviceOption(RpcWebTransportConnector).pipe(
        Effect.provide(layer),
      );
      expect(Option.isNone(outdated)).toBe(true);
    }),
  );

  it.effect("provides a working connector only when the native module is available", () =>
    Effect.gen(function* () {
      const fake = fakeNative();
      mocks.requireModule.mockReturnValue(fake.native);
      const connector = yield* Effect.serviceOption(RpcWebTransportConnector).pipe(
        Effect.provide(layer),
      );
      expect(Option.isSome(connector)).toBe(true);
      if (Option.isNone(connector)) throw new Error("Missing connector.");
      const connecting = connector.value.connect({
        url: "https://host.example/rpc",
        signal: signal(),
      });
      fake.take(1).resolve(result());
      const session = yield* Effect.promise(() => connecting);
      session.close(0);
      expect(yield* Effect.promise(() => session.closed)).toEqual({
        category: "closed",
        applicationCode: 0,
      });
    }),
  );
});
