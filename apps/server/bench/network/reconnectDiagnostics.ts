// @effect-diagnostics anyUnknownInErrorContext:off - Reflection preserves overloaded native Web Streams reader methods without changing their returned promises.
import type {
  StandardWebTransportSession,
  StandardWebTransportStream,
  ReliableByteStream,
} from "@t3tools/shared/rpcWebTransport";

export function redactDiagnostic(text: string) {
  return text
    .replace(/(?:https?|wss?):\/\/[^\s"'<>]+/gu, (url) => {
      try {
        const value = new URL(url);
        return `${value.origin}${value.pathname}`;
      } catch {
        return "<redacted-url>";
      }
    })
    .replace(/Bearer\s+[^\s"',}]+/giu, "Bearer <redacted>")
    .replace(
      /((?:wsTicket|credential|access_token|sessionToken)["']?\s*[:=]\s*["']?)[^\s"'&,}]+/giu,
      "$1<redacted>",
    );
}

export function diagnosticError(error: unknown, depth = 0): unknown {
  if (depth > 10) return "<depth-limit>";
  if (typeof error === "string") return redactDiagnostic(error);
  if (typeof error !== "object" || error === null) return error;
  const fields: Record<string, unknown> = {};
  for (const key of [
    "_tag",
    "name",
    "message",
    "stack",
    "code",
    "closeCode",
    "streamErrorCode",
    "cause",
    "reason",
  ]) {
    if (key in error) fields[key] = diagnosticError(Reflect.get(error, key), depth + 1);
  }
  return fields;
}

type Fields = Readonly<Record<string, unknown>>;
type Event = { readonly atMs: number; readonly event: string } & Fields;

/** Keeps framing lengths only. Bodies, credentials and RPC payloads are never retained. */
function recordCounter() {
  const header = new Uint8Array(10);
  let prefaceRemaining = 5;
  let headerBytes = 0;
  let bodyRemaining = 0;
  let bytes = 0;
  let records = 0;
  let messages = 0;
  let flags = 0;
  let wire = 0;
  let decoded = 0;
  let invalid = false;
  return {
    push(chunk: Uint8Array) {
      bytes += chunk.byteLength;
      let offset = 0;
      while (offset < chunk.byteLength && !invalid) {
        if (prefaceRemaining) {
          const count = Math.min(prefaceRemaining, chunk.byteLength - offset);
          offset += count;
          prefaceRemaining -= count;
        } else if (bodyRemaining) {
          const count = Math.min(bodyRemaining, chunk.byteLength - offset);
          offset += count;
          bodyRemaining -= count;
          if (bodyRemaining === 0) {
            records++;
            if (flags & 1) messages++;
          }
        } else {
          const count = Math.min(header.length - headerBytes, chunk.byteLength - offset);
          header.set(chunk.subarray(offset, offset + count), headerBytes);
          offset += count;
          headerBytes += count;
          if (headerBytes === header.length) {
            const view = new DataView(header.buffer);
            flags = header[1]!;
            wire = view.getUint32(2);
            decoded = view.getUint32(6);
            invalid = header[0] !== 1 || flags > 3 || wire < 1 || wire > 65536;
            bodyRemaining = wire;
            headerBytes = 0;
          }
        }
      }
    },
    snapshot: () => ({
      bytes,
      records,
      messages,
      prefaceRemaining,
      headerBytes,
      bodyRemaining,
      flags,
      wire,
      decoded,
      invalid,
    }),
  };
}

export function makeReconnectDiagnostics(side: "client" | "server") {
  const started = performance.now();
  const startedEpochMs = performance.timeOrigin + started;
  const events: Event[] = [];
  let firstSessionClose: Event | undefined;
  const milestones: Event[] = [];
  const progress = new Map<string, Event>();
  const nativeStates: {
    id: number;
    readBytes: number;
    writeBytes: number;
    writeSuccess: number;
    writeFailure: number;
    fins: number;
    paused: boolean;
    commitsWhilePaused: number;
    events: Event[];
    firstTermination?: Event;
  }[] = [];
  const nativeObjects = new WeakMap<object, number>();
  const nativeStreams = new WeakMap<object, number>();
  const adapters: {
    id: number;
    reads: ReturnType<typeof recordCounter>;
    events: Event[];
    firstEOF?: Event;
  }[] = [];
  const streams: {
    id: number;
    readable: object;
    reads: ReturnType<typeof recordCounter>;
    writes: ReturnType<typeof recordCounter>;
    events: Event[];
    firstClosure: Event | undefined;
  }[] = [];
  const event = (name: string, fields: Fields = {}) => {
    const item = { atMs: performance.now() - started, event: name, ...fields };
    events.push(item);
    if (events.length > 512) events.shift();
    if (name.startsWith("app-")) milestones.push(item);
    if (milestones.length > 512) milestones.shift();
    if (name === "socket-progress") progress.set(String(fields.connection), item);
    if (
      !firstSessionClose &&
      (name === "native-session-owner-close" ||
        name === "native-session-closed" ||
        name === "native-session-closed-error" ||
        name === "adapter-owner-close")
    )
      firstSessionClose = item;
    return item;
  };
  const native = (owner: object, method: string, argument?: unknown, phase = "call") => {
    let id = nativeObjects.get(owner);
    if (id === undefined) {
      id = nativeStates.length;
      nativeObjects.set(owner, id);
      nativeStates.push({
        id,
        readBytes: 0,
        writeBytes: 0,
        writeSuccess: 0,
        writeFailure: 0,
        fins: 0,
        paused: false,
        commitsWhilePaused: 0,
        events: [],
      });
    }
    const state = nativeStates[id]!;
    const readable: unknown = Reflect.get(owner, "readable");
    if (typeof readable === "object" && readable !== null) nativeStreams.set(readable, id);
    const fields: Record<string, unknown> = {};
    if (typeof argument === "object" && argument !== null)
      for (const key of [
        "success",
        "readBytes",
        "fin",
        "drained",
        "byteSize",
        "nettask",
        "code",
        "bufferBytes",
        "byob",
      ]) {
        const value: unknown = Reflect.get(argument, key);
        if (typeof value === "string" || typeof value === "boolean" || typeof value === "number")
          fields[key] = value;
      }
    const controller: unknown = Reflect.get(owner, "readableController");
    if (typeof controller === "object" && controller !== null) {
      fields.desiredSize = Reflect.get(controller, "desiredSize");
      fields.hasByob = Reflect.get(controller, "byobRequest") != null;
    }
    fields.readableClosed = Reflect.get(owner, "readableclosed") === true;
    if (phase === "call") {
      if (method === "stopReading") state.paused = true;
      if (method === "startReading") state.paused = false;
      if (method === "commitReadBuffer") {
        state.readBytes += typeof fields.readBytes === "number" ? fields.readBytes : 0;
        if (fields.fin) state.fins++;
        if (state.paused && typeof fields.readBytes === "number" && fields.readBytes > 0)
          state.commitsWhilePaused++;
      }
      if (method === "onStreamWrite") {
        if (fields.success === false) state.writeFailure++;
        else if (fields.success === true) state.writeSuccess++;
      }
      if (method === "writeChunk" && argument instanceof Uint8Array)
        state.writeBytes += argument.byteLength;
    }
    const item = event(`native-${method}-${phase}`, {
      nativeStream: id,
      paused: state.paused,
      ...fields,
    });
    state.events.push(item);
    if (
      !state.firstTermination &&
      phase === "call" &&
      (["stopSending", "resetStream", "streamFinal", "onStreamRecvSignal"].includes(method) ||
        fields.fin === true)
    )
      state.firstTermination = item;
    if (state.events.length > 80) state.events.shift();
  };
  const wrapStream = <T extends StandardWebTransportStream>(stream: T): T => {
    const state = {
      id: streams.length,
      readable: stream.readable,
      reads: recordCounter(),
      writes: recordCounter(),
      events: [] as Event[],
      firstClosure: undefined as Event | undefined,
    };
    streams.push(state);
    const stamp = (name: string, fields: Fields = {}) => {
      const item = event(name, { stream: state.id, ...fields });
      state.events.push(item);
      if (
        !state.firstClosure &&
        ([
          "reader-closed",
          "reader-closed-error",
          "reader-cancel",
          "writer-close",
          "writer-abort",
        ].includes(name) ||
          fields.done === true)
      )
        state.firstClosure = item;
      if (state.events.length > 80) state.events.shift();
    };
    stamp("stream-open");
    const readable = new Proxy(stream.readable, {
      get(target, property) {
        if (property !== "getReader") return Reflect.get(target, property, target);
        return (...args: readonly unknown[]) => {
          const reader: unknown = Reflect.apply(target.getReader, target, args);
          if (typeof reader !== "object" || reader === null)
            throw new Error("Invalid native reader");
          const closed: unknown = Reflect.get(reader, "closed", reader);
          if (closed instanceof Promise)
            void closed.then(
              () => stamp("reader-closed", state.reads.snapshot()),
              (error: unknown) =>
                stamp("reader-closed-error", {
                  error: diagnosticError(error),
                  ...state.reads.snapshot(),
                }),
            );
          return new Proxy(reader, {
            get(readerTarget, key) {
              const method: unknown = Reflect.get(readerTarget, key, readerTarget);
              if (typeof method !== "function") return method;
              return (...readArgs: readonly unknown[]) => {
                const result: unknown = Reflect.apply(method, readerTarget, readArgs);
                if (key === "read") {
                  stamp("reader-read", {
                    requested: readArgs[0] instanceof Uint8Array ? readArgs[0].byteLength : 0,
                  });
                  if (result instanceof Promise)
                    void result.then(
                      (item: unknown) => {
                        if (typeof item !== "object" || item === null) return;
                        const bytes: unknown = Reflect.get(item, "value");
                        if (bytes instanceof Uint8Array) state.reads.push(bytes);
                        stamp("reader-read-result", {
                          done: Reflect.get(item, "done"),
                          chunkBytes: bytes instanceof Uint8Array ? bytes.byteLength : 0,
                          ...state.reads.snapshot(),
                        });
                      },
                      (error: unknown) =>
                        stamp("reader-read-error", { error: diagnosticError(error) }),
                    );
                } else stamp(`reader-${String(key)}`);
                // Return the original native promise, preserving its settlement ordering.
                return result;
              };
            },
          });
        };
      },
    });
    const writable = {
      getWriter: () => {
        const writer = stream.writable.getWriter();
        return new Proxy(writer, {
          get(target, property) {
            const method: unknown = Reflect.get(target, property, target);
            if (typeof method !== "function") return method;
            return (...args: readonly unknown[]) => {
              if (property === "write" && args[0] instanceof Uint8Array) state.writes.push(args[0]);
              stamp(`writer-${String(property)}`, state.writes.snapshot());
              const result: unknown = Reflect.apply(method, target, args);
              if (result instanceof Promise)
                void result.then(
                  () => stamp(`writer-${String(property)}-settled`, state.writes.snapshot()),
                  (error: unknown) =>
                    stamp(`writer-${String(property)}-error`, { error: diagnosticError(error) }),
                );
              return result;
            };
          },
        });
      },
    };
    return new Proxy(stream, {
      get(target, property) {
        if (property === "readable") return readable;
        if (property === "writable") return writable;
        return Reflect.get(target, property, target);
      },
    });
  };
  const mapReadable = <T extends object>(readable: T, map: (value: unknown) => unknown): T =>
    new Proxy(readable, {
      get(target, key) {
        const method: unknown = Reflect.get(target, key, target);
        if (typeof method !== "function") return method;
        if (key !== "getReader") return method.bind(target);
        return (...args: readonly unknown[]) => {
          const reader: object = Reflect.apply(method, target, args);
          return new Proxy(reader, {
            get(readerTarget, property) {
              const operation: unknown = Reflect.get(readerTarget, property, readerTarget);
              if (typeof operation !== "function") return operation;
              if (property !== "read") return operation.bind(readerTarget);
              return (...readArgs: readonly unknown[]) => {
                const result: unknown = Reflect.apply(operation, readerTarget, readArgs);
                if (!(result instanceof Promise)) throw new Error("Invalid native stream source");
                return result.then((item: unknown) => {
                  if (typeof item !== "object" || item === null || Reflect.get(item, "done"))
                    return item;
                  return { done: false, value: map(Reflect.get(item, "value")) };
                });
              };
            },
          });
        };
      },
    });
  const session = <T extends StandardWebTransportSession>(raw: T): T => {
    event("native-session");
    void raw.closed.then(
      (info) => event("native-session-closed", { code: info?.closeCode ?? 0 }),
      (error: unknown) => event("native-session-closed-error", { error: diagnosticError(error) }),
    );
    return new Proxy(raw, {
      get(target, property) {
        if (property === "incomingBidirectionalStreams")
          return mapReadable(target.incomingBidirectionalStreams, (stream) =>
            wrapStream(stream as StandardWebTransportStream),
          );
        if (property === "createBidirectionalStream")
          return () => target.createBidirectionalStream().then(wrapStream);
        if (property === "close")
          return (info?: { readonly closeCode: number }) => {
            event("native-session-owner-close", { info });
            target.close(info);
          };
        const value: unknown = Reflect.get(target, property, target);
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
  };
  return {
    event,
    native,
    adapter: (stream: ReliableByteStream, id: number) => {
      const state: (typeof adapters)[number] = { id, reads: recordCounter(), events: [] };
      adapters.push(state);
      const stamp = (name: string, fields: Fields = {}) => {
        const item = event(name, { adapter: id, ...fields });
        state.events.push(item);
        if (state.events.length > 80) state.events.shift();
        if (fields.eof === true && !state.firstEOF) state.firstEOF = item;
      };
      const read = stream.read.bind(stream);
      stream.read = (maxBytes, signal) => {
        stamp("adapter-read", { maxBytes });
        const pending = read(maxBytes, signal);
        void pending.then(
          (bytes) => {
            if (bytes) state.reads.push(bytes);
            stamp("adapter-read-result", {
              eof: bytes === null,
              chunkBytes: bytes?.byteLength ?? 0,
              ...state.reads.snapshot(),
            });
          },
          (error: unknown) => stamp("adapter-read-error", { error: diagnosticError(error) }),
        );
        return pending;
      };
      const reset = stream.reset.bind(stream);
      const stop = stream.stop.bind(stream);
      const finish = stream.finish.bind(stream);
      stream.reset = (code) => {
        stamp("adapter-reset", { code });
        reset(code);
      };
      stream.stop = (code) => {
        stamp("adapter-stop", { code });
        stop(code);
      };
      stream.finish = (signal) => {
        stamp("adapter-finish");
        return finish(signal);
      };
      // Mutate methods on the original stream so its production accounting WeakMap identity remains intact.
      return stream;
    },
    session,
    sessionSource: <T extends object>(source: T) =>
      mapReadable(source, (raw) => session(raw as StandardWebTransportSession)),
    snapshot: () => ({
      side,
      startedEpochMs,
      firstSessionClose,
      adapters: adapters.map((state) => ({
        id: state.id,
        reads: state.reads.snapshot(),
        firstEOF: state.firstEOF,
        events: state.events.slice(),
      })),
      milestones: milestones.slice(),
      progress: [...progress.values()],
      events: events.slice(),
      nativeStreams: nativeStates.map((state) => ({ ...state, events: state.events.slice() })),
      streams: streams.map((state) => ({
        id: state.id,
        nativeId: nativeStreams.get(state.readable) ?? null,
        firstClosure: state.firstClosure,
        reads: state.reads.snapshot(),
        writes: state.writes.snapshot(),
        events: state.events.slice(),
      })),
    }),
  };
}
