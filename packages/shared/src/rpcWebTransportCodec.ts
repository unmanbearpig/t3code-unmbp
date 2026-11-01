import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import { deflateSync, inflateSync, Inflate } from "fflate";
import {
  RPC_WEBTRANSPORT_LIMITS as defaults,
  RPC_WEBTRANSPORT_WIRE_VERSION,
  RpcWebTransportError,
  type ReliableByteStream,
} from "./rpcWebTransportCore.ts";
import type { TransportByteBudget, TransportByteLease } from "./rpcWebTransportBudget.ts";
import { accountStandardWebTransportReadBuffers } from "./rpcWebTransportStreams.ts";

export interface RpcWebTransportLimits {
  readonly messageBytes: number;
  readonly ordinaryRequestStreams: number;
  readonly pendingOrdinaryRequests: number;
}

export const rpcWebTransportDefaultLimits: RpcWebTransportLimits = {
  messageBytes: defaults.messageBytes,
  ordinaryRequestStreams: defaults.ordinaryRequestStreams,
  pendingOrdinaryRequests: defaults.pendingOrdinaryRequests,
};

const LimitsSchema = Schema.Struct({
  messageBytes: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: defaults.messageBytes })),
  ordinaryRequestStreams: Schema.Int.check(
    Schema.isBetween({ minimum: 1, maximum: defaults.ordinaryRequestStreams }),
  ),
  pendingOrdinaryRequests: Schema.Int.check(
    Schema.isBetween({ minimum: 1, maximum: defaults.pendingOrdinaryRequests }),
  ),
});

export const HelloSchema = Schema.TaggedStruct("Hello", {
  wireVersion: Schema.Literal(RPC_WEBTRANSPORT_WIRE_VERSION),
  orchestrationProtocolVersion: Schema.Int,
  encodings: Schema.Array(Schema.Literals(["identity", "deflate"])),
  limits: LimitsSchema,
});
export const ReadySchema = Schema.TaggedStruct("Ready", {
  wireVersion: Schema.Literal(RPC_WEBTRANSPORT_WIRE_VERSION),
  orchestrationProtocolVersion: Schema.Int,
  environmentId: Schema.String,
  encoding: Schema.Literal("deflate"),
  limits: LimitsSchema,
});
const Id = Schema.Union([Schema.String, Schema.Int]);
const Request = Schema.TaggedStruct("Request", {
  id: Id,
  tag: Schema.String,
  payload: Schema.Unknown,
  headers: Schema.Array(Schema.mutable(Schema.Tuple([Schema.String, Schema.String]))),
  traceId: Schema.optionalKey(Schema.String),
  spanId: Schema.optionalKey(Schema.String),
  sampled: Schema.optionalKey(Schema.Boolean),
  isNotification: Schema.optionalKey(Schema.Literal(true)),
});
const Ack = Schema.TaggedStruct("Ack", {
  requestId: Id,
});
const Interrupt = Schema.TaggedStruct("Interrupt", {
  requestId: Id,
});
const Exit = Schema.Union([
  Schema.TaggedStruct("Success", {
    value: Schema.Unknown,
  }),
  Schema.TaggedStruct("Failure", {
    cause: Schema.Array(
      Schema.Union([
        Schema.TaggedStruct("Fail", {
          error: Schema.Unknown,
        }),
        Schema.TaggedStruct("Die", {
          defect: Schema.Unknown,
        }),
        Schema.TaggedStruct("Interrupt", {
          fiberId: Schema.optionalKey(Schema.Union([Schema.Int, Schema.Null])),
        }),
      ]),
    ),
  }),
]);
export const ClientRequestEnvelopeSchema = Schema.Union([Request, Ack, Interrupt]);
export const ServerRequestEnvelopeSchema = Schema.Union([
  Schema.TaggedStruct("Chunk", {
    requestId: Id,
    values: Schema.NonEmptyArray(Schema.Unknown),
  }),
  Schema.TaggedStruct("Exit", {
    requestId: Id,
    exit: Exit,
  }),
]);
export const ClientControlEnvelopeSchema = Schema.Union([
  Schema.TaggedStruct("Ping", {}),
  Schema.TaggedStruct("Eof", {}),
]);
export const ServerControlEnvelopeSchema = Schema.Union([
  Schema.TaggedStruct("Pong", {}),
  Schema.TaggedStruct("Eof", {}),
  Schema.TaggedStruct("Defect", {
    defect: Schema.Unknown,
  }),
  Request,
]);

export const transportError = (cause: unknown): RpcWebTransportError =>
  cause instanceof RpcWebTransportError
    ? cause
    : new RpcWebTransportError("network", "Reliable transport operation failed", { cause });

export const transportTry = <A>(f: (signal: AbortSignal) => Promise<A>) =>
  Effect.tryPromise({ try: f, catch: transportError });

const protocolFailure = (message: string) => new RpcWebTransportError("protocol", message);

function inflateRecord(encoded: Uint8Array, expected: number, guard: Uint8Array) {
  const pushBytes = 256;
  // Tiny compressed inputs already bound synchronous work to one small push. Avoid
  // streaming history/copy overhead for the common highly compressed 64 KiB record.
  if (encoded.byteLength > pushBytes) {
    let decoded = 0;
    const check = new Inflate((bytes) => {
      decoded += bytes.byteLength;
      if (decoded > expected) throw protocolFailure("Deflate decoded length mismatch");
    });
    // Public streaming pushes stop a hostile record after its first excess output.
    // The sync guard alone truncates retained output but still processes the entire bomb.
    for (let offset = 0; offset < encoded.byteLength; offset += pushBytes) {
      check.push(
        encoded.subarray(offset, offset + pushBytes),
        offset + pushBytes >= encoded.byteLength,
      );
    }
    if (decoded !== expected) throw protocolFailure("Deflate decoded length mismatch");
  }
  // Keep the existing sync final-block/truncation validation and fixed output guard.
  // A valid record now inflates at most its declared <=64 KiB in this second pass.
  // Both supported APIs ignore trailing compressed bytes within the bounded record.
  return inflateSync(encoded, { out: guard });
}
const json = Schema.fromJsonString(Schema.Unknown);
const decodeJson = Schema.decodeUnknownEffect(json);
const encodeJson = Schema.encodeEffect(json);

export const decodeTransportEnvelope = <S extends Schema.Top>(schema: S, value: unknown) =>
  Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.mapError(
      (cause) => new RpcWebTransportError("protocol", "Invalid RPC transport envelope", { cause }),
    ),
  );

export const resolveRpcWebTransportLimits = (limits?: Partial<RpcWebTransportLimits>) =>
  decodeTransportEnvelope(LimitsSchema, { ...rpcWebTransportDefaultLimits, ...limits });

export function lowerRpcWebTransportLimits(
  a: RpcWebTransportLimits,
  b: RpcWebTransportLimits,
): RpcWebTransportLimits {
  return {
    messageBytes: Math.min(a.messageBytes, b.messageBytes),
    ordinaryRequestStreams: Math.min(a.ordinaryRequestStreams, b.ordinaryRequestStreams),
    pendingOrdinaryRequests: Math.min(a.pendingOrdinaryRequests, b.pendingOrdinaryRequests),
  };
}

const byteLeases = Effect.gen(function* () {
  const live = new Set<TransportByteLease>();
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      for (const lease of live) lease.release();
    }),
  );
  // A single scope finalizer owns only live leases. Released pull/header/scratch
  // reservations must not leave one retained cleanup closure per past operation.
  return (budget: TransportByteBudget, bytes: number) =>
    Effect.try({
      try: () => {
        const underlying = budget.acquire(bytes);
        const lease = {
          release() {
            underlying.release();
            live.delete(lease);
          },
        };
        live.add(lease);
        return lease;
      },
      catch: transportError,
    });
});

/** Version, flags, wire bytes, decoded bytes. Non-final records decode to exactly 64 KiB. */
const RPC_WEBTRANSPORT_RECORD_HEADER_BYTES = 10;
export const RPC_WEBTRANSPORT_PREFACE = new Uint8Array([
  0x54,
  0x33,
  0x51,
  RPC_WEBTRANSPORT_WIRE_VERSION,
]);

export interface RpcWebTransportFrameChannel {
  readonly readPreface: () => Effect.Effect<"control" | "request", RpcWebTransportError>;
  readonly writePreface: (role: "control" | "request") => Effect.Effect<void, RpcWebTransportError>;
  readonly read: (options?: {
    readonly first?: boolean;
    readonly control?: boolean;
  }) => Effect.Effect<unknown | null, RpcWebTransportError>;
  readonly write: (value: unknown, control?: boolean) => Effect.Effect<void, RpcWebTransportError>;
  readonly finish: Effect.Effect<void, RpcWebTransportError>;
}

function utf8Bytes(text: string): number {
  let size = 0;
  for (let i = 0; i < text.length; i++) {
    const code = text.charCodeAt(i);
    if (code < 0x80) size++;
    else if (code < 0x800) size += 2;
    else if (
      code >= 0xd800 &&
      code <= 0xdbff &&
      i + 1 < text.length &&
      text.charCodeAt(i + 1) >= 0xdc00 &&
      text.charCodeAt(i + 1) <= 0xdfff
    ) {
      size += 4;
      i++;
    } else size += 3;
  }
  return size;
}

export function makeRpcWebTransportFrameChannel(
  stream: ReliableByteStream,
  input: TransportByteBudget,
  output: TransportByteBudget,
  limits: RpcWebTransportLimits,
): RpcWebTransportFrameChannel {
  accountStandardWebTransportReadBuffers(stream, input);
  const writer = Semaphore.makeUnsafe(1);
  let prefaceStarted: number | undefined;
  const exact = Effect.fnUntraced(function* (
    acquire: Effect.Success<typeof byteLeases>,
    size: number,
    allowFin = false,
    idle?: number,
  ) {
    const lease = yield* acquire(input, size);
    const bytes = new Uint8Array(size);
    let offset = 0;
    let handedOff = false;
    try {
      while (offset < size) {
        const maxRead = Math.min(size - offset, defaults.recordBytes);
        // Pull adapters may allocate their BYOB/native result before the promise settles.
        const pending = yield* acquire(input, maxRead);
        const pull = transportTry((signal) => stream.read(maxRead, signal));
        const idleDeadline = idle ?? (offset > 0 ? defaults.assemblyIdleTimeoutMs : undefined);
        const chunk = yield* idleDeadline === undefined
          ? pull
          : pull.pipe(
              Effect.timeoutOrElse({
                duration: idleDeadline,
                orElse: () => Effect.fail(protocolFailure("Message assembly stalled")),
              }),
            );
        if (chunk === null) {
          pending.release();
          if (offset === 0 && allowFin) return null;
          return yield* Effect.fail(protocolFailure("Truncated transport record"));
        }
        const retained = yield* acquire(input, Math.max(0, chunk.buffer.byteLength - maxRead));
        try {
          if (chunk.byteLength === 0 || chunk.byteLength > size - offset)
            return yield* Effect.fail(protocolFailure("Invalid byte adapter read"));
          bytes.set(chunk, offset);
          offset += chunk.byteLength;
        } finally {
          retained.release();
          pending.release();
        }
      }
      handedOff = true;
      return { bytes, lease };
    } finally {
      if (!handedOff) lease.release();
    }
  });
  const header = Effect.fnUntraced(function* (
    acquire: Effect.Success<typeof byteLeases>,
    first: boolean,
    allowFin: boolean,
  ) {
    const effect = exact(
      acquire,
      RPC_WEBTRANSPORT_RECORD_HEADER_BYTES,
      allowFin,
      allowFin ? undefined : defaults.assemblyIdleTimeoutMs,
    );
    const now = yield* Clock.currentTimeMillis;
    const remaining = Math.max(
      0,
      defaults.streamPrefaceTimeoutMs - (prefaceStarted === undefined ? 0 : now - prefaceStarted),
    );
    return yield* first
      ? effect.pipe(
          Effect.timeoutOrElse({
            duration: remaining,
            orElse: () => Effect.fail(protocolFailure("Missing initial record header")),
          }),
        )
      : effect;
  });
  return {
    readPreface: Effect.fnUntraced(function* () {
      const acquire = yield* byteLeases;
      prefaceStarted = yield* Clock.currentTimeMillis;
      const item = yield* exact(acquire, 5).pipe(
        Effect.timeoutOrElse({
          duration: defaults.streamPrefaceTimeoutMs,
          orElse: () => Effect.fail(protocolFailure("Missing stream preface")),
        }),
      );
      if (!item) return yield* Effect.fail(protocolFailure("Missing stream preface"));
      try {
        if (
          RPC_WEBTRANSPORT_PREFACE.some((byte, i) => item.bytes[i] !== byte) ||
          (item.bytes[4] !== 0 && item.bytes[4] !== 1)
        )
          return yield* Effect.fail(protocolFailure("Invalid stream preface"));
        return item.bytes[4] === 0 ? "control" : "request";
      } finally {
        item.lease.release();
      }
    }, Effect.scoped),
    writePreface: (role) =>
      transportTry((signal) =>
        stream.write(
          new Uint8Array([...RPC_WEBTRANSPORT_PREFACE, role === "control" ? 0 : 1]),
          signal,
        ),
      ),
    read: Effect.fnUntraced(function* (options = {}) {
      const acquire = yield* byteLeases;
      const chunks: Array<{ bytes: Uint8Array; lease: TransportByteLease }> = [];
      let total = 0;
      let assembly: TransportByteLease | undefined;
      try {
        for (let record = 0; ; record++) {
          const h = yield* header(acquire, options.first === true && record === 0, record === 0);
          if (!h) return null;
          let wire: number, decoded: number, flags: number;
          try {
            const view = new DataView(h.bytes.buffer, h.bytes.byteOffset, h.bytes.byteLength);
            flags = view.getUint8(1);
            wire = view.getUint32(2);
            decoded = view.getUint32(6);
            if (
              h.bytes[0] !== RPC_WEBTRANSPORT_WIRE_VERSION ||
              flags > 3 ||
              decoded < 1 ||
              decoded > defaults.recordBytes ||
              ((flags & 1) === 0 && decoded !== defaults.recordBytes) ||
              wire < 1 ||
              ((flags & 2) === 0
                ? wire !== decoded
                : wire > defaults.compressedRecordBytes || wire >= decoded)
            )
              return yield* Effect.fail(protocolFailure("Invalid record header"));
            total += decoded;
            if (total > (options.control ? defaults.controlMessageBytes : limits.messageBytes))
              return yield* Effect.fail(protocolFailure("Logical message exceeds limit"));
          } finally {
            h.lease.release();
          }
          const body = yield* exact(acquire, wire, false, defaults.assemblyIdleTimeoutMs);
          if (!body) return yield* Effect.fail(protocolFailure("Missing record body"));
          if ((flags & 2) === 0) chunks.push(body);
          else {
            let expanded: TransportByteLease | undefined;
            try {
              expanded = yield* acquire(input, decoded + 1);
              const guard = new Uint8Array(decoded + 1);
              // fflate's supported fixed-output API bounds retained output. It can ignore
              // trailing compressed bytes within this <=8 KiB record; v1 permits that.
              const bytes = yield* Effect.try({
                try: () => inflateRecord(body.bytes, decoded, guard),
                catch: () => protocolFailure("Invalid deflate record"),
              });
              if (bytes.byteLength !== decoded)
                return yield* Effect.fail(protocolFailure("Deflate decoded length mismatch"));
              chunks.push({ bytes, lease: expanded });
              expanded = undefined;
            } finally {
              body.lease.release();
              expanded?.release();
            }
          }
          if ((flags & 1) !== 0) break;
          if (chunks.length % 8 === 0) yield* Effect.yieldNow;
        }
        assembly = yield* acquire(input, total);
        const bytes = new Uint8Array(total);
        let offset = 0;
        for (const chunk of chunks) {
          bytes.set(chunk.bytes, offset);
          offset += chunk.bytes.byteLength;
        }
        const text = yield* Effect.try({
          try: () => new TextDecoder("utf-8", { fatal: true }).decode(bytes),
          catch: () => protocolFailure("Invalid JSON or UTF-8 message"),
        });
        return yield* decodeJson(text).pipe(
          Effect.mapError(() => protocolFailure("Invalid JSON message")),
        );
      } finally {
        assembly?.release();
        for (const chunk of chunks) chunk.lease.release();
      }
    }, Effect.scoped),
    write: (value, control = false) =>
      writer.withPermits(1)(
        Effect.gen(function* () {
          const acquire = yield* byteLeases;
          const text = yield* encodeJson(value).pipe(
            Effect.mapError(() => protocolFailure("Cannot serialize RPC message")),
          );
          if (text === undefined) return yield* Effect.fail(protocolFailure("Empty JSON message"));
          const size = utf8Bytes(text);
          if (size < 1 || size > (control ? defaults.controlMessageBytes : limits.messageBytes))
            return yield* Effect.fail(protocolFailure("Logical message exceeds limit"));
          // Charge both the serialized text's UTF-8 size and its byte encoding. This is
          // transport accounting, not a prediction of the engine's string/object heap.
          const source = yield* acquire(output, size * 2);
          try {
            const bytes = new TextEncoder().encode(text);
            for (
              let offset = 0, record = 0;
              offset < bytes.byteLength;
              offset += defaults.recordBytes, record++
            ) {
              const data = bytes.subarray(
                offset,
                Math.min(offset + defaults.recordBytes, bytes.byteLength),
              );
              // Reserve a returned compressed block, header and one pending bounded write.
              // Synchronous compressor workspace is separate from queued transport storage.
              // Control uses identity, keeping its storage inside the independent reserve.
              const scratch = yield* acquire(
                output,
                control
                  ? data.byteLength + RPC_WEBTRANSPORT_RECORD_HEADER_BYTES
                  : defaults.recordBytes * 2 + 256,
              );
              try {
                const compressed = control ? undefined : deflateSync(data, { level: 1 });
                const useDeflate =
                  compressed !== undefined &&
                  compressed.byteLength <= defaults.compressedRecordBytes &&
                  compressed.byteLength < data.byteLength;
                const payload = useDeflate ? compressed : data;
                const h = new Uint8Array(RPC_WEBTRANSPORT_RECORD_HEADER_BYTES);
                const view = new DataView(h.buffer);
                view.setUint8(0, RPC_WEBTRANSPORT_WIRE_VERSION);
                view.setUint8(
                  1,
                  (offset + data.byteLength === bytes.byteLength ? 1 : 0) | (useDeflate ? 2 : 0),
                );
                view.setUint32(2, payload.byteLength);
                view.setUint32(6, data.byteLength);
                yield* transportTry((signal) => stream.write(h, signal));
                yield* transportTry((signal) => stream.write(payload, signal));
              } finally {
                scratch.release();
              }
              if (record % 8 === 7) yield* Effect.yieldNow;
            }
          } finally {
            source.release();
          }
        }).pipe(Effect.scoped),
      ),
    finish: writer.withPermits(1)(transportTry((signal) => stream.finish(signal))),
  };
}
