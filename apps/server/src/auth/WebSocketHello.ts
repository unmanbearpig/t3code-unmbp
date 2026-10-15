import {
  RECONNECT_AUTH_DEADLINE_MS,
  RECONNECT_AUTH_MAX_BYTES,
  RECONNECT_BUFFER_MAX_BYTES,
  RECONNECT_BUFFER_MAX_FRAMES,
  ReconnectBearerAuth,
  ReconnectHello,
  ReconnectAuthenticated,
  ReconnectHandshakeError,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Socket from "effect/socket/Socket";
import * as EnvironmentAuth from "./EnvironmentAuth.ts";

class HandshakeFailure extends Schema.TaggedError<HandshakeFailure>()("HandshakeFailure", {
  code: ReconnectHandshakeError.fields.code,
}) {}
const decodeAuth = Schema.decodeUnknownEffect(Schema.fromJsonString(ReconnectBearerAuth));
const encodeHello = Schema.encodeSync(Schema.fromJsonString(ReconnectHello));
const encodeAuthenticated = Schema.encodeSync(Schema.fromJsonString(ReconnectAuthenticated));
const encodeError = Schema.encodeSync(Schema.fromJsonString(ReconnectHandshakeError));
const encoder = new TextEncoder();
const size = (frame: string | Uint8Array) =>
  typeof frame === "string" ? encoder.encode(frame).byteLength : frame.byteLength;

/** Acquires one reader in the socket scope and transfers it, including pipelined frames, to RPC. */
export const authenticate = Effect.fnUntraced(function* (
  socket: Socket.Socket,
  hello: ReconnectHello,
  completeAdmission: () => boolean = () => true,
) {
  const auth = yield* EnvironmentAuth.EnvironmentAuth;
  const startedAt = yield* Clock.currentTimeMillis;
  const reader = yield* socket.reader.pipe(
    Effect.timeoutOrElse({
      duration: RECONNECT_AUTH_DEADLINE_MS,
      orElse: () =>
        Effect.fail(
          new Socket.SocketError({
            reason: new Socket.SocketReadError({ cause: "Hello upgrade deadline exceeded" }),
          }),
        ),
    }),
  );
  const writer = yield* socket.writer;
  let buffered: Array<string | Uint8Array> = [];
  let bytes = 0;
  const buffer = (frames: ReadonlyArray<string | Uint8Array>) => {
    for (const frame of frames) {
      const nextBytes = bytes + size(frame);
      if (
        buffered.length >= RECONNECT_BUFFER_MAX_FRAMES ||
        nextBytes > RECONNECT_BUFFER_MAX_BYTES
      ) {
        return Effect.fail(new HandshakeFailure({ code: "limit_exceeded" }));
      }
      bytes = nextBytes;
      buffered.push(frame);
    }
    return Effect.void;
  };
  const remainingMs = Math.max(
    0,
    RECONNECT_AUTH_DEADLINE_MS - ((yield* Clock.currentTimeMillis) - startedAt),
  );
  const handshake = Effect.gen(function* () {
    yield* writer.write(encodeHello(hello));
    const [first, ...rest] = yield* reader.pull;
    if (size(first) > RECONNECT_AUTH_MAX_BYTES) {
      return yield* new HandshakeFailure({ code: "limit_exceeded" });
    }
    // Only text frames are credentials. Parse failures deliberately discard their raw cause.
    if (typeof first !== "string") return yield* new HandshakeFailure({ code: "invalid_frame" });
    const frame = yield* decodeAuth(first).pipe(
      Effect.mapError(() => new HandshakeFailure({ code: "invalid_frame" })),
    );
    yield* buffer(rest);
    const pending = Effect.forever(reader.pull.pipe(Effect.flatMap(buffer)));
    const session = yield* Effect.raceFirst(
      auth.authenticateWebSocketFrame(frame.token).pipe(
        Effect.mapError(
          (error) =>
            new HandshakeFailure({
              code: EnvironmentAuth.isServerAuthCredentialError(error)
                ? "auth_rejected"
                : "internal_error",
            }),
        ),
      ),
      pending,
    );
    if (!completeAdmission()) return yield* new HandshakeFailure({ code: "limit_exceeded" });
    yield* writer.write(encodeAuthenticated({ _tag: "Authenticated", handshakeVersion: 1 }));
    const adopted = Socket.make({
      writer: Effect.succeed(writer),
      reader: Effect.succeed({
        ...reader,
        pull: Effect.suspend(() => {
          const frames = buffered;
          buffered = [];
          return frames.length > 0
            ? Effect.succeed(frames as [string | Uint8Array, ...Array<string | Uint8Array>])
            : reader.pull;
        }),
      }),
    });
    return { session, socket: adopted };
  }).pipe(
    Effect.timeoutOrElse({
      duration: remainingMs,
      orElse: () => Effect.fail(new HandshakeFailure({ code: "auth_timeout" })),
    }),
  );
  return yield* handshake.pipe(
    Effect.catchTags({
      HandshakeFailure: (error) =>
        Effect.gen(function* () {
          buffered = [];
          yield* writer
            .write(
              encodeError({
                _tag: "HandshakeError",
                handshakeVersion: 1,
                code: error.code,
              }),
            )
            .pipe(Effect.ignore);
          yield* writer.write(new Socket.CloseEvent(1008, error.code)).pipe(Effect.ignore);
          return undefined;
        }),
    }),
  );
});
