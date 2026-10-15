import {
  RECONNECT_AUTH_DEADLINE_MS,
  RECONNECT_AUTH_MAX_BYTES,
  RECONNECT_BUFFER_MAX_BYTES,
  ReconnectServerControl,
  ReconnectBearerAuth,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Socket from "effect/socket/Socket";
import type { ConnectionAttemptError, PreparedConnection } from "../connection/model.ts";
import { ConnectionBlockedError, ConnectionTransientError } from "../connection/model.ts";
import { environmentMismatchError } from "../connection/errors.ts";
import { orchestrationProtocolCompatibilityError } from "../connection/compatibility.ts";

const decode = Schema.decodeUnknownEffect(Schema.fromJsonString(ReconnectServerControl));
const decodeHeader = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({
      _tag: Schema.String,
      handshakeVersion: Schema.optionalKey(Schema.Number),
    }),
  ),
);
const encodeAuth = Schema.encodeEffect(Schema.fromJsonString(ReconnectBearerAuth));
const transportError = () =>
  new Socket.SocketError({
    reason: new Socket.SocketReadError({ cause: "Reconnect handshake failed" }),
  });
const encoder = new TextEncoder();

/** Gates RPC writes until Hello is checked and auth is sent, then strips handshake control frames. */
export const wrap = Effect.fnUntraced(function* (
  socket: Socket.Socket,
  connection: PreparedConnection,
  failure: Deferred.Deferred<never, ConnectionAttemptError>,
) {
  const sent = yield* Deferred.make<void, Socket.SocketError>();
  const unavailable = () =>
    new ConnectionTransientError({
      reason: "transport",
      detail: "Server-first hello is unavailable.",
      helloFallback: true,
    });
  const reject = (error: ConnectionAttemptError) =>
    Effect.gen(function* () {
      yield* Deferred.fail(failure, error);
      yield* Deferred.fail(sent, transportError());
      return yield* transportError();
    });
  return Socket.make({
    writer: socket.writer.pipe(
      Effect.map((writer) => ({
        write: (frame) => Deferred.await(sent).pipe(Effect.andThen(writer.write(frame))),
        writeAll: (frames) => Deferred.await(sent).pipe(Effect.andThen(writer.writeAll(frames))),
      })),
    ),
    reader: Effect.gen(function* () {
      const reader = yield* socket.reader.pipe(Effect.catch(() => reject(unavailable())));
      const writer = yield* socket.writer;
      const [first, ...rest] = yield* reader.pull.pipe(
        Effect.timeout(RECONNECT_AUTH_DEADLINE_MS),
        Effect.catch(() => reject(unavailable())),
      );
      const text = typeof first === "string" ? first : new TextDecoder().decode(first);
      if (encoder.encode(text).byteLength > RECONNECT_BUFFER_MAX_BYTES)
        return yield* reject(unavailable());
      const header = yield* decodeHeader(text).pipe(Effect.catch(() => reject(unavailable())));
      if (header._tag === "Hello" && header.handshakeVersion !== 1) {
        return yield* reject(
          new ConnectionBlockedError({
            reason: "unsupported",
            detail: "This client does not support the server's reconnect handshake.",
          }),
        );
      }
      const hello = yield* decode(text).pipe(Effect.catch(() => reject(unavailable())));
      if (hello._tag === "HandshakeError")
        return yield* reject(
          hello.code === "auth_rejected"
            ? new ConnectionBlockedError({
                reason: "authentication",
                detail: "The environment rejected the credential.",
              })
            : new ConnectionTransientError({
                reason: "transport",
                detail: "Reconnect handshake failed.",
              }),
        );
      if (hello._tag !== "Hello") return yield* reject(unavailable());
      if (hello.environment.environmentId !== connection.environmentId) {
        return yield* reject(
          environmentMismatchError({
            expected: connection.environmentId,
            actual: hello.environment.environmentId,
          }),
        );
      }
      const compatibility = orchestrationProtocolCompatibilityError(hello.environment);
      if (compatibility !== null) return yield* reject(compatibility);
      const authorization = connection.httpAuthorization;
      if (authorization?._tag !== "Bearer")
        return yield* reject(
          new ConnectionBlockedError({
            reason: "configuration",
            detail: "Hello requires bearer authorization.",
          }),
        );
      const authFrame = yield* encodeAuth({
        _tag: "BearerAuth",
        handshakeVersion: 1,
        token: authorization.token,
      }).pipe(
        Effect.catch(() =>
          reject(
            new ConnectionBlockedError({
              reason: "authentication",
              detail: "Invalid reconnect credential.",
            }),
          ),
        ),
      );
      if (encoder.encode(authFrame).byteLength > RECONNECT_AUTH_MAX_BYTES)
        return yield* reject(
          new ConnectionBlockedError({
            reason: "authentication",
            detail: "Reconnect credential exceeds the frame limit.",
          }),
        );
      yield* writer.write(authFrame).pipe(
        Effect.catch(() =>
          reject(
            new ConnectionTransientError({
              reason: "transport",
              detail: "Could not send reconnect credential.",
            }),
          ),
        ),
      );
      yield* Deferred.succeed(sent, undefined);
      let buffered = rest;
      let authenticated = false;
      return {
        ...reader,
        pull: Effect.gen(function* () {
          while (true) {
            const frames =
              buffered.length > 0
                ? buffered
                : yield* authenticated
                    ? reader.pull
                    : reader.pull.pipe(
                        Effect.timeout(RECONNECT_AUTH_DEADLINE_MS),
                        Effect.catch(() =>
                          reject(
                            new ConnectionTransientError({
                              reason: "transport",
                              detail: "Reconnect authentication did not complete.",
                            }),
                          ),
                        ),
                      );
            buffered = [];
            const output: Array<string | Uint8Array> = [];
            for (const frame of frames) {
              if (authenticated) {
                output.push(frame);
                continue;
              }
              const control = yield* decode(
                typeof frame === "string" ? frame : new TextDecoder().decode(frame),
              ).pipe(
                Effect.catch(() =>
                  reject(
                    new ConnectionTransientError({
                      reason: "transport",
                      detail: "Invalid reconnect control frame.",
                    }),
                  ),
                ),
              );
              if (control._tag === "HandshakeError")
                return yield* reject(
                  control.code === "auth_rejected"
                    ? new ConnectionBlockedError({
                        reason: "authentication",
                        detail: "The environment rejected the credential.",
                      })
                    : new ConnectionTransientError({
                        reason: "transport",
                        detail: "Reconnect handshake failed.",
                      }),
                );
              if (control._tag !== "Authenticated")
                return yield* reject(
                  new ConnectionTransientError({
                    reason: "transport",
                    detail: "Unexpected reconnect control frame.",
                  }),
                );
              authenticated = true;
            }
            if (output.length > 0)
              return output as [string | Uint8Array, ...Array<string | Uint8Array>];
          }
        }),
      };
    }),
  });
});
