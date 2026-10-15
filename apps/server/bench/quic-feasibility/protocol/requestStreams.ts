import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import { RpcClient, RpcClientError, RpcSerialization } from "effect/rpc";
import type { FromClientEncoded, FromServerEncoded } from "effect/rpc/RpcMessage";

import { makeFramedJson } from "./framedJson.ts";

interface DuplexStream {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
}

/** Standard stream types keep the experimental adapter independent of Node native addons. */
interface Session {
  readonly createBidirectionalStream: () => Promise<DuplexStream>;
  readonly closed: Promise<unknown>;
  readonly close: () => void;
}

const protocolError = (cause: unknown) =>
  new RpcClientError.RpcClientError({
    reason: new RpcClientError.RpcClientDefect({
      message: "QUIC RPC transport closed or received an incomplete response",
      cause,
    }),
  });

/**
 * Experimental client adapter. A failed stream fails the session and all its calls.
 * Authentication, application liveness probes and reconnect policy belong to its owner.
 */
export const makeRequestStreamProtocol = (session: Session) =>
  RpcClient.Protocol.make(
    Effect.fnUntraced(function* (writeResponse, clientIds) {
      const scope = yield* Effect.scope;
      const pending = new Map<
        string | number,
        {
          readonly parser: ReturnType<typeof makeFramedJson>;
          readonly writer: Promise<WritableStreamDefaultWriter<Uint8Array>>;
        }
      >();
      let closedError: RpcClientError.RpcClientError | undefined;
      let closeRequested = false;
      const closeSession = () => {
        if (closeRequested) return;
        closeRequested = true;
        session.close();
      };

      const failSession = (cause: unknown) =>
        Effect.suspend(() => {
          if (closedError !== undefined) return Effect.void;
          closedError = protocolError(cause);
          const error = closedError;
          pending.clear();
          closeSession();
          return Effect.forEach(clientIds, (clientId) =>
            writeResponse(clientId, { _tag: "ClientProtocolError", error }),
          ).pipe(Effect.asVoid);
        });

      yield* Effect.tryPromise(() => session.closed).pipe(
        Effect.matchEffect({ onSuccess: failSession, onFailure: failSession }),
        Effect.forkIn(scope),
      );

      const readResponses = (
        clientId: number,
        requestId: string | number,
        stream: DuplexStream,
        parser: ReturnType<typeof makeFramedJson>,
        writer: Promise<WritableStreamDefaultWriter<Uint8Array>>,
      ) =>
        Effect.gen(function* () {
          const reader = stream.readable.getReader();
          yield* Effect.addFinalizer(() => Effect.promise(() => reader.cancel().catch(() => {})));
          while (true) {
            const next = yield* Effect.tryPromise(() => reader.read());
            if (next.done) {
              parser.finish();
              if (pending.has(requestId)) throw new Error("Stream ended before RPC Exit");
              return;
            }
            const responses = parser.decode(next.value) as FromServerEncoded[];
            for (const response of responses) {
              if ("requestId" in response && response.requestId !== requestId) {
                throw new Error("Response belongs to a different RPC stream");
              }
              if (response._tag === "Exit") {
                pending.delete(requestId);
                yield* Effect.tryPromise(() => writer.then((value) => value.close()));
              }
              yield* writeResponse(clientId, response);
            }
          }
        }).pipe(
          Effect.scoped,
          Effect.catchCause((cause) => failSession(Cause.squash(cause))),
        );

      const send = (clientId: number, message: FromClientEncoded) =>
        Effect.gen(function* () {
          if (closedError !== undefined) return yield* closedError;
          if (message._tag === "Eof") return;
          // No socket pinger is installed by this adapter. Use an RPC probe until a control
          // stream supplies the socket protocol's keepalive and ConnectionHooks behavior.
          if (message._tag === "Ping") {
            return yield* protocolError(new Error("A QUIC control stream is required"));
          }
          const requestId = message._tag === "Request" ? message.id : message.requestId;
          if (message._tag === "Request") {
            const parser = makeFramedJson();
            const opened = session.createBidirectionalStream();
            const writer = opened.then((stream) => stream.writable.getWriter());
            void writer.catch(() => {});
            pending.set(requestId, { parser, writer });
            const stream = yield* Effect.tryPromise(() => opened);
            yield* Effect.forkIn(readResponses(clientId, requestId, stream, parser, writer), scope);
          }
          const request = pending.get(requestId);
          if (request === undefined) return;
          const bytes = request.parser.encode(message);
          const writer = yield* Effect.tryPromise(() => request.writer);
          yield* Effect.tryPromise(() => writer.write(bytes));
        }).pipe(
          Effect.catchCause((cause) =>
            failSession(Cause.squash(cause)).pipe(
              Effect.andThen(Effect.fail(closedError ?? protocolError(Cause.squash(cause)))),
            ),
          ),
        );

      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          pending.clear();
          closeSession();
        }),
      );
      return {
        send,
        supportsAck: true,
        supportsTransferables: false,
        codecFor: RpcSerialization.json.codecFor,
      };
    }),
  );
