// @effect-diagnostics nodeBuiltinImport:off - Per-connection Node WebSocket admission before Effect receives frames.
import type * as NodeHttp from "node:http";
import type * as NodeStream from "node:stream";
import { NodeWS } from "@effect/platform-node/NodeSocket";
import {
  RECONNECT_AUTH_MAX_BYTES,
  RECONNECT_BUFFER_MAX_BYTES,
  RECONNECT_BUFFER_MAX_FRAMES,
  ReconnectHandshakeError,
} from "@t3tools/contracts";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";

const finishAdmission = Symbol("hello.finishAdmission");
const encodeError = Schema.encodeSync(Schema.fromJsonString(ReconnectHandshakeError));

/** Ends raw-frame admission only after framed authentication has succeeded. */
export const authenticated = (source: unknown): boolean => {
  if (
    Predicate.hasProperty(source, finishAdmission) &&
    typeof source[finishAdmission] === "function"
  ) {
    return source[finishAdmission]() === true;
  }
  return true;
};

/** Keeps fragmented messages and bursts bounded before the pull-based socket can run. */
export class HelloWebSocketServer extends NodeWS.WebSocketServer {
  constructor() {
    super({ noServer: true, maxPayload: RECONNECT_BUFFER_MAX_BYTES, perMessageDeflate: false });
  }

  override handleUpgrade(
    request: NodeHttp.IncomingMessage,
    socket: NodeStream.Duplex,
    head: Buffer,
    callback: (client: NodeWS.WebSocket, request: NodeHttp.IncomingMessage) => void,
  ): void {
    super.handleUpgrade(request, socket, head, (ws, upgradedRequest) => {
      let frames = 0;
      let bufferedBytes = 0;
      let rejected = false;
      const onMessage = (data: NodeWS.RawData, binary: boolean) => {
        if (rejected) return;
        const bytes = Array.isArray(data)
          ? data.reduce((sum, chunk) => sum + chunk.byteLength, 0)
          : data.byteLength;
        frames++;
        if (frames > 1) bufferedBytes += bytes;
        const code =
          frames === 1 && binary
            ? "invalid_frame"
            : (frames === 1 && bytes > RECONNECT_AUTH_MAX_BYTES) ||
                frames > RECONNECT_BUFFER_MAX_FRAMES + 1 ||
                bufferedBytes > RECONNECT_BUFFER_MAX_BYTES
              ? "limit_exceeded"
              : undefined;
        if (code !== undefined) {
          rejected = true;
          // Stop subsequent frames in this parser batch reaching the pull queue.
          ws.removeAllListeners("message");
          ws.pause();
          ws.send(encodeError({ _tag: "HandshakeError", handshakeVersion: 1, code }));
          ws.close(1008, code);
        }
      };
      Object.defineProperty(upgradedRequest, finishAdmission, {
        value: () => {
          ws.off("message", onMessage);
          return !rejected;
        },
      });
      ws.on("message", onMessage);
      callback(ws, upgradedRequest);
    });
  }
}
