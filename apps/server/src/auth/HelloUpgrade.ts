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

// ws has no public setter for a connection's payload limit. Both the frame receiver and
// inflater enforce it independently, so switch both at this Node adapter boundary.
const setPayloadLimit = (ws: NodeWS.WebSocket, maxPayload: number) => {
  if (
    !Predicate.hasProperty(ws, "_receiver") ||
    !Predicate.hasProperty(ws._receiver, "_maxPayload") ||
    typeof ws._receiver._maxPayload !== "number"
  ) {
    throw new Error("Unsupported ws receiver payload limit");
  }
  const receiver = ws._receiver;
  if (
    Predicate.hasProperty(receiver, "_extensions") &&
    Predicate.hasProperty(receiver._extensions, "permessage-deflate")
  ) {
    const deflate = receiver._extensions["permessage-deflate"];
    if (!Predicate.hasProperty(deflate, "_maxPayload") || typeof deflate._maxPayload !== "number") {
      throw new Error("Unsupported ws inflater payload limit");
    }
    deflate._maxPayload = maxPayload;
  }
  receiver._maxPayload = maxPayload;
};

/** Keeps fragmented messages and bursts bounded before the pull-based socket can run. */
export class HelloWebSocketServer extends NodeWS.WebSocketServer {
  constructor() {
    super({
      noServer: true,
      // The client resets its dictionary after EVERY message, including BearerAuth.
      // Server compression keeps the production defaults, including context takeover.
      perMessageDeflate: { clientNoContextTakeover: true },
    });
  }

  override handleUpgrade(
    request: NodeHttp.IncomingMessage,
    socket: NodeStream.Duplex,
    head: Buffer,
    callback: (client: NodeWS.WebSocket, request: NodeHttp.IncomingMessage) => void,
  ): void {
    super.handleUpgrade(request, socket, head, (ws, upgradedRequest) => {
      const legacyMaxPayload = this.options.maxPayload;
      if (legacyMaxPayload === undefined) throw new Error("Missing ws default payload limit");
      setPayloadLimit(ws, RECONNECT_BUFFER_MAX_BYTES);
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
          if (rejected) return false;
          setPayloadLimit(ws, legacyMaxPayload);
          ws.off("message", onMessage);
          return true;
        },
      });
      ws.on("message", onMessage);
      callback(ws, upgradedRequest);
    });
  }
}
