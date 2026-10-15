// @effect-diagnostics nodeBuiltinImport:off - Exercises the Node upgrade adapter entirely in memory, with no listener.
import * as NodeHttp from "node:http";
import * as NodeNet from "node:net";
import * as NodeStream from "node:stream";
import { NodeWS } from "@effect/platform-node/NodeSocket";
import { describe, expect, it } from "@effect/vitest";
import { HelloWebSocketServer, authenticated } from "./HelloUpgrade.ts";

const upgrade = () => {
  const writes: Buffer[] = [];
  const transport = new NodeStream.Duplex({
    read() {},
    write(data, _encoding, done) {
      writes.push(Buffer.from(data));
      done();
    },
  });
  const requestSocket = new NodeNet.Socket();
  const request = new NodeHttp.IncomingMessage(requestSocket);
  request.method = "GET";
  request.url = "/ws?handshake=hello-v1";
  request.headers = {
    host: "test",
    upgrade: "websocket",
    connection: "Upgrade",
    "sec-websocket-key": "dGhlIHNhbXBsZSBub25jZQ==",
    "sec-websocket-version": "13",
    "sec-websocket-extensions": "permessage-deflate; client_max_window_bits",
  };
  const server = new HelloWebSocketServer();
  let client: NodeWS.WebSocket | undefined;
  server.handleUpgrade(request, transport, Buffer.alloc(0), (ws) => {
    client = ws;
  });
  if (client === undefined) throw new Error("In-memory upgrade failed");
  const ws = client;
  return {
    request,
    ws,
    transport,
    writes,
    cleanup: () => {
      ws.terminate();
      transport.destroy();
      requestSocket.destroy();
      server.close();
    },
  };
};
describe("H Node admission", () => {
  it("refuses compression even when the client offers it", () => {
    const h = upgrade();
    try {
      expect(h.ws.extensions).toBe("");
      expect(h.writes[0]!.toString()).toContain("101 Switching Protocols");
      expect(h.writes[0]!.toString()).not.toContain("permessage-deflate");
    } finally {
      h.cleanup();
    }
  });
  it.each(["oversized", "binary", "burst", "bytes"] as const)(
    "closes %s input and discards subsequent frames before Effect can buffer them",
    (kind) => {
      const h = upgrade();
      try {
        const frame = Buffer.from(kind === "oversized" ? "x".repeat(8193) : "auth");
        h.ws.emit("message", frame, kind === "binary");
        if (kind === "burst") {
          for (let i = 0; i < 17; i++) h.ws.emit("message", Buffer.alloc(0), false);
        }
        if (kind === "bytes") {
          h.ws.emit("message", Buffer.alloc(65536), false);
          h.ws.emit("message", Buffer.from("x"), false);
        }
        expect(h.ws.readyState).toBe(NodeWS.WebSocket.CLOSING);
        expect(authenticated(h.request)).toBe(false);
        expect(Buffer.concat(h.writes).toString()).toContain('"HandshakeError"');
        const count = h.writes.length;
        for (let i = 0; i < 100; i++) h.ws.emit("message", Buffer.alloc(0), false);
        expect(h.writes).toHaveLength(count);
      } finally {
        h.cleanup();
      }
    },
  );
  it("ends admission after authentication so later messages do not consume the pre-auth budget", () => {
    const h = upgrade();
    try {
      h.ws.emit("message", Buffer.from("auth"), false);
      expect(authenticated(h.request)).toBe(true);
      for (let i = 0; i < 100; i++) h.ws.emit("message", Buffer.from("rpc"), false);
      expect(h.ws.readyState).toBe(NodeWS.WebSocket.OPEN);
    } finally {
      h.cleanup();
    }
  });
  it("rejects a message over the hard cap in the actual WS frame parser", async () => {
    const h = upgrade();
    try {
      const error = new Promise<Error & { code?: string }>((resolve) =>
        h.ws.once("error", resolve),
      );
      const frame = Buffer.alloc(14 + 65537, 97);
      frame[0] = 0x81;
      frame[1] = 0xff;
      frame.writeBigUInt64BE(65537n, 2);
      frame.fill(0, 10, 14);
      h.transport.push(frame);
      expect((await error).code).toBe("WS_ERR_UNSUPPORTED_MESSAGE_LENGTH");
    } finally {
      h.cleanup();
    }
  });
});
