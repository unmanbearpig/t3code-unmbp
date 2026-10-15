// @effect-diagnostics nodeBuiltinImport:off - Exercises the Node upgrade adapter entirely in memory, with no listener.
import * as NodeHttp from "node:http";
import * as NodeEvents from "node:events";
import * as NodeNet from "node:net";
import * as NodeStream from "node:stream";
import { NodeWS } from "@effect/platform-node/NodeSocket";
import { describe, expect, it } from "@effect/vitest";
import { HelloWebSocketServer, authenticated } from "./HelloUpgrade.ts";

const upgrade = (compressed = false, legacy = false) => {
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
    "sec-websocket-extensions": compressed ? "permessage-deflate; client_max_window_bits" : "",
  };
  if (!compressed) delete request.headers["sec-websocket-extensions"];
  const server = legacy
    ? new NodeWS.WebSocketServer({ noServer: true, perMessageDeflate: true })
    : new HelloWebSocketServer();
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
const connect = async (offer = "permessage-deflate; client_max_window_bits") => {
  const server = new HelloWebSocketServer();
  const http = NodeHttp.createServer();
  let clientTransport: NodeStream.Duplex;
  const serverTransport = new NodeStream.Duplex({
    read() {},
    write(data, _encoding, done) {
      clientTransport.push(Buffer.from(data));
      done();
    },
  });
  clientTransport = new NodeStream.Duplex({
    read() {},
    write(data, _encoding, done) {
      serverTransport.push(Buffer.from(data));
      done();
    },
  });
  const connected = new Promise<{ ws: NodeWS.WebSocket; request: NodeHttp.IncomingMessage }>(
    (resolve) => {
      http.once("upgrade", (request, socket, head) => {
        request.headers["sec-websocket-extensions"] = offer;
        server.handleUpgrade(request, socket, head, (ws) => resolve({ ws, request }));
      });
    },
  );
  http.emit("connection", serverTransport);
  const client = new NodeWS.WebSocket("ws://test/ws?handshake=hello-v1", {
    createConnection: () => clientTransport,
    perMessageDeflate: true,
  });
  let extensions: string | undefined;
  client.on("upgrade", (response) => {
    extensions = response.headers["sec-websocket-extensions"];
  });
  const opened = NodeEvents.EventEmitter.once(client, "open");
  const { ws, request } = await connected;
  ws.on("error", () => {});
  client.on("error", () => {});
  await opened;
  return {
    ws,
    client,
    request,
    extensions,
    cleanup: () => {
      client.terminate();
      ws.terminate();
      clientTransport.destroy();
      serverTransport.destroy();
      server.close();
      http.close();
    },
  };
};

const maskedFrame = (length: number, opcode = 0x81) => {
  const frame = Buffer.alloc(14 + length, 97);
  frame[0] = opcode;
  frame[1] = 0xff;
  frame.writeBigUInt64BE(BigInt(length), 2);
  frame.fill(0, 10, 14);
  return frame;
};

describe("H Node admission", () => {
  it.each([
    "permessage-deflate; client_max_window_bits",
    "permessage-deflate",
    "permessage-deflate; server_no_context_takeover; client_no_context_takeover",
  ])("negotiates client dictionary resets for offer %s", async (offer) => {
    const h = await connect(offer);
    try {
      expect(h.client.extensions).toBe("permessage-deflate");
      expect(h.ws.extensions).toBe("permessage-deflate");
      expect(h.extensions).toBe(
        offer.includes("server_no_context_takeover")
          ? "permessage-deflate; server_no_context_takeover; client_no_context_takeover"
          : "permessage-deflate; client_no_context_takeover",
      );
      const received = NodeEvents.EventEmitter.once(h.ws, "message");
      h.client.send("auth".repeat(1024));
      expect((await received)[0].toString()).toBe("auth".repeat(1024));
      const reply = NodeEvents.EventEmitter.once(h.client, "message");
      h.ws.send("snapshot".repeat(1024));
      expect((await reply)[0].toString()).toBe("snapshot".repeat(1024));
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
  it.each([false, true])(
    "accepts fragmented messages above 64 KiB after auth, compression %s",
    async (compress) => {
      const h = await connect();
      try {
        expect(authenticated(h.request)).toBe(true);
        const message = NodeEvents.EventEmitter.once(h.ws, "message");
        h.client.send("x".repeat(32768), { fin: false, compress });
        h.client.send("y".repeat(32769), { fin: true, compress });
        expect((await message)[0].toString()).toBe("x".repeat(32768) + "y".repeat(32769));
      } finally {
        h.cleanup();
      }
    },
  );
  it("rejects compressed expansion above the pre-auth parser ceiling", async () => {
    const h = await connect();
    try {
      const error = NodeEvents.EventEmitter.once(h.ws, "error");
      h.client.send("x".repeat(65537), { compress: true });
      expect((await error)[0].code).toBe("WS_ERR_UNSUPPORTED_MESSAGE_LENGTH");
    } finally {
      h.cleanup();
    }
  });
  it.each([false, true])(
    "matches the legacy parser ceiling after auth, legacy %s",
    async (legacy) => {
      const h = upgrade(false, legacy);
      try {
        expect(authenticated(h.request)).toBe(true);
        const error = NodeEvents.EventEmitter.once(h.ws, "error");
        const header = Buffer.alloc(14);
        header[0] = 0x81;
        header[1] = 0xff;
        header.writeBigUInt64BE(BigInt(100 * 1024 * 1024 + 1), 2);
        h.transport.push(header);
        expect((await error)[0].code).toBe("WS_ERR_UNSUPPORTED_MESSAGE_LENGTH");
      } finally {
        h.cleanup();
      }
    },
  );
  it("rejects a message over the hard cap in the actual WS frame parser", async () => {
    const h = upgrade();
    try {
      const error = new Promise<Error & { code?: string }>((resolve) =>
        h.ws.once("error", resolve),
      );
      h.transport.push(maskedFrame(65537));
      expect((await error).code).toBe("WS_ERR_UNSUPPORTED_MESSAGE_LENGTH");
    } finally {
      h.cleanup();
    }
  });
});
