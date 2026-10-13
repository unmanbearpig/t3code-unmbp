// Bench client transports. Each one exposes the same channel-addressed connection so workloads
// stay transport-agnostic: `ws`/`wss` and `wt-single` carry every channel on one ordered pipe
// (head-of-line blocking included), `wt-multi` gives every channel its own QUIC stream.
import { WebTransport, quicheLoaded } from "@fails-components/webtransport";
import WebSocket from "ws";

import { lengthPrefixed, makeFrameSplitter, ports, quicFlowControl } from "./protocol.ts";

export const transportNames = ["ws", "wss", "wt-single", "wt-multi"] as const;
export type TransportName = (typeof transportNames)[number];

export interface TlsMaterial {
  readonly certPem: string;
  readonly certSha256: Uint8Array;
}

export interface Connection {
  readonly send: (channel: number, frame: Uint8Array) => void;
  /** One receive handler per connection; workloads route frames by channel themselves. */
  onFrame: (frame: Uint8Array) => void;
  readonly close: () => void;
}

export async function connect(transport: TransportName, tls: TlsMaterial): Promise<Connection> {
  switch (transport) {
    case "ws":
    case "wss":
      return connectWebSocket(transport, tls);
    case "wt-single":
    case "wt-multi":
      return connectWebTransport(transport === "wt-multi", tls);
  }
}

function connectWebSocket(transport: "ws" | "wss", tls: TlsMaterial) {
  const url = transport === "ws" ? `ws://127.0.0.1:${ports.ws}/` : `wss://127.0.0.1:${ports.wss}/`;
  const socket = new WebSocket(url, { ca: tls.certPem, perMessageDeflate: false });
  return new Promise<Connection>((resolve, reject) => {
    const connection: Connection = {
      send: (_channel, frame) => socket.send(frame),
      onFrame: () => {},
      close: () => socket.terminate(),
    };
    socket.on("message", (data: Buffer) => connection.onFrame(new Uint8Array(data)));
    socket.once("open", () => resolve(connection));
    socket.once("error", reject);
  });
}

async function connectWebTransport(streamPerChannel: boolean, tls: TlsMaterial) {
  await quicheLoaded;
  const transport = new WebTransport(`https://127.0.0.1:${ports.wt}/bench`, {
    serverCertificateHashes: [{ algorithm: "sha-256", value: tls.certSha256 }],
    ...quicFlowControl,
  });
  await transport.ready;
  const writers = new Map<number, Promise<WritableStreamDefaultWriter<Uint8Array>>>();
  const connection: Connection = {
    send: (channel, frame) => {
      const key = streamPerChannel ? channel : 0;
      let writer = writers.get(key);
      if (!writer) {
        writer = transport.createBidirectionalStream().then((stream) => {
          const split = makeFrameSplitter((bytes) => connection.onFrame(bytes));
          void stream.readable
            .pipeTo(new WritableStream({ write: (chunk: Uint8Array) => split(chunk) }))
            .catch(() => {});
          return stream.writable.getWriter();
        });
        writers.set(key, writer);
      }
      void writer.then((w) => w.write(lengthPrefixed(frame))).catch(() => {});
    },
    onFrame: () => {},
    close: () => {
      try {
        transport.close();
      } catch {
        // Already closed by the peer.
      }
    },
  };
  return connection;
}
