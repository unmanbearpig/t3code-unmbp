// @effect-diagnostics nodeBuiltinImport:off - Isolated native transport integration fixture.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { Http3Server, WebTransport, quicheLoaded } from "@fails-components/webtransport";
import type {
  WebTransportSession,
  WebTransportBidirectionalStream,
} from "@fails-components/webtransport";
import * as Effect from "effect/Effect";
import * as NetAddress from "effect/net/NetAddress";
import * as Socket from "effect/socket/Socket";
import * as SocketServer from "effect/socket/SocketServer";

export async function openQuicFixture(
  options: {
    readonly port?: number;
    readonly request?: (
      path: string,
    ) => Promise<{ readonly status: number; readonly path: string }>;
  } = {},
) {
  await quicheLoaded;
  const directory = await NodeFSP.mkdtemp(
    NodePath.join(NodeOS.homedir(), "codex-tmp", "t3-quic-protocol-"),
  );
  const certPath = NodePath.join(directory, "cert.pem");
  const keyPath = NodePath.join(directory, "key.pem");
  NodeChildProcess.execFileSync(
    "openssl",
    [
      "req",
      "-x509",
      "-newkey",
      "ec",
      "-pkeyopt",
      "ec_paramgen_curve:prime256v1",
      "-nodes",
      "-keyout",
      keyPath,
      "-out",
      certPath,
      "-days",
      "10",
      "-subj",
      "/CN=localhost",
      "-addext",
      "subjectAltName=DNS:localhost,IP:127.0.0.1",
    ],
    { stdio: "ignore" },
  );
  const cert = await NodeFSP.readFile(certPath, "utf8");
  const key = await NodeFSP.readFile(keyPath, "utf8");
  const server = new Http3Server({
    port: options.port ?? 0,
    host: "127.0.0.1",
    secret: "isolated-protocol-fixture",
    cert,
    privKey: key,
    defaultDatagramsReadableMode: "bytes",
  });
  if (options.request !== undefined) {
    server.setRequestCallback(({ header }: { readonly header: Record<string, string> }) =>
      options.request!(header[":path"] ?? ""),
    );
  }
  const sessions = server.sessionStream("/rpc", options.request ? { noAutoPaths: true } : {});
  server.startServer();
  await server.ready;
  const port = server.address()!.port;
  const clients: WebTransport[] = [];
  let closed = false;
  const serverSessions: WebTransportSession[] = [];
  const streams: { readonly close: () => Promise<void>; readonly reset: () => void }[] = [];
  const connect = async (query = "") => {
    const client = new WebTransport(`https://127.0.0.1:${port}/rpc${query}`, {
      serverCertificateHashes: [
        {
          algorithm: "sha-256",
          value: new Uint8Array(
            NodeCrypto.createHash("sha256")
              .update(new NodeCrypto.X509Certificate(cert).raw)
              .digest(),
          ),
        },
      ],
      requireUnreliable: true,
    });
    const nativeClose = client.close.bind(client);
    let closed = false;
    client.close = () => {
      if (closed) return;
      closed = true;
      nativeClose();
    };
    clients.push(client);
    void client.closed.catch(() => {});
    await client.ready;
    return client;
  };

  const socketServer: SocketServer.SocketServer["Service"] = {
    address: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", port),
    run: (handle) =>
      Effect.gen(function* () {
        const scope = yield* Effect.scope;
        const reader = sessions.getReader();
        while (true) {
          const next = yield* Effect.promise(() => reader.read());
          if (next.done) return yield* Effect.never;
          const session = next.value;
          serverSessions.push(session);
          yield* Effect.gen(function* () {
            const incoming = (
              session.incomingBidirectionalStreams as ReadableStream<WebTransportBidirectionalStream>
            ).getReader();
            while (true) {
              const nextStream = yield* Effect.promise(() => incoming.read());
              if (nextStream.done) return;
              const stream = nextStream.value;
              const writer = stream.writable.getWriter();
              streams.push({
                close: () => writer.close(),
                reset: () => {
                  void writer.abort(new Error("Deliberate stream reset")).catch(() => {});
                },
              });
              yield* Effect.gen(function* () {
                const socket = yield* Socket.fromTransformStream(
                  Effect.succeed({
                    readable: stream.readable,
                    writable: new WritableStream<Uint8Array>({
                      write: (bytes) => writer.write(bytes),
                    }),
                  }),
                );
                yield* Effect.raceFirst(
                  handle(socket),
                  Effect.promise(() => session.closed).pipe(Effect.asVoid),
                );
              }).pipe(
                Effect.scoped,
                Effect.ignoreCause,
                Effect.ensuring(Effect.promise(() => writer.close().catch(() => {}))),
                Effect.forkIn(scope),
              );
            }
          }).pipe(Effect.scoped, Effect.ignoreCause, Effect.forkIn(scope));
        }
      }).pipe(Effect.scoped),
  };

  return {
    connect,
    socketServer,
    streams,
    serverSessions,
    sessions,
    async close() {
      if (closed) return;
      closed = true;
      for (const client of clients) client.close();
      await Promise.allSettled(clients.map((client) => client.closed));
      server.stopServer();
      await server.closed;
      await NodeFSP.rm(directory, { recursive: true, force: true });
    },
  };
}
