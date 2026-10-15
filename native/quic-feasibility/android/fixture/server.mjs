import * as NodeFS from "node:fs";
import * as NodeURL from "node:url";
import * as NodePath from "node:path";
const { readFileSync } = NodeFS;
const { pathToFileURL } = NodeURL;
const { resolve } = NodePath;

// Import the repository's already-installed, pinned backend without installing another copy.
const [moduleDirectory, certPath, keyPath, port] = process.argv.slice(2);
const backend = resolve(moduleDirectory, "@fails-components/webtransport/lib/index.node.js");
const { Http3Server, quicheLoaded } = await import(pathToFileURL(backend).href);
await quicheLoaded;
const log = (event) => console.log(JSON.stringify(event));
const server = new Http3Server({
  port: Number(port),
  host: "127.0.0.1",
  secret: "isolated-native-probe",
  cert: readFileSync(certPath, "utf8"),
  privKey: readFileSync(keyPath, "utf8"),
});
server.setRequestCallback(async ({ header }) => {
  const authenticated = header.authorization === `Bearer ${process.env.T3_QUIC_PROBE_TOKEN}`;
  log({ event: "connect", authenticated, nativeHeader: header["x-t3-probe"] === "native-android" });
  return { status: authenticated ? 200 : 403, path: "/probe" };
});
const sessions = server.sessionStream("/probe").getReader();

async function serveStream(stream) {
  const reader = stream.readable.getReader();
  const writer = stream.writable.getWriter();
  let op;
  let bytes = 0;
  writer.closed.catch((error) =>
    log({
      event: "send-stopped",
      op,
      error: error.message,
      streamErrorCode: Number(error.message.match(/code:(\d+)/)?.[1]),
    }),
  );
  try {
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      op ??= next.value[0];
      bytes += next.value.length;
      if (op === 0) await writer.write(next.value);
      if (op === 1 && bytes === next.value.length) await writer.write(new Uint8Array([1]));
      // op 2 deliberately withholds data so the client must cancel its read on a deadline.
    }
    if (op === 0) {
      await writer.close();
      log({ event: "echo-finished", bytes });
    }
  } catch (error) {
    log({
      event: "receive-reset",
      op,
      bytes,
      error: error.message,
      streamErrorCode: Number(error.message.match(/code:(\d+)/)?.[1]),
    });
    await writer.abort(error).catch(() => {});
  }
}

async function serveSession(session) {
  await session.ready;
  session.closed.then(
    () => log({ event: "session-closed" }),
    () => log({ event: "session-failed" }),
  );
  const streams = session.incomingBidirectionalStreams.getReader();
  while (true) {
    const next = await streams.read();
    if (next.done) break;
    void serveStream(next.value);
  }
}

server.startServer();
await server.ready;
log({ event: "ready", port: Number(port), backend: "fails-components-1.6.9" });
for (const signal of ["SIGTERM", "SIGINT"]) {
  process.once(signal, () => {
    server.stopServer();
    process.exit(0);
  });
}
while (true) {
  const next = await sessions.read();
  if (next.done) break;
  void serveSession(next.value).catch((error) =>
    log({ event: "session-handler-error", error: error.message }),
  );
}
