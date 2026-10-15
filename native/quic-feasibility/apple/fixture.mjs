import * as NodeFSP from "node:fs/promises";
import * as NodeURL from "node:url";
import * as NodePath from "node:path";
import * as NodeCrypto from "node:crypto";
const { readFile } = NodeFSP;
const { pathToFileURL } = NodeURL;
const { resolve } = NodePath;
const { randomBytes } = NodeCrypto;

// Resolve the exact pinned Node backend from a caller-owned fixture installation.
const backend = resolve(
  process.env.T3_PROBE_NODE_MODULES,
  "node_modules/@fails-components/webtransport/lib/index.node.js",
);
const { Http3Server, quicheLoaded } = await import(pathToFileURL(backend).href);
await quicheLoaded;
const [certPath, keyPath] = process.argv.slice(2);
const server = new Http3Server({
  host: "127.0.0.1",
  port: 0,
  cert: await readFile(certPath, "utf8"),
  privKey: await readFile(keyPath, "utf8"),
  secret: randomBytes(32).toString("hex"),
});
server.setRequestCallback(async ({ header }) => ({
  status: header.authorization === "Bearer isolated-fixture-ticket" ? 200 : 401,
  path: "/probe",
}));
const sessions = server.sessionStream("/probe").getReader();
server.startServer();
await server.ready;
console.log(JSON.stringify({ url: `https://127.0.0.1:${server.address().port}/probe` }));

async function serveStream(stream, cancellation) {
  const reader = stream.readable.getReader();
  const writer = stream.writable.getWriter();
  const first = await reader.read();
  if (first.done) return;
  if (first.value[0] === 1) {
    // The pinned backend exposes received stream error codes in its public errors' messages.
    const reset = reader.read().then(
      () => "unexpected readable completion",
      (error) => error.message,
    );
    const stop = writer.closed.then(
      () => "unexpected writable completion",
      (error) => error.message,
    );
    await writer.write(Uint8Array.of(1));
    const [resetError, stopError] = await Promise.all([reset, stop]);
    cancellation.resolve(
      `reset=${resetError.replace(/^Resetstream with code:/, "")};stop=${stopError.replace(/^StopSending with code:/, "")}`,
    );
    return;
  }
  if (first.value[0] === 2) {
    await writer.write(new TextEncoder().encode(await cancellation.promise));
    await writer.close();
    return;
  }
  if (first.value.length > 1) await writer.write(first.value.subarray(1));
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    await writer.write(value);
  }
  await writer.close();
}

void (async () => {
  for (;;) {
    const { value: session, done } = await sessions.read();
    if (done) return;
    void (async () => {
      await session.ready;
      session.closed.catch(() => {});
      const cancellation = {};
      cancellation.promise = new Promise((resolve) => {
        cancellation.resolve = resolve;
      });
      const streams = session.incomingBidirectionalStreams.getReader();
      for (;;) {
        const { value, done } = await streams.read();
        if (done) return;
        void serveStream(value, cancellation).catch(() => {});
      }
    })().catch((error) => console.error(error.message));
  }
})();

process.on("SIGTERM", () => {
  server.stopServer();
  // The pinned addon can retain handles after close. Only this fixture process exits.
  setTimeout(() => process.exit(0), 1000).unref();
});
