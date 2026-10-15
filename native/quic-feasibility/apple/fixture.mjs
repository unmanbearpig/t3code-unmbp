import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { resolve } from "node:path";
import { randomBytes } from "node:crypto";

// Resolve the exact pinned Node backend from a caller-owned fixture installation.
const backend = resolve(process.env.T3_PROBE_NODE_MODULES, "node_modules/@fails-components/webtransport/lib/index.node.js");
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

async function serveStream(stream) {
  const reader = stream.readable.getReader();
  const writer = stream.writable.getWriter();
  const first = await reader.read();
  if (first.done) return;
  if (first.value[0] === 1) {
    // Cancellation probe: leave this stream pending while its sibling completes.
    await reader.read();
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
      const streams = session.incomingBidirectionalStreams.getReader();
      for (;;) {
        const { value, done } = await streams.read();
        if (done) return;
        void serveStream(value).catch(() => {});
      }
    })().catch((error) => console.error(error.message));
  }
})();

process.on("SIGTERM", () => {
  server.stopServer();
  // The pinned addon can retain handles after close. Only this fixture process exits.
  setTimeout(() => process.exit(0), 1000).unref();
});
