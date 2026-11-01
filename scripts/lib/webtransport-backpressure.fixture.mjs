import * as NodeAssert from "node:assert/strict";
import * as NodeChildProcess from "node:child_process";
const { execFileSync } = NodeChildProcess;
import * as NodeCrypto from "node:crypto";
const { createHash, randomUUID, X509Certificate } = NodeCrypto;
import * as NodeFSP from "node:fs/promises";
const { mkdtemp, readFile, rm } = NodeFSP;
import * as NodeModule from "node:module";
const { createRequire, findPackageJSON } = NodeModule;
import * as NodeOS from "node:os";
const { homedir } = NodeOS;
import * as NodePath from "node:path";
const { dirname, join } = NodePath;
import * as NodeStreamWeb from "node:stream/web";
const { ReadableByteStreamController, ReadableStreamBYOBRequest } = NodeStreamWeb;
import * as NodeURL from "node:url";
const { pathToFileURL } = NodeURL;

const [moduleDirectory, mode = "default", temporaryRoot = join(homedir(), "codex-tmp")] =
  process.argv.slice(2);
const diskRequire = createRequire(join(moduleDirectory, "../package.json"));
const manifest = findPackageJSON(
  "@fails-components/webtransport",
  pathToFileURL(join(moduleDirectory, "../package.json")),
);
const { Http3Server, WebTransport, quicheLoaded } = diskRequire(
  join(dirname(manifest), "lib/index.node.js"),
);
await quicheLoaded;
const flowWindow = 32768;
const chunkSize = 65536;
const chunkCount = 256;
const tag = new Uint8Array([
  251, 252, 253, 254, 33, 44, 55, 66, 77, 88, 99, 111, 123, 135, 147, 159,
]);
const chunk = (index) => {
  const data = new Uint8Array(chunkSize).fill((index * 13) % 251);
  if (index === 0) data.set(tag);
  return data;
};
const expected = createHash("sha256");
for (let index = 0; index < chunkCount; index += 1) expected.update(chunk(index));
const expectedHash = expected.digest("hex");

// Observe the standard controller API without reaching into backend stream objects.
const metrics = new Map();
const requests = new WeakMap();
let bulkController;
const metric = (controller) => {
  if (!metrics.has(controller))
    metrics.set(controller, { enqueued: 0, backing: 0, byob: 0, enqueueCalls: 0 });
  return metrics.get(controller);
};
const identify = (controller, view) => {
  if (view.byteLength >= tag.byteLength && tag.every((value, index) => view[index] === value))
    bulkController = controller;
};
const enqueue = Object.getOwnPropertyDescriptor(ReadableByteStreamController.prototype, "enqueue");
const byobRequest = Object.getOwnPropertyDescriptor(
  ReadableByteStreamController.prototype,
  "byobRequest",
);
const respond = Object.getOwnPropertyDescriptor(ReadableStreamBYOBRequest.prototype, "respond");
Object.defineProperty(ReadableByteStreamController.prototype, "enqueue", {
  ...enqueue,
  value(view) {
    identify(this, view);
    const value = metric(this);
    value.enqueued += view.byteLength;
    value.backing += view.buffer.byteLength;
    value.enqueueCalls += 1;
    return Reflect.apply(enqueue.value, this, [view]);
  },
});
Object.defineProperty(ReadableByteStreamController.prototype, "byobRequest", {
  ...byobRequest,
  get() {
    const request = Reflect.apply(byobRequest.get, this, []);
    if (request) requests.set(request, this);
    return request;
  },
});
Object.defineProperty(ReadableStreamBYOBRequest.prototype, "respond", {
  ...respond,
  value(bytes) {
    const controller = requests.get(this);
    if (controller && bytes > 0) {
      identify(controller, this.view);
      metric(controller).byob += bytes;
    }
    return Reflect.apply(respond.value, this, [bytes]);
  },
});

const directory = await mkdtemp(join(temporaryRoot, "quic-reader-pressure-"));
let server, client, serverSession;
const observe = (promise) => {
  promise.catch(() => {});
  return promise;
};
const result = { mode, flowWindow, bytes: chunkSize * chunkCount, probes: 0 };
try {
  execFileSync(
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
      join(directory, "key.pem"),
      "-out",
      join(directory, "cert.pem"),
      "-days",
      "10",
      "-subj",
      "/CN=localhost",
      "-addext",
      "subjectAltName=DNS:localhost,IP:127.0.0.1",
    ],
    { stdio: "ignore" },
  );
  const cert = await readFile(join(directory, "cert.pem"), "utf8");
  server = new Http3Server({
    host: "127.0.0.1",
    port: 0,
    secret: randomUUID(),
    cert,
    privKey: await readFile(join(directory, "key.pem"), "utf8"),
    initialStreamFlowControlWindow: flowWindow,
    streamShouldAutoTuneReceiveWindow: false,
    streamFlowControlWindowSizeLimit: flowWindow,
    initialSessionFlowControlWindow: 1048576,
    sessionShouldAutoTuneReceiveWindow: false,
    sessionFlowControlWindowSizeLimit: 1048576,
    initialBidirectionalReceiveStreams: 72,
  });
  const sessions = server.sessionStream("/backpressure").getReader();
  server.startServer();
  await server.ready;
  client = new WebTransport(`https://127.0.0.1:${server.address().port}/backpressure`, {
    serverCertificateHashes: [
      {
        algorithm: "sha-256",
        value: createHash("sha256").update(new X509Certificate(cert).raw).digest(),
      },
    ],
    requireUnreliable: true,
  });
  observe(client.closed);
  await client.ready;
  serverSession = (await sessions.read()).value;
  observe(serverSession.closed);
  await serverSession.ready;
  const incoming = serverSession.incomingBidirectionalStreams.getReader();
  const nextPeer = async () => {
    const next = await incoming.read();
    NodeAssert.equal(next.done, false);
    return next.value;
  };
  const readWith = (reader, first = false) =>
    mode === "byob" ? reader.read(new Uint8Array(first ? 1024 : 65536)) : reader.read();
  const makeReader = (stream) => {
    const reader = stream.readable.getReader(mode === "byob" ? { mode: "byob" } : undefined);
    observe(reader.closed);
    return reader;
  };
  const probe = async (index) => {
    const payload = new TextEncoder().encode(`healthy-sibling-probe-${index}`);
    const local = await client.createBidirectionalStream();
    const writer = local.writable.getWriter();
    observe(writer.closed);
    const sending = observe(
      (async () => {
        await writer.write(payload);
        await writer.close();
      })(),
    );
    const localReader = local.readable.getReader();
    observe(localReader.closed);
    const receive = observe(
      (async () => {
        const parts = [];
        for (;;) {
          const next = await localReader.read();
          if (next.done) break;
          parts.push(next.value);
        }
        NodeAssert.deepEqual(Buffer.concat(parts), Buffer.from(payload));
      })(),
    );
    const remote = await nextPeer();
    const reader = remote.readable.getReader();
    const peerWriter = remote.writable.getWriter();
    observe(reader.closed);
    observe(peerWriter.closed);
    for (;;) {
      const next = await reader.read();
      if (next.done) break;
      await peerWriter.write(next.value);
    }
    await peerWriter.close();
    await Promise.all([sending, receive]);
    result.probes += 1;
  };

  const bulk = await client.createBidirectionalStream();
  const writer = bulk.writable.getWriter();
  const localReader = bulk.readable.getReader();
  observe(writer.closed);
  observe(localReader.closed);
  let writes = 0,
    transferComplete = false;
  const transfer = observe(
    (async () => {
      for (let index = 0; index < chunkCount; index += 1) {
        await writer.write(chunk(index));
        writes += 1;
      }
      await writer.close();
      transferComplete = true;
    })(),
  );
  const remote = await nextPeer();
  const reader = makeReader(remote);
  const remoteWriter = remote.writable.getWriter();
  observe(remoteWriter.closed);
  const digest = createHash("sha256");
  const first = await readWith(reader, true);
  NodeAssert.equal(first.done, false);
  NodeAssert.ok(first.value.byteLength > 0);
  let bytes = first.value.byteLength;
  digest.update(first.value);
  NodeAssert.ok(bulkController, "the public controller instrumentation must observe bulk bytes");
  const pausedBefore = { ...metric(bulkController), writes };
  for (let index = 0; index < 32; index += 1) await probe(index);
  const paused = { ...metric(bulkController), writes };
  console.log(JSON.stringify({ event: "paused", mode, ...paused, consumed: bytes }));
  NodeAssert.equal(
    paused.enqueued,
    pausedBefore.enqueued,
    "stopping public reads must stop native enqueues",
  );
  NodeAssert.equal(paused.byob, pausedBefore.byob);
  NodeAssert.equal(
    transferComplete,
    false,
    "native writes must remain backpressured while the public reader is paused",
  );
  NodeAssert.ok(writes < chunkCount);
  NodeAssert.ok(
    paused.backing <= 2 * flowWindow,
    `paused enqueue backing exceeded ${2 * flowWindow}: ${JSON.stringify(paused)}`,
  );
  NodeAssert.ok(
    paused.enqueued + paused.byob - bytes <= flowWindow,
    `paused JS queue exceeded the native receive window: ${JSON.stringify(paused)}`,
  );
  result.paused = {
    before: pausedBefore,
    after: paused,
    consumed: bytes,
    queuedBytes: paused.enqueued + paused.byob - bytes,
    backingUpperBound: paused.backing,
  };
  for (;;) {
    const next = await readWith(reader);
    if (next.done) break;
    bytes += next.value.byteLength;
    digest.update(next.value);
  }
  await Promise.all([transfer, reader.closed]);
  NodeAssert.equal(bytes, chunkCount * chunkSize);
  NodeAssert.equal(digest.digest("hex"), expectedHash);
  const peerFin = localReader.read();
  await remoteWriter.close();
  NodeAssert.equal((await peerFin).done, true);
  result.resumed = { bytes, sha256: expectedHash, writes, fin: true };
  await probe(32);

  const openMarked = async () => {
    const local = await client.createBidirectionalStream();
    const writer = local.writable.getWriter();
    observe(writer.closed);
    await writer.write(new Uint8Array([201]));
    const remote = await nextPeer();
    const reader = makeReader(remote);
    const peerWriter = remote.writable.getWriter();
    observe(peerWriter.closed);
    NodeAssert.equal((await readWith(reader, true)).value[0], 201);
    return { local, writer, reader, peerWriter };
  };
  const eof = await openMarked();
  const emptyRead = readWith(eof.reader);
  await eof.writer.close();
  const empty = await emptyRead;
  NodeAssert.equal(empty.done, true);
  await eof.reader.closed;
  const eofPeerReader = eof.local.readable.getReader();
  observe(eofPeerReader.closed);
  const eofPeerRead = eofPeerReader.read();
  await eof.peerWriter.close();
  NodeAssert.equal((await eofPeerRead).done, true);
  result.emptyFin = true;

  const blockedLocal = await client.createBidirectionalStream();
  const blockedWriter = blockedLocal.writable.getWriter();
  const blockedLocalReader = blockedLocal.readable.getReader();
  observe(blockedWriter.closed);
  observe(blockedLocalReader.closed);
  let blockedWrites = 0;
  const blockedTransfer = observe(
    (async () => {
      for (let index = 0; index < chunkCount; index += 1) {
        await blockedWriter.write(chunk(index));
        blockedWrites += 1;
      }
    })(),
  );
  const blockedRemote = await nextPeer();
  const blockedReader = makeReader(blockedRemote);
  const blockedPeerWriter = blockedRemote.writable.getWriter();
  observe(blockedPeerWriter.closed);
  await readWith(blockedReader, true);
  await probe(33);
  NodeAssert.ok(blockedWrites < chunkCount);
  await blockedReader.cancel({ code: 46 });
  await NodeAssert.rejects(blockedWriter.closed, /code:46/);
  await NodeAssert.rejects(blockedTransfer, /code:46/);
  await blockedPeerWriter.abort({ code: 47 });
  result.cancelWhileBackpressured = true;

  const cancelled = await openMarked();
  const cancelledRead = readWith(cancelled.reader);
  await cancelled.reader.cancel({ code: 42 });
  NodeAssert.equal((await cancelledRead).done, true);
  await NodeAssert.rejects(cancelled.writer.closed, /code:42/);
  await cancelled.peerWriter.abort({ code: 43 });
  result.cancel = true;

  const reset = await openMarked();
  const resetRead = observe(readWith(reset.reader));
  await reset.writer.abort({ code: 44 });
  await NodeAssert.rejects(resetRead, /code:44/);
  await reset.peerWriter.abort({ code: 45 });
  result.reset = true;

  const closed = await openMarked();
  const pendingRead = observe(readWith(closed.reader));
  client.close({ closeCode: 77, reason: "backpressure fixture complete" });
  await Promise.all([client.closed, serverSession.closed]);
  await NodeAssert.rejects(pendingRead);
  result.sessionClose = true;
  console.log(JSON.stringify(result));
} finally {
  client?.close();
  serverSession?.close();
  if (server) {
    server.stopServer();
    await server.closed;
  }
  Object.defineProperty(ReadableByteStreamController.prototype, "enqueue", enqueue);
  Object.defineProperty(ReadableByteStreamController.prototype, "byobRequest", byobRequest);
  Object.defineProperty(ReadableStreamBYOBRequest.prototype, "respond", respond);
  await rm(directory, { recursive: true, force: true });
}
