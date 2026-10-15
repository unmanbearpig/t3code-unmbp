// @effect-diagnostics nodeBuiltinImport:off - Native HTTP adapter tests own their TLS sockets and temporary certificates.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeHttps from "node:https";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeZlib from "node:zlib";
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";

import { makeSnapshotHttpClient } from "./http3.ts";
import { Recorder } from "./workloads.ts";

const tlsFixture = Effect.gen(function* () {
  const root = NodePath.join(NodeOS.homedir(), "codex-tmp");
  NodeFS.mkdirSync(root, { recursive: true });
  const directory = yield* Effect.acquireRelease(
    Effect.sync(() => NodeFS.mkdtempSync(NodePath.join(root, "netbench-http-test-"))),
    (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
  );
  const certPath = NodePath.join(directory, "cert.pem");
  const keyPath = NodePath.join(directory, "key.pem");
  yield* Effect.sync(() =>
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
    ),
  );
  const certPem = NodeFS.readFileSync(certPath, "utf8");
  return {
    key: NodeFS.readFileSync(keyPath, "utf8"),
    certPem,
    certSha256: new Uint8Array(
      NodeCrypto.createHash("sha256").update(new NodeCrypto.X509Certificate(certPem).raw).digest(),
    ),
  };
});

const serverFixture = Effect.fnUntraced(function* (
  tls: Effect.Success<typeof tlsFixture>,
  handler: Parameters<typeof NodeHttps.createServer>[1],
) {
  const server = yield* Effect.acquireRelease(
    Effect.sync(() => NodeHttps.createServer({ cert: tls.certPem, key: tls.key }, handler)),
    (server) =>
      Effect.promise(
        () =>
          new Promise<void>((resolve) => {
            server.closeAllConnections();
            server.close(() => resolve());
          }),
      ),
  );
  const port = yield* Effect.callback<number>((resume) => {
    server.once("error", (cause) => resume(Effect.die(cause)));
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        resume(Effect.die(new Error("Expected a TCP test server")));
        return;
      }
      resume(Effect.succeed(address.port));
    });
  });
  return { server, url: "https://127.0.0.1:" + port + "/snapshot" };
});

for (const encoding of ["gzip", "br"] as const)
  it.effect.skipIf(process.env.NETBENCH_INTEGRATION !== "1")(
    `decodes ${encoding}, counts compressed bytes, and reuses only its own connection`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const tls = yield* tlsFixture;
          const body = { text: "Message with Unicode 🌍", messages: 20 };
          const compressed =
            encoding === "gzip"
              ? NodeZlib.gzipSync(JSON.stringify(body))
              : NodeZlib.brotliCompressSync(JSON.stringify(body));
          const { url } = yield* serverFixture(tls, (request, response) => {
            assert.equal(request.headers["accept-encoding"], encoding);
            response.setHeader("Content-Encoding", encoding);
            response.end(compressed);
          });
          const record = new Recorder();
          const client = yield* makeSnapshotHttpClient("h1", tls, record, encoding);
          assert.deepEqual(yield* (yield* client.get(url)).json, body);
          assert.equal(record.metrics().httpEncoding, encoding);
          assert.equal(record.metrics().httpCompressedBytes, compressed.length);
          assert.equal(record.metrics().httpNewConnections, 1);
          const connection = record.metrics().httpConnectionId;
          assert.deepEqual(yield* (yield* client.get(url)).json, body);
          assert.equal(record.metrics().httpNewConnections, 0);
          assert.equal(record.metrics().httpConnectionId, connection);
          const isolatedRecord = new Recorder();
          const isolated = yield* makeSnapshotHttpClient("h1", tls, isolatedRecord, encoding);
          assert.deepEqual(yield* (yield* isolated.get(url)).json, body);
          assert.equal(isolatedRecord.metrics().httpNewConnections, 1);
        }),
      ),
  );

it.effect.skipIf(process.env.NETBENCH_INTEGRATION !== "1")(
  "interrupts an in-flight native request without late unhandled errors",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const tls = yield* tlsFixture;
        const { server, url } = yield* serverFixture(tls, () => {});
        const client = yield* makeSnapshotHttpClient("h1", tls, new Recorder());
        const incoming = yield* Effect.callback<void>((resume) => {
          const arrived = () => resume(Effect.void);
          server.once("request", arrived);
          return Effect.sync(() => server.off("request", arrived));
        }).pipe(Effect.forkChild);
        const request = yield* client.get(url).pipe(Effect.forkChild);
        yield* Fiber.join(incoming);
        yield* Fiber.interrupt(request);
        const exit = yield* Fiber.await(request);
        assert.isTrue(Exit.isFailure(exit));
        if (Exit.isFailure(exit)) assert.isTrue(Cause.hasInterrupts(exit.cause));
      }),
    ),
);
