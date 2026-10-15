import * as NodeChildProcess from "node:child_process";
const { execFileSync } = NodeChildProcess;
import * as NodeCrypto from "node:crypto";
const { createHash, randomUUID, X509Certificate } = NodeCrypto;
import * as NodeHttp from "node:http";
const { createServer } = NodeHttp;
import * as NodeFSP from "node:fs/promises";
const { mkdir, mkdtemp, readFile, rm, writeFile } = NodeFSP;
import * as NodeOS from "node:os";
const { homedir } = NodeOS;
import * as NodePath from "node:path";
const { join, resolve } = NodePath;
import * as NodeURL from "node:url";
const { pathToFileURL } = NodeURL;
import { Http3Server, quicheLoaded } from "@fails-components/webtransport";

/** Owns only a short-lived loopback UDP listener and a disposable ECDSA certificate. */
export async function startProtocolFixture() {
  const temporaryRoot = join(homedir(), "codex-tmp");
  await mkdir(temporaryRoot, { recursive: true });
  const directory = await mkdtemp(join(temporaryRoot, "quic-platform-"));
  let server;
  const sessions = new Set();
  const background = new Set();
  const run = (promise) => {
    background.add(promise);
    promise.catch(() => {}).finally(() => background.delete(promise));
  };
  const close = async () => {
    for (const session of sessions) session.close();
    if (server?.transportInt) server.stopServer();
    await Promise.allSettled(background);
    await rm(directory, { recursive: true, force: true });
  };
  try {
    const certPath = join(directory, "cert.pem");
    const keyPath = join(directory, "key.pem");
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
    const cert = await readFile(certPath, "utf8");
    await quicheLoaded;
    server = new Http3Server({
      host: "127.0.0.1",
      port: 0,
      secret: randomUUID(),
      cert,
      privKey: await readFile(keyPath, "utf8"),
      initialStreamFlowControlWindow: 1024 * 1024,
      initialSessionFlowControlWindow: 8 * 1024 * 1024,
    });
    const sessionReader = server.sessionStream("/probe").getReader();
    server.startServer();
    await server.ready;
    const echo = async (stream, session) => {
      const reader = stream.readable.getReader();
      const writer = stream.writable.getWriter();
      reader.closed.catch(() => {});
      writer.closed.catch(() => {});
      let command;
      let received = 0;
      try {
        for (;;) {
          const next = await reader.read();
          if (next.done) break;
          let bytes = next.value;
          if (command === undefined && bytes.byteLength > 0) {
            command = bytes[0];
            bytes = bytes.subarray(1);
            if (command === 1) {
              await Promise.allSettled([reader.cancel(), writer.abort()]);
              return;
            }
            if (command === 2) {
              sessions.delete(session);
              session.close({ closeCode: 23, reason: "fixture disconnect" });
              return;
            }
            if (command !== 0) throw new Error("Unknown fixture command");
          }
          received += bytes.byteLength;
          if (received > 8 * 1024 * 1024) throw new Error("Fixture stream limit exceeded");
          if (bytes.byteLength > 0) await writer.write(bytes);
        }
        await writer.close();
      } catch {
        await Promise.allSettled([reader.cancel(), writer.abort()]);
      } finally {
        reader.releaseLock();
        writer.releaseLock();
      }
    };
    run(
      (async () => {
        for (;;) {
          const next = await sessionReader.read();
          if (next.done) break;
          const session = next.value;
          sessions.add(session);
          session.closed.catch(() => {}).finally(() => sessions.delete(session));
          run(
            (async () => {
              await session.ready;
              const streams = session.incomingBidirectionalStreams.getReader();
              try {
                for (;;) {
                  const stream = await streams.read();
                  if (stream.done) break;
                  run(echo(stream.value, session));
                }
              } finally {
                streams.releaseLock();
              }
            })(),
          );
        }
        sessionReader.releaseLock();
      })(),
    );
    return {
      config: {
        url: `https://127.0.0.1:${server.address().port}/probe`,
        certificateHash: [...createHash("sha256").update(new X509Certificate(cert).raw).digest()],
      },
      close,
    };
  } catch (error) {
    await close();
    throw error;
  }
}

/** Serves a manual browser probe; does not launch a browser or access T3 state. */
export async function serveBrowserFixture(outputDirectory) {
  const fixture = await startProtocolFixture();
  await mkdir(outputDirectory, { recursive: true });
  const assets = new Map([
    ["/", ["browser.html", "text/html"]],
    ["/browserProbe.mjs", ["browserProbe.mjs", "text/javascript"]],
  ]);
  const http = createServer(async (request, response) => {
    try {
      response.setHeader("Cache-Control", "no-store");
      if (request.url === "/config.json") {
        response.setHeader("Content-Type", "application/json");
        response.end(JSON.stringify(fixture.config));
      } else if (request.url === "/result" && request.method === "POST") {
        const chunks = [];
        let bytes = 0;
        for await (const chunk of request) {
          bytes += chunk.byteLength;
          if (bytes > 512 * 1024) throw new Error("Result limit exceeded");
          chunks.push(chunk);
        }
        const report = JSON.parse(Buffer.concat(chunks).toString());
        const path = join(outputDirectory, `browser-${Date.now()}-${randomUUID()}.json`);
        await writeFile(path, `${JSON.stringify(report, null, 2)}\n`);
        console.log(JSON.stringify({ resultPath: path, ok: report.ok }));
        response.end("saved");
      } else if (assets.has(request.url)) {
        const [name, mime] = assets.get(request.url);
        response.setHeader("Content-Type", mime);
        response.setHeader(
          "Content-Security-Policy",
          "default-src 'self'; script-src 'self' 'unsafe-inline'; connect-src 'self' https:",
        );
        response.end(await readFile(new URL(name, import.meta.url)));
      } else {
        response.writeHead(404).end();
      }
    } catch (error) {
      response.writeHead(400).end(String(error));
    }
  });
  try {
    await new Promise((resolve, reject) => {
      http.once("error", reject);
      http.listen(0, "127.0.0.1", resolve);
    });
  } catch (error) {
    await fixture.close();
    throw error;
  }
  return {
    ...fixture,
    pageUrl: `http://localhost:${http.address().port}/`,
    close: async () => {
      http.closeAllConnections();
      await new Promise((resolve) => http.close(resolve));
      await fixture.close();
    },
  };
}

if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url) {
  const output = process.argv[2];
  if (!output) throw new Error("Usage: node fixture.mjs <evidence-directory>");
  const fixture = await serveBrowserFixture(resolve(output));
  console.log(JSON.stringify({ pid: process.pid, pageUrl: fixture.pageUrl, ...fixture.config }));
  let closing = false;
  const stop = async () => {
    if (closing) return;
    closing = true;
    await fixture.close();
  };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
}
