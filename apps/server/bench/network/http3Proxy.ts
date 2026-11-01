// @effect-diagnostics nodeBuiltinImport:off globalTimers:off - Owns the pinned benchmark proxy and its startup deadline outside measured operations.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import { FetchHttpClient, HttpClient } from "effect/http";

import { snapshotPort } from "./http3.ts";

export const http3ProxySettings = {
  version: "2.11.7",
  sha256: "727b91701a392de6ebc5027509f548bf39979e5216340d0faed8fa5e69c84f8b",
  protocols: ["h1", "h3"],
  earlyData: false,
  upstream: "unix-http1-production-gzip",
} as const;

/** Downloads the fixed proxy before entering the network namespace; later runs reuse it. */
export async function ensureHttp3Proxy() {
  const override = process.env.NETBENCH_CADDY;
  const directory = NodePath.join(
    NodeOS.homedir(),
    "codex-tmp",
    "network-bench-tools",
    "caddy-" + http3ProxySettings.version,
  );
  const executable = override ?? NodePath.join(directory, "caddy");
  if (!NodeFS.existsSync(executable)) {
    if (override) throw new Error("NETBENCH_CADDY does not exist");
    if (
      Effect.runSync(HostProcess.Platform) !== "linux" ||
      Effect.runSync(HostProcess.Architecture) !== "x64"
    )
      throw new Error(
        "Set NETBENCH_CADDY to Caddy " + http3ProxySettings.version + " on this architecture",
      );
    NodeFS.mkdirSync(directory, { recursive: true });
    const archiveBytes = await Effect.runPromise(
      HttpClient.get(
        "https://github.com/caddyserver/caddy/releases/download/v" +
          http3ProxySettings.version +
          "/caddy_" +
          http3ProxySettings.version +
          "_linux_amd64.tar.gz",
      ).pipe(
        Effect.flatMap((response) =>
          response.status === 200
            ? response.arrayBuffer
            : Effect.die(new Error("Cannot download the benchmark's pinned Caddy")),
        ),
        Effect.provide(FetchHttpClient.layer),
        Effect.scoped,
      ),
    );
    const archive = Buffer.from(archiveBytes);
    if (NodeCrypto.createHash("sha256").update(archive).digest("hex") !== http3ProxySettings.sha256)
      throw new Error("Caddy download checksum differs");
    const archivePath = NodePath.join(directory, "caddy.tar.gz");
    NodeFS.writeFileSync(archivePath, archive);
    try {
      NodeChildProcess.execFileSync("tar", ["-xzf", archivePath, "-C", directory, "caddy"]);
    } finally {
      NodeFS.rmSync(archivePath, { force: true });
    }
  }
  const version = NodeChildProcess.execFileSync(executable, ["version"], {
    encoding: "utf8",
  }).trim();
  if (!version.startsWith("v" + http3ProxySettings.version + " "))
    throw new Error("Use the suite's pinned Caddy " + http3ProxySettings.version);
  return { executable, version };
}

/** Both snapshot protocols reach the same production route without a counted TCP proxy hop. */
export function http3ProxyConfig(
  certPath: string,
  keyPath: string,
  socketPath: string,
  storagePath: string,
) {
  return {
    admin: { disabled: true },
    storage: { module: "file_system", root: storagePath },
    apps: {
      tls: {
        certificates: { load_files: [{ certificate: certPath, key: keyPath, tags: ["fixture"] }] },
      },
      http: {
        servers: {
          snapshots: {
            listen: ["127.0.0.1:" + snapshotPort],
            protocols: http3ProxySettings.protocols,
            allow_0rtt: false,
            automatic_https: { disable: true },
            tls_connection_policies: [{ certificate_selection: { any_tag: ["fixture"] } }],
            routes: [
              {
                handle: [
                  {
                    handler: "headers",
                    response: { set: { "X-Netbench-Protocol": ["{http.request.proto}"] } },
                  },
                  {
                    handler: "reverse_proxy",
                    upstreams: [{ dial: "unix/" + socketPath }],
                    transport: { protocol: "http", versions: ["1.1"], compression: false },
                  },
                ],
              },
            ],
          },
        },
      },
    },
  };
}

/** Resolves on Caddy's startup milestone instead of a fixed readiness sleep. */
export function waitForHttp3Proxy(child: NodeChildProcess.ChildProcess) {
  return new Promise<void>((resolve, reject) => {
    let output = "";
    const onData = (chunk: Buffer) => {
      output += chunk.toString();
      const lines = output.split("\n");
      output = lines.pop() ?? "";
      for (const line of lines) {
        if (line.includes('"msg":"serving initial configuration"')) {
          cleanup();
          resolve();
          return;
        }
      }
    };
    const onError = (cause: Error) => {
      cleanup();
      reject(cause);
    };
    const onExit = (code: number | null) => {
      cleanup();
      reject(new Error("HTTP/3 proxy exited with " + code + ": " + output));
    };
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error("HTTP/3 proxy readiness timed out: " + output));
    }, 15000);
    const cleanup = () => {
      clearTimeout(timer);
      child.stderr?.off("data", onData);
      child.off("error", onError);
      child.off("exit", onExit);
    };
    child.stderr?.on("data", onData);
    child.once("error", onError);
    child.once("exit", onExit);
  });
}
