// @effect-diagnostics nodeBuiltinImport:off - Test-only Node backend and certificate pin, never shipped as a platform connector.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import {
  RpcWebTransportError,
  wrapStandardWebTransportSession,
  type ReliableConnector,
  type ReliableSession,
} from "@t3tools/shared/rpcWebTransport";

let backend: Promise<typeof import("@fails-components/webtransport")> | undefined;
/** Node's package export condition differs between static imports and the verified disk loader. */
export function loadBenchmarkWebTransportBackend(platform: NodeJS.Platform, arch: string) {
  return (backend ??= (async () => {
    const origin = new URL("../../package.json", import.meta.url);
    const diskRequire = NodeModule.createRequire(origin);
    // Explicit native preflight happens before loading the high-level wrapper.
    const entry = diskRequire.resolve("@fails-components/webtransport-transport-http3-quiche");
    const root = NodePath.dirname(NodePath.dirname(entry));
    const platformBuild = NodePath.join(root, `build_${platform}_${arch}`);
    const build = NodeFS.existsSync(platformBuild) ? platformBuild : NodePath.join(root, "build");
    diskRequire(NodePath.join(build, "Release", "webtransport.node"));
    const manifest = NodeModule.findPackageJSON("@fails-components/webtransport", origin);
    if (manifest === undefined) throw new Error("Optional WebTransport backend is unavailable");
    const loaded = diskRequire(
      NodePath.join(NodePath.dirname(manifest), "lib/index.node.js"),
    ) as typeof import("@fails-components/webtransport");
    await loaded.quicheLoaded;
    return loaded;
  })());
}

export interface NativeConnectionEvidence {
  constructed: number;
  connectReady: number;
  controlStreamsOpened: number;
  requestStreamsOpened: number;
  closedByOwner: number;
  aborted: number;
  admissionPath: string;
  ticketPresent: boolean;
  reliability: string;
}

/** Inject the actual Node QUIC implementation through the production optional connector service. */
export async function makeBenchmarkConnector(
  certPem: string,
  platform: NodeJS.Platform,
  arch: string,
) {
  const { WebTransport } = await loadBenchmarkWebTransportBackend(platform, arch);
  const hash = new Uint8Array(
    NodeCrypto.createHash("sha256").update(new NodeCrypto.X509Certificate(certPem).raw).digest(),
  );
  const evidence: NativeConnectionEvidence = {
    constructed: 0,
    connectReady: 0,
    controlStreamsOpened: 0,
    requestStreamsOpened: 0,
    closedByOwner: 0,
    aborted: 0,
    admissionPath: "",
    ticketPresent: false,
    reliability: "unestablished",
  };
  const sessions = new Set<ReliableSession>();
  const connector: ReliableConnector = {
    connect: ({ url, signal }) =>
      new Promise((resolve, reject) => {
        if (signal.aborted) {
          reject(new RpcWebTransportError("aborted", "Benchmark connection aborted"));
          return;
        }
        const endpoint = new URL(url);
        evidence.admissionPath = endpoint.pathname;
        evidence.ticketPresent = endpoint.searchParams.has("wsTicket");
        evidence.constructed++;
        const options = {
          requireUnreliable: true,
          serverCertificateHashes: [{ algorithm: "sha-256", value: hash }],
          initialBidirectionalReceiveStreams: 80,
          initialBidirectionalSendStreams: 80,
        };
        const raw = new WebTransport(url, options);
        void raw.closed.catch(() => undefined);
        const underlying = wrapStandardWebTransportSession(raw);
        let opens = 0;
        const session: ReliableSession = {
          closed: underlying.closed,
          openBidirectionalStream: async (operationSignal) => {
            const stream = await underlying.openBidirectionalStream(operationSignal);
            if (opens++ === 0) evidence.controlStreamsOpened++;
            else evidence.requestStreamsOpened++;
            return stream;
          },
          close: (code) => {
            if (sessions.delete(session)) evidence.closedByOwner++;
            underlying.close(code);
          },
        };
        sessions.add(session);
        let settled = false;
        const abort = () => {
          session.close(0);
          if (settled) return;
          settled = true;
          evidence.aborted++;
          signal.removeEventListener("abort", abort);
          reject(new RpcWebTransportError("aborted", "Benchmark connection aborted"));
        };
        signal.addEventListener("abort", abort, { once: true });
        if (signal.aborted) {
          abort();
          return;
        }
        raw.ready.then(
          () => {
            if (settled) {
              session.close(0);
              return;
            }
            evidence.reliability = raw.reliability;
            if (raw.reliability !== "supports-unreliable") {
              settled = true;
              signal.removeEventListener("abort", abort);
              session.close(0);
              reject(new Error("Benchmark must negotiate QUIC over UDP"));
              return;
            }
            settled = true;
            signal.removeEventListener("abort", abort);
            evidence.connectReady++;
            resolve(session);
          },
          (cause: unknown) => {
            if (settled) return;
            settled = true;
            signal.removeEventListener("abort", abort);
            session.close(0);
            reject(new RpcWebTransportError("network", "Benchmark CONNECT failed", { cause }));
          },
        );
      }),
  };
  return {
    connector,
    evidence,
    dispose: () => {
      for (const session of sessions) session.close(0);
    },
  };
}
