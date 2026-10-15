// @effect-diagnostics nodeBuiltinImport:off - Optional correctness probes observe the installed pinned addon without modifying dependency files.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import type { makeReconnectDiagnostics } from "./reconnectDiagnostics.ts";

/** Hash the resolved installed package and selected platform addon without loading or changing them. */
export function backendProvenance(platform: NodeJS.Platform, arch: string) {
  const origin = new URL("../../package.json", import.meta.url);
  const require = NodeModule.createRequire(origin);
  const manifest = NodeModule.findPackageJSON("@fails-components/webtransport", origin);
  if (!manifest) throw new Error("WebTransport diagnostic manifest unavailable");
  const root = NodePath.dirname(NodeFS.realpathSync(manifest));
  const addonEntry = require.resolve("@fails-components/webtransport-transport-http3-quiche");
  const addonRoot = NodePath.dirname(NodePath.dirname(addonEntry));
  const platformBuild = NodePath.join(addonRoot, `build_${platform}_${arch}`);
  const build = NodeFS.existsSync(platformBuild)
    ? platformBuild
    : NodePath.join(addonRoot, "build");
  const paths = [
    manifest,
    NodePath.join(root, "lib/index.node.js"),
    NodePath.join(root, "lib/stream.js"),
    NodePath.join(root, "lib/webstreams.js"),
    addonEntry,
    NodePath.join(addonRoot, "lib/clientsocket.js"),
    NodePath.join(addonRoot, "lib/serversocket.js"),
    NodePath.join(build, "Release/webtransport.node"),
  ].map((file) => NodeFS.realpathSync(file));
  return {
    root,
    files: paths.map((file) => ({
      path: file,
      sha256: NodeCrypto.createHash("sha256").update(NodeFS.readFileSync(file)).digest("hex"),
    })),
  };
}

/** Instrument the actual cached backend class; preserve callback return values and native promises. */
export async function installNativeDiagnostics(
  trace: ReturnType<typeof makeReconnectDiagnostics>,
  platform: NodeJS.Platform,
  arch: string,
) {
  const { root, files } = backendProvenance(platform, arch);
  trace.event("app-backend-provenance", { files });
  const loaded: unknown = await import(
    NodeURL.pathToFileURL(NodePath.join(root, "lib/stream.js")).href
  );
  if (typeof loaded !== "object" || loaded === null)
    throw new Error("Invalid backend diagnostic module");
  const constructor: unknown = Reflect.get(loaded, "HttpWTStream");
  if (typeof constructor !== "function") throw new Error("Missing backend stream constructor");
  const prototype: object = Reflect.get(constructor, "prototype");
  const observedNative = new WeakSet<object>();
  const observeNative = (owner: object) => {
    const native: unknown = Reflect.get(owner, "objint");
    if (typeof native !== "object" || native === null || observedNative.has(native)) return;
    observedNative.add(native);
    for (const method of [
      "startReading",
      "stopReading",
      "drainReads",
      "stopSending",
      "resetStream",
      "streamFinal",
      "writeChunk",
    ]) {
      const original: unknown = Reflect.get(native, method, native);
      if (typeof original !== "function") continue;
      const observer = (...args: readonly unknown[]) => {
        trace.native(owner, method, args[0]);
        return Reflect.apply(original, native, args);
      };
      const installed =
        Reflect.set(native, method, observer) && Reflect.get(native, method) === observer;
      trace.event("native-hook-installed", { method, installed });
      if (!installed) throw new Error(`Cannot install native diagnostic method ${method}`);
    }
  };
  for (const method of [
    "getReadBuffer",
    "commitReadBuffer",
    "onStreamWrite",
    "onStreamRecvSignal",
    "onStreamNetworkFinish",
    "finalDrain",
  ]) {
    const original: unknown = Reflect.get(prototype, method);
    if (typeof original !== "function")
      throw new Error(`Missing diagnostic backend callback ${method}`);
    const observer = function (this: object, ...args: readonly unknown[]) {
      observeNative(this);
      trace.native(this, method, args[0]);
      const result: unknown = Reflect.apply(original, this, args);
      if (method === "getReadBuffer" && typeof result === "object" && result !== null) {
        const buffer: unknown = Reflect.get(result, "buffer");
        trace.native(
          this,
          method,
          {
            bufferBytes: buffer instanceof Uint8Array ? buffer.byteLength : 0,
            byob: Reflect.get(result, "byob") != null,
          },
          "return",
        );
      } else trace.native(this, method, undefined, "return");
      return result;
    };
    const installed =
      Reflect.set(prototype, method, observer) && Reflect.get(prototype, method) === observer;
    trace.event("app-backend-hook-installed", { method, installed });
    if (!installed) throw new Error(`Cannot install backend diagnostic callback ${method}`);
  }
}
