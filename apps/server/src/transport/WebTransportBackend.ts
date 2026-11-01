// @effect-diagnostics nodeBuiltinImport:off -- Native loading must resolve from the packaged disk tree.
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { WebTransportUnavailableError } from "./WebTransportConfig.ts";

type Backend = Pick<typeof import("@fails-components/webtransport"), "Http3Server">;

export class WebTransportBackend extends Context.Service<
  WebTransportBackend,
  { readonly load: Effect.Effect<Backend, WebTransportUnavailableError> }
>()("t3/transport/WebTransportBackend") {}

const make = Effect.gen(function* () {
  const executable = yield* HostProcess.IsExecutable;
  const executablePath = yield* HostProcess.ExecutablePath;
  const platform = yield* HostProcess.Platform;
  const arch = yield* HostProcess.Architecture;
  const load = Effect.gen(function* () {
    const wrapper = yield* Effect.try({
      try: () => {
        const base = executable
          ? NodePath.join(NodePath.dirname(executablePath), "package.json")
          : NodeURL.fileURLToPath(import.meta.url);
        const diskRequire = NodeModule.createRequire(base);
        const entry = diskRequire.resolve("@fails-components/webtransport-transport-http3-quiche");
        const root = NodePath.dirname(NodePath.dirname(entry));
        const platformBuild = NodePath.join(root, `build_${platform}_${arch}`);
        const build = NodeFS.existsSync(platformBuild)
          ? platformBuild
          : NodePath.join(root, "build");
        // Preflight the exact file the upstream loader selects before requiring
        // its wrapper, which starts an eager addon import and logs its failure.
        diskRequire(NodePath.join(build, "Release", "webtransport.node"));
        const manifest = NodeModule.findPackageJSON(
          "@fails-components/webtransport",
          NodeURL.pathToFileURL(base),
        );
        if (!manifest) throw new WebTransportUnavailableError({ stage: "backend" });
        return diskRequire(
          NodePath.join(NodePath.dirname(manifest), "lib", "index.node.js"),
        ) as typeof import("@fails-components/webtransport");
      },
      catch: (cause) => new WebTransportUnavailableError({ stage: "backend", cause }),
    });
    yield* Effect.tryPromise({
      try: () => wrapper.quicheLoaded,
      catch: (cause) => new WebTransportUnavailableError({ stage: "backend", cause }),
    });
    return { Http3Server: wrapper.Http3Server };
  }).pipe(
    Effect.timeoutOrElse({
      duration: "5 seconds",
      orElse: () => Effect.fail(new WebTransportUnavailableError({ stage: "backend" })),
    }),
  );
  return WebTransportBackend.of({ load });
});

export const layer = Layer.effect(WebTransportBackend, make);
