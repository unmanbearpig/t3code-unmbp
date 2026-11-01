import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import { webTransportInstallEnvironment } from "./cli-external-packages.ts";

/** pnpm's host-side build cache does not include our requested artifact target. */
export const stageWebTransportPrebuilt = Effect.fn("stageWebTransportPrebuilt")(function* (input: {
  readonly nodeModulesDir: string;
  readonly platform: "mac" | "linux" | "win";
  readonly arch: "arm64" | "x64" | "universal";
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const packageDir = path.join(
    input.nodeModulesDir,
    "@fails-components/webtransport-transport-http3-quiche",
  );
  if (!(yield* fs.exists(packageDir))) return;
  // Discard host-side cache payloads before requesting the actual artifact target.
  const removeBinaries = Effect.gen(function* () {
    for (const entry of yield* fs.readDirectory(packageDir)) {
      if (entry === "build" || entry.startsWith("build_")) {
        yield* fs.remove(path.join(packageDir, entry), { recursive: true, force: true });
      }
    }
  });
  yield* removeBinaries;
  const installer = path.join(packageDir, "install-prebuilt.js");
  const available = yield* Effect.gen(function* () {
    if (!(yield* fs.exists(installer))) return false;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const node = yield* HostProcess.ExecutablePath;
    const child = yield* spawner.spawn(
      ChildProcess.make(node, [installer], {
        cwd: packageDir,
        extendEnv: true,
        env: webTransportInstallEnvironment(input.platform, input.arch),
        stdout: "inherit",
        stderr: "inherit",
      }),
    );
    return Number(yield* child.exitCode) === 0;
  }).pipe(Effect.orElseSucceed(() => false));
  if (available) return;
  yield* removeBinaries;
  yield* Effect.logWarning(
    "Optional WebTransport prebuilt unavailable for artifact; packaging WebSocket support",
  );
});
