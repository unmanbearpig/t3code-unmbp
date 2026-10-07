import {
  EXTERNAL_TERMINALS,
  ExternalTerminalLaunchError,
  type DesktopTerminalLaunchInput,
  type DesktopTerminalTarget,
} from "@t3tools/contracts";
import { fetchRemoteEnvironmentDescriptor } from "@t3tools/client-runtime/environment";
import {
  HostProcessAddresses,
  HostProcessEnvironment,
  HostProcessPlatform,
} from "@t3tools/shared/hostProcess";
import { SpawnExecutableResolution } from "@t3tools/shared/shell";
import * as NodeDns from "node:dns";
import * as NodeNet from "node:net";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import { HttpClient } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

export class DesktopExternalTerminal extends Context.Service<
  DesktopExternalTerminal,
  {
    readonly isLocalEnvironment: (target: DesktopTerminalTarget) => Effect.Effect<boolean>;
    readonly launch: (
      input: DesktopTerminalLaunchInput,
    ) => Effect.Effect<boolean, ExternalTerminalLaunchError>;
  }
>()("@t3tools/desktop/shell/DesktopExternalTerminal") {}

const normalizeAddress = (address: string) => address.replace(/^::ffff:/, "");

/** @public Service construction is part of the canonical Effect module API. */
export const make = Effect.gen(function* () {
  const httpClient = yield* HttpClient.HttpClient;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

  // A local address alone can belong to a different server after a port or
  // network change. Confirm the environment id before using its workspace path.
  const isLocalEnvironment = Effect.fn("desktop.externalTerminal.isLocalEnvironment")(
    function* (target: DesktopTerminalTarget) {
      if (target.candidateUrls.length === 0) return false;
      const readAddresses = yield* HostProcessAddresses;
      const addresses = new Set([...(yield* readAddresses)].map(normalizeAddress));
      return yield* Effect.raceAll(
        [...new Set(target.candidateUrls)].map((candidateUrl) =>
          Effect.gen(function* () {
            const url = yield* Effect.try(() => new URL(candidateUrl));
            if (!["http:", "https:"].includes(url.protocol) || url.username || url.password) {
              return false;
            }
            const hostname = url.hostname.replace(/^\[|\]$/g, "");
            const resolved = NodeNet.isIP(hostname)
              ? [hostname]
              : yield* Effect.tryPromise(() =>
                  NodeDns.promises.lookup(hostname, { all: true }),
                ).pipe(Effect.map((entries) => entries.map((entry) => entry.address)));
            if (
              resolved.length === 0 ||
              !resolved.every((address) => addresses.has(normalizeAddress(address)))
            )
              return false;
            const descriptor = yield* fetchRemoteEnvironmentDescriptor({
              httpBaseUrl: url.origin,
              timeoutMs: 1_000,
            }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient));
            return descriptor.environmentId === target.environmentId;
          }).pipe(
            Effect.orElseSucceed(() => false),
            Effect.flatMap((local) => (local ? Effect.succeed(true) : Effect.fail(false))),
          ),
        ),
      ).pipe(Effect.orElseSucceed(() => false));
    },
    (effect) =>
      effect.pipe(Effect.timeoutOption("2 seconds"), Effect.map(Option.getOrElse(() => false))),
  );

  const launch = Effect.fn("desktop.externalTerminal.launch")(function* (
    input: DesktopTerminalLaunchInput,
  ) {
    if (!(yield* isLocalEnvironment(input))) return false;
    const platform = yield* HostProcessPlatform;
    if (platform !== "linux" && (platform !== "darwin" || input.terminal === "foot")) {
      return yield* new ExternalTerminalLaunchError({ ...input, reason: "unsupported-platform" });
    }
    const env = yield* HostProcessEnvironment;
    if (
      platform === "linux" &&
      (input.terminal === "foot"
        ? !env.WAYLAND_DISPLAY?.trim()
        : !env.WAYLAND_DISPLAY?.trim() && !env.DISPLAY?.trim())
    ) {
      return yield* new ExternalTerminalLaunchError({ ...input, reason: "display-unavailable" });
    }
    const info = yield* fileSystem
      .stat(input.cwd)
      .pipe(
        Effect.mapError(
          (cause) =>
            new ExternalTerminalLaunchError({ ...input, reason: "invalid-directory", cause }),
        ),
      );
    if (!path.isAbsolute(input.cwd) || info.type !== "Directory") {
      return yield* new ExternalTerminalLaunchError({ ...input, reason: "invalid-directory" });
    }
    const terminal = EXTERNAL_TERMINALS.find((candidate) => candidate.id === input.terminal);
    if (terminal === undefined) {
      return yield* new ExternalTerminalLaunchError({ ...input, reason: "unsupported-platform" });
    }
    const resolveExecutable = yield* SpawnExecutableResolution;
    const executable = resolveExecutable(terminal.command, platform, env);
    if (executable === undefined) {
      return yield* new ExternalTerminalLaunchError({ ...input, reason: "command-not-found" });
    }
    yield* spawner
      .spawn(
        ChildProcess.make(executable, [terminal.directoryFlag, input.cwd], {
          cwd: input.cwd,
          env,
          extendEnv: false,
          detached: true,
          stdin: "ignore",
          stdout: "ignore",
          stderr: "ignore",
        }),
      )
      .pipe(
        Effect.flatMap((handle) => handle.unref),
        Effect.asVoid,
        Effect.scoped,
        Effect.mapError(
          (cause) => new ExternalTerminalLaunchError({ ...input, reason: "spawn-failed", cause }),
        ),
      );
    return true;
  });

  return DesktopExternalTerminal.of({ isLocalEnvironment, launch });
});

export const layer = Layer.effect(DesktopExternalTerminal, make);
