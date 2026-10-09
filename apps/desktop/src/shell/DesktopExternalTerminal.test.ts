import * as NodeOS from "node:os";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { EnvironmentId, EXTERNAL_TERMINALS } from "@t3tools/contracts";
import {
  HostProcessAddresses,
  HostProcessEnvironment,
  HostProcessPlatform,
} from "@t3tools/shared/hostProcess";
import { SpawnExecutableResolution } from "@t3tools/shared/shell";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { HttpClient, HttpClientResponse } from "effect/http";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { vi } from "vite-plus/test";

import * as DesktopExternalTerminal from "./DesktopExternalTerminal.ts";

const { lookup } = vi.hoisted(() => ({ lookup: vi.fn() }));
vi.mock("node:dns", () => ({ promises: { lookup } }));

const environmentId = EnvironmentId.make("local-service");
const localUrl = "http://100.101.102.103:3773";
const target = { environmentId, candidateUrls: [localUrl] };
const localAddresses = new Set(["127.0.0.1", "::1", "100.101.102.103", "192.168.1.5"]);

function harness(
  options: {
    readonly platform?: NodeJS.Platform;
    readonly env?: NodeJS.ProcessEnv;
    readonly descriptorId?: string;
    readonly malformedDescriptor?: boolean;
    readonly missingCommand?: boolean;
  } = {},
) {
  const requests: string[] = [];
  const spawned: ChildProcess.StandardCommand[] = [];
  let unrefs = 0;
  const http = Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make((request) =>
      Effect.sync(() => {
        requests.push(request.url);
        return HttpClientResponse.fromWeb(
          request,
          Response.json(
            options.malformedDescriptor
              ? {}
              : {
                  environmentId: options.descriptorId ?? environmentId,
                  label: "Local service",
                  platform: { os: "linux", arch: "x64" },
                  serverVersion: "0.0.47",
                  capabilities: {},
                },
          ),
        );
      }),
    ),
  );
  const spawner = Layer.succeed(
    ChildProcessSpawner.ChildProcessSpawner,
    ChildProcessSpawner.make((command) => {
      assert(ChildProcess.isStandardCommand(command));
      spawned.push(command);
      return Effect.succeed(
        ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(1),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
          isRunning: Effect.succeed(true),
          kill: () => Effect.void,
          unref: Effect.sync(() => {
            unrefs += 1;
            return Effect.void;
          }),
          stdin: Sink.drain,
          stdout: Stream.empty,
          stderr: Stream.empty,
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
        }),
      );
    }),
  );
  const layer = DesktopExternalTerminal.layer.pipe(
    Layer.provide(Layer.mergeAll(NodeServices.layer, http, spawner)),
    Layer.provideMerge(
      Layer.mergeAll(
        Layer.succeed(HostProcessAddresses, Effect.succeed(localAddresses)),
        Layer.succeed(HostProcessPlatform, options.platform ?? "linux"),
        Layer.succeed(
          HostProcessEnvironment,
          options.env ?? {
            PATH: "/gui/bin",
            WAYLAND_DISPLAY: "wayland-gui",
            XDG_RUNTIME_DIR: "/run/user/1000",
          },
        ),
        Layer.succeed(SpawnExecutableResolution, (command) =>
          options.missingCommand ? undefined : `/gui/bin/${command}`,
        ),
      ),
    ),
  );
  return { layer, requests, spawned, unrefs: () => unrefs };
}

describe("local environment verification", () => {
  it.effect("recognizes a local service through loopback, LAN, and tailnet addresses", () => {
    const h = harness();
    return Effect.gen(function* () {
      const terminal = yield* DesktopExternalTerminal.DesktopExternalTerminal;
      for (const url of [
        "http://127.0.0.1:3773",
        "http://[::1]:3773",
        "http://192.168.1.5:3773",
        localUrl,
      ]) {
        assert.isTrue(yield* terminal.isLocalEnvironment({ environmentId, candidateUrls: [url] }));
      }
      assert.lengthOf(h.requests, 4);
    }).pipe(Effect.provide(h.layer));
  });

  it.effect("recognizes a local service reached through its Tailscale hostname", () => {
    const h = harness();
    lookup.mockResolvedValueOnce([{ address: "100.101.102.103", family: 4 }]);
    return Effect.gen(function* () {
      const terminal = yield* DesktopExternalTerminal.DesktopExternalTerminal;
      assert.isTrue(
        yield* terminal.isLocalEnvironment({
          environmentId,
          candidateUrls: ["https://laptop.tail123.ts.net"],
        }),
      );
      assert.deepEqual(h.requests, ["https://laptop.tail123.ts.net/.well-known/t3/environment"]);
    }).pipe(Effect.provide(h.layer));
  });

  it.effect("verifies advertised local endpoints when the connection itself uses a relay", () => {
    const h = harness();
    lookup.mockResolvedValueOnce([{ address: "203.0.113.10", family: 4 }]);
    return Effect.gen(function* () {
      const terminal = yield* DesktopExternalTerminal.DesktopExternalTerminal;
      assert.isTrue(
        yield* terminal.isLocalEnvironment({
          environmentId,
          candidateUrls: ["https://relay.example.com", localUrl],
        }),
      );
      assert.deepEqual(h.requests, [`${localUrl}/.well-known/t3/environment`]);
    }).pipe(Effect.provide(h.layer));
  });

  it.effect("uses a verified local route without waiting for another route's DNS", () => {
    const h = harness();
    lookup.mockImplementationOnce(() => new Promise(() => undefined));
    return Effect.gen(function* () {
      const terminal = yield* DesktopExternalTerminal.DesktopExternalTerminal;
      assert.isTrue(
        yield* terminal.isLocalEnvironment({
          environmentId,
          candidateUrls: ["https://unavailable.example.com", localUrl],
        }),
      );
      assert.deepEqual(h.requests, [`${localUrl}/.well-known/t3/environment`]);
    }).pipe(Effect.provide(h.layer));
  });

  it.effect("does not trust a hostname that also resolves to another computer", () => {
    const h = harness();
    lookup.mockResolvedValueOnce([
      { address: "100.101.102.103", family: 4 },
      { address: "203.0.113.10", family: 4 },
    ]);
    return Effect.gen(function* () {
      const terminal = yield* DesktopExternalTerminal.DesktopExternalTerminal;
      assert.isFalse(
        yield* terminal.isLocalEnvironment({
          environmentId,
          candidateUrls: ["https://ambiguous.example.com"],
        }),
      );
      assert.isEmpty(h.requests);
    }).pipe(Effect.provide(h.layer));
  });

  it.effect(
    "does not contact or launch for another computer, even with a matching local path",
    () => {
      const h = harness();
      return Effect.gen(function* () {
        const terminal = yield* DesktopExternalTerminal.DesktopExternalTerminal;
        assert.isFalse(
          yield* terminal.launch({
            environmentId,
            candidateUrls: ["http://192.168.1.99:3773"],
            cwd: NodeOS.homedir(),
            terminal: "foot",
          }),
        );
        assert.isEmpty(h.requests);
        assert.isEmpty(h.spawned);
      }).pipe(Effect.provide(h.layer));
    },
  );

  it.effect("requires the matching environment id before inspecting the directory", () => {
    const h = harness({ descriptorId: "other-computer" });
    return Effect.gen(function* () {
      const terminal = yield* DesktopExternalTerminal.DesktopExternalTerminal;
      assert.isFalse(
        yield* terminal.launch({ ...target, cwd: "/does-not-exist", terminal: "foot" }),
      );
      assert.isEmpty(h.spawned);
    }).pipe(Effect.provide(h.layer));
  });

  it.effect("treats missing or malformed descriptors as unavailable", () => {
    const h = harness({ malformedDescriptor: true });
    return Effect.gen(function* () {
      const terminal = yield* DesktopExternalTerminal.DesktopExternalTerminal;
      assert.isFalse(yield* terminal.isLocalEnvironment(target));
      assert.isFalse(yield* terminal.isLocalEnvironment({ environmentId, candidateUrls: [] }));
      assert.isFalse(
        yield* terminal.isLocalEnvironment({
          environmentId,
          candidateUrls: ["not a URL", "file:///workspace"],
        }),
      );
      assert.isEmpty(h.spawned);
    }).pipe(Effect.provide(h.layer));
  });
});

describe("desktop terminal launching", () => {
  it.effect.each(EXTERNAL_TERMINALS)(
    "opens independent $id windows with the desktop's environment",
    (terminal) => {
      const h = harness();
      return Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const root = yield* fs.makeTempDirectoryScoped({
          directory: path.join(NodeOS.homedir(), "codex-tmp"),
          prefix: "t3-desktop-terminal-",
        });
        const cwd = path.join(root, "worktree with spaces; literal $name");
        yield* fs.makeDirectory(cwd);
        const launcher = yield* DesktopExternalTerminal.DesktopExternalTerminal;
        assert.isTrue(yield* launcher.launch({ ...target, cwd, terminal: terminal.id }));
        assert.isTrue(yield* launcher.launch({ ...target, cwd, terminal: terminal.id }));
        assert.lengthOf(h.spawned, 2);
        assert.equal(h.unrefs(), 2);
        for (const command of h.spawned) {
          assert.equal(command.command, `/gui/bin/${terminal.command}`);
          assert.deepEqual(command.args, [terminal.directoryFlag, cwd]);
          assert.equal(command.options.cwd, cwd);
          assert.equal(command.options.detached, true);
          assert.equal(command.options.extendEnv, false);
          assert.equal(command.options.env?.WAYLAND_DISPLAY, "wayland-gui");
          assert.notEqual(command.options.shell, true);
        }
      }).pipe(Effect.scoped, Effect.provide(Layer.merge(h.layer, NodeServices.layer)));
    },
  );

  it.effect("refuses foot without Wayland and on unsupported platforms", () => {
    const check = (h: ReturnType<typeof harness>, reason: string) =>
      Effect.gen(function* () {
        const terminal = yield* DesktopExternalTerminal.DesktopExternalTerminal;
        const error = yield* terminal
          .launch({ ...target, cwd: NodeOS.homedir(), terminal: "foot" })
          .pipe(Effect.flip);
        assert.equal(error.reason, reason);
        assert.isEmpty(h.spawned);
      }).pipe(Effect.provide(h.layer));
    return Effect.all([
      check(harness({ env: { DISPLAY: ":0" } }), "display-unavailable"),
      check(harness({ platform: "darwin" }), "unsupported-platform"),
      check(harness({ platform: "win32" }), "unsupported-platform"),
    ]);
  });

  it.effect("reports invalid directories and missing terminal executables", () => {
    return Effect.gen(function* () {
      const h = harness({ missingCommand: true });
      const path = yield* Path.Path;
      const root = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped({
        directory: path.join(NodeOS.homedir(), "codex-tmp"),
        prefix: "t3-desktop-terminal-",
      });
      yield* Effect.gen(function* () {
        const terminal = yield* DesktopExternalTerminal.DesktopExternalTerminal;
        const missing = yield* terminal
          .launch({ ...target, cwd: root, terminal: "foot" })
          .pipe(Effect.flip);
        assert.equal(missing.reason, "command-not-found");
        const invalid = yield* terminal
          .launch({ ...target, cwd: path.join(root, "missing"), terminal: "foot" })
          .pipe(Effect.flip);
        assert.equal(invalid.reason, "invalid-directory");
        assert.isEmpty(h.spawned);
      }).pipe(Effect.provide(h.layer));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));
  });
});
