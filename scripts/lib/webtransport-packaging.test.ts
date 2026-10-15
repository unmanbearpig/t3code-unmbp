// @effect-diagnostics nodeBuiltinImport:off - exercises the published Node installer and disk loader.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import { afterEach, assert, describe, it } from "@effect/vitest";

import {
  createStagePatchedDependencies,
  createStageWorkspaceConfig,
} from "../build-desktop-artifact.ts";
import { stageWebTransportPrebuilt } from "./stage-webtransport.ts";
import {
  selectCliRuntimeExternalDependencies,
  webTransportInstallEnvironment,
} from "./cli-external-packages.ts";
import serverPackageJson from "../../apps/server/package.json" with { type: "json" };

const repoRoot = NodeURL.fileURLToPath(new URL("../../", import.meta.url));
const patchName = "@fails-components/webtransport-transport-http3-quiche@1.6.9";
const patchPath = "patches/@fails-components__webtransport-transport-http3-quiche@1.6.9.patch";
const wrapperPatchName = "@fails-components/webtransport@1.6.9";
const wrapperPatchPath = "patches/@fails-components__webtransport@1.6.9.patch";
const patch = NodeFS.readFileSync(NodePath.join(repoRoot, patchPath), "utf8");
const installer = patch
  .slice(patch.indexOf("+++ b/install-prebuilt.js\n"))
  .split("\n")
  .filter((line) => line.startsWith("+") && !line.startsWith("+++"))
  .map((line) => line.slice(1))
  .join("\n");
const scratch: string[] = [];
const fixture = () => {
  const directory = NodeFS.mkdtempSync(
    NodePath.join(NodeOS.homedir(), "codex-tmp", "t3-quic-install-test-"),
  );
  scratch.push(directory);
  NodeFS.writeFileSync(NodePath.join(directory, "package.json"), '{"type":"module"}');
  NodeFS.writeFileSync(NodePath.join(directory, "install-prebuilt.js"), installer);
  return directory;
};
afterEach(() => {
  for (const directory of scratch.splice(0))
    NodeFS.rmSync(directory, { recursive: true, force: true });
});

const runInstaller = (directory: string, env: Record<string, string>) =>
  NodeChildProcess.spawnSync(process.execPath, ["install-prebuilt.js"], {
    cwd: directory,
    env: {
      ...process.env,
      T3CODE_WEBTRANSPORT_PLATFORM: "",
      T3CODE_WEBTRANSPORT_ARCH: "",
      T3CODE_WEBTRANSPORT_LIBC: "",
      LIBC: "",
      ...env,
    },
    encoding: "utf8",
    timeout: 10_000,
  });
const fakeDownloader = (directory: string) => {
  const moduleDir = NodePath.join(directory, "node_modules/prebuild-install");
  NodeFS.mkdirSync(moduleDir, { recursive: true });
  NodeFS.writeFileSync(
    NodePath.join(moduleDir, "bin.js"),
    `
const fs = require('node:fs');
fs.appendFileSync('download-args.jsonl', JSON.stringify(process.argv.slice(2)) + '\\n');
if (process.env.TEST_DOWNLOAD_FAILURE || fs.existsSync('fail-download')) process.exit(37);
if (!process.env.TEST_EMPTY_DOWNLOAD) {
  fs.mkdirSync('build/Release', {recursive: true});
  fs.writeFileSync('build/Release/webtransport.node', process.argv[process.argv.indexOf('--arch') + 1]);
}
`,
  );
};
const writePayload = (directory: string, buildDirectory: string, contents: string) => {
  const file = NodePath.join(directory, buildDirectory, "Release/webtransport.node");
  NodeFS.mkdirSync(NodePath.dirname(file), { recursive: true });
  NodeFS.writeFileSync(file, contents);
  return file;
};

describe("optional WebTransport installer", () => {
  it("replaces only normal install and preserves explicit upstream builds", () => {
    assert.include(patch, '+    "install": "node install-prebuilt.js"');
    assert.notInclude(installer, "cmake");
    assert.notInclude(installer, "build.js");
  });

  it.each([
    { npm_config_platform: "win32", npm_config_arch: "arm64", npm_config_libc: "glibc" },
    { npm_config_platform: "linux", npm_config_arch: "x64", npm_config_libc: "musl" },
    { npm_config_platform: "freebsd", npm_config_arch: "x64", npm_config_libc: "glibc" },
  ])("skips unsupported requested target %j without a downloader", (env) => {
    const directory = fixture();
    const result = runInstaller(directory, env);
    assert.equal(result.status, 0, result.stderr);
    assert.include(result.stdout, "unsupported target");
    assert.isFalse(NodeFS.existsSync(NodePath.join(directory, "build")));
  });

  it.each(["missing", "failed"])(
    "preserves an existing target payload with a %s downloader",
    (mode) => {
      const directory = fixture();
      const payload = writePayload(directory, "build_linux_x64", "bundled-native-payload");
      if (mode === "failed") fakeDownloader(directory);
      const result = runInstaller(directory, {
        ...webTransportInstallEnvironment("linux", "x64"),
        TEST_DOWNLOAD_FAILURE: "1",
      });
      assert.equal(result.status, 0, result.stderr);
      assert.equal(NodeFS.readFileSync(payload, "utf8"), "bundled-native-payload");
      assert.isFalse(NodeFS.existsSync(NodePath.join(directory, "download-args.jsonl")));
    },
  );

  it.each(["build_linux_arm64", "build"])(
    "does not reuse a payload from %s for another requested target",
    (buildDirectory) => {
      const directory = fixture();
      const previousPayload = writePayload(directory, buildDirectory, "other-native-payload");
      fakeDownloader(directory);
      const result = runInstaller(directory, webTransportInstallEnvironment("linux", "x64"));
      assert.equal(result.status, 0, result.stderr);
      assert.equal(
        NodeFS.readFileSync(
          NodePath.join(directory, "build_linux_x64/Release/webtransport.node"),
          "utf8",
        ),
        "x64",
      );
      if (buildDirectory !== "build") {
        assert.equal(NodeFS.readFileSync(previousPayload, "utf8"), "other-native-payload");
      }
      assert.isTrue(NodeFS.existsSync(NodePath.join(directory, "download-args.jsonl")));
    },
  );

  it.each([
    { npm_config_platform: "win32", npm_config_arch: "x64", npm_config_libc: "glibc" },
    { npm_config_platform: "linux", npm_config_arch: "arm64", npm_config_libc: "glibc" },
    { npm_config_platform: "darwin", npm_config_arch: "x64", npm_config_libc: "glibc" },
  ])("downloads the requested target and retains its native loader path %j", (env) => {
    const directory = fixture();
    fakeDownloader(directory);
    const result = runInstaller(directory, env);
    assert.equal(result.status, 0, result.stderr);
    const args = JSON.parse(
      NodeFS.readFileSync(NodePath.join(directory, "download-args.jsonl"), "utf8"),
    );
    assert.deepStrictEqual(args, [
      "--runtime",
      "napi",
      "--target",
      "6",
      "--download",
      "--force",
      "--platform",
      env.npm_config_platform,
      "--arch",
      env.npm_config_arch,
      "--libc",
      "glibc",
    ]);
    assert.equal(
      NodeFS.readFileSync(
        NodePath.join(
          directory,
          `build_${env.npm_config_platform}_${env.npm_config_arch}/Release/webtransport.node`,
        ),
        "utf8",
      ),
      env.npm_config_arch,
    );
  });

  it("stages both macOS universal binaries separately", () => {
    const directory = fixture();
    fakeDownloader(directory);
    const result = runInstaller(directory, webTransportInstallEnvironment("mac", "universal"));
    assert.equal(result.status, 0, result.stderr);
    for (const arch of ["arm64", "x64"]) {
      assert.equal(
        NodeFS.readFileSync(
          NodePath.join(directory, `build_darwin_${arch}/Release/webtransport.node`),
          "utf8",
        ),
        arch,
      );
    }
  });

  it("preserves both macOS universal payloads without a downloader", () => {
    const directory = fixture();
    const arm64 = writePayload(directory, "build_darwin_arm64", "bundled-arm64");
    const x64 = writePayload(directory, "build_darwin_x64", "bundled-x64");
    const result = runInstaller(directory, webTransportInstallEnvironment("mac", "universal"));
    assert.equal(result.status, 0, result.stderr);
    assert.equal(NodeFS.readFileSync(arm64, "utf8"), "bundled-arm64");
    assert.equal(NodeFS.readFileSync(x64, "utf8"), "bundled-x64");
    assert.isFalse(NodeFS.existsSync(NodePath.join(directory, "download-args.jsonl")));
  });

  it.each(["arm64", "x64"])(
    "downloads only the missing macOS universal architecture when %s is bundled",
    (existingArch) => {
      const directory = fixture();
      const existingPayload = writePayload(
        directory,
        `build_darwin_${existingArch}`,
        "bundled-payload",
      );
      fakeDownloader(directory);
      const result = runInstaller(directory, webTransportInstallEnvironment("mac", "universal"));
      assert.equal(result.status, 0, result.stderr);
      const missingArch = existingArch === "arm64" ? "x64" : "arm64";
      const downloads = NodeFS.readFileSync(NodePath.join(directory, "download-args.jsonl"), "utf8")
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line));
      assert.lengthOf(downloads, 1);
      assert.equal(downloads[0][downloads[0].indexOf("--arch") + 1], missingArch);
      assert.equal(NodeFS.readFileSync(existingPayload, "utf8"), "bundled-payload");
      assert.equal(
        NodeFS.readFileSync(
          NodePath.join(directory, `build_darwin_${missingArch}/Release/webtransport.node`),
          "utf8",
        ),
        missingArch,
      );
    },
  );

  it.each(["missing", "failed", "empty"])(
    "fails an optional %s download without invoking source tools",
    (mode) => {
      const directory = fixture();
      if (mode !== "missing") fakeDownloader(directory);
      const result = runInstaller(directory, {
        npm_config_platform: "linux",
        npm_config_arch: "x64",
        npm_config_libc: "glibc",
        ...(mode === "failed" ? { TEST_DOWNLOAD_FAILURE: "1" } : {}),
        ...(mode === "empty" ? { TEST_EMPTY_DOWNLOAD: "1" } : {}),
      });
      assert.equal(result.status, 1, result.stderr);
      assert.include(result.stderr, "WebSocket remains supported");
      assert.isFalse(NodeFS.existsSync(NodePath.join(directory, "third_party")));
      assert.isFalse(NodeFS.existsSync(NodePath.join(directory, "build_linux_x64")));
    },
  );

  it("carries the optional roots and installer policy into stage metadata", () => {
    const optionalDependencies = selectCliRuntimeExternalDependencies(
      serverPackageJson.optionalDependencies,
    );
    assert.deepStrictEqual(Object.keys(optionalDependencies), [
      "@fails-components/webtransport",
      "@fails-components/webtransport-transport-http3-quiche",
    ]);
    const patchedDependencies = createStagePatchedDependencies(
      { [patchName]: patchPath, [wrapperPatchName]: wrapperPatchPath },
      optionalDependencies,
    );
    const workspace = createStageWorkspaceConfig({
      platform: "linux",
      arch: "arm64",
      patchedDependencies,
      allowBuilds: { "@fails-components/webtransport-transport-http3-quiche": true },
    });
    assert.deepStrictEqual(workspace.patchedDependencies, {
      [patchName]: patchPath,
      [wrapperPatchName]: wrapperPatchPath,
    });
    assert.equal(
      workspace.allowBuilds?.["@fails-components/webtransport-transport-http3-quiche"],
      true,
    );
  });
});

const serverRequire = NodeModule.createRequire(NodePath.join(repoRoot, "apps/server/package.json"));
const sourceNative = NodePath.join(
  repoRoot,
  "apps/server/node_modules/@fails-components/webtransport-transport-http3-quiche",
);
const nativeBinary = NodePath.join(
  sourceNative,
  // oxlint-disable-next-line t3code/no-global-process-runtime -- The real binary test must use this host, not an injected target.
  `build_${NodeOS.platform()}_${NodeOS.arch()}/Release/webtransport.node`,
);
const legacyNativeBinary = NodePath.join(sourceNative, "build/Release/webtransport.node");

const availableNative = (() => {
  try {
    serverRequire(NodeFS.existsSync(nativeBinary) ? nativeBinary : legacyNativeBinary);
    return true;
  } catch {
    // Optional prebuilts can be absent or require a newer host libc.
    return false;
  }
})();

it.skipIf(!availableNative)(
  "imports the available real addon and wrapper from a physical staged runtime closure",
  () => {
    const directory = fixture();
    const packages = [
      "@fails-components/webtransport",
      "@fails-components/webtransport-transport-http3-quiche",
      "debug",
      "ms",
      "bindings",
      "file-uri-to-path",
    ];
    for (const name of packages) {
      let source: string;
      if (name.startsWith("@fails-components/")) {
        source = NodeFS.realpathSync(NodePath.join(repoRoot, "apps/server/node_modules", name));
      } else {
        const nativeRequire = NodeModule.createRequire(
          serverRequire.resolve("@fails-components/webtransport-transport-http3-quiche"),
        );
        const runtimeRequire =
          name === "ms"
            ? NodeModule.createRequire(nativeRequire.resolve("debug"))
            : name === "file-uri-to-path"
              ? NodeModule.createRequire(nativeRequire.resolve("bindings"))
              : nativeRequire;
        source = NodePath.dirname(runtimeRequire.resolve(`${name}/package.json`));
      }
      NodeFS.cpSync(source, NodePath.join(directory, "node_modules", name), {
        recursive: true,
        dereference: true,
      });
    }
    const result = NodeChildProcess.spawnSync(
      process.execPath,
      [
        "--input-type=module",
        "-e",
        `
import {createRequire} from 'node:module';
import {existsSync} from 'node:fs';
import {dirname, join} from 'node:path';
import {pathToFileURL} from 'node:url';
const require = createRequire(join(process.cwd(), 'package.json'));
const entry = require.resolve('@fails-components/webtransport-transport-http3-quiche');
const root = dirname(dirname(entry));
const target = join(root, 'build_' + process.platform + '_' + process.arch);
require(join(existsSync(target) ? target : join(root, 'build'), 'Release/webtransport.node'));
const wrapper = require(join(process.cwd(), 'node_modules/@fails-components/webtransport/lib/index.node.js'));
if (typeof wrapper.Http3Server !== 'function') process.exit(42);
console.log('staged native preflight and wrapper import passed');
`,
      ],
      { cwd: directory, encoding: "utf8", timeout: 10_000 },
    );
    assert.equal(result.status, 0, result.stderr);
    assert.include(result.stdout, "staged native preflight and wrapper import passed");
  },
);

it.layer(NodeServices.layer)("WebTransport artifact target staging", (it) => {
  it.effect("refreshes an existing payload for the requested artifact target", () =>
    Effect.gen(function* () {
      const root = fixture();
      const packageDir = NodePath.join(
        root,
        "node_modules/@fails-components/webtransport-transport-http3-quiche",
      );
      const payload = writePayload(packageDir, "build_linux_x64", "same-target-cache");
      NodeFS.copyFileSync(
        NodePath.join(root, "install-prebuilt.js"),
        NodePath.join(packageDir, "install-prebuilt.js"),
      );
      NodeFS.writeFileSync(NodePath.join(packageDir, "package.json"), '{"type":"module"}');
      fakeDownloader(packageDir);
      yield* stageWebTransportPrebuilt({
        nodeModulesDir: NodePath.join(root, "node_modules"),
        platform: "linux",
        arch: "x64",
      });
      assert.equal(NodeFS.readFileSync(payload, "utf8"), "x64");
      assert.isTrue(NodeFS.existsSync(NodePath.join(packageDir, "download-args.jsonl")));
    }),
  );

  it.effect("replaces a cached host payload with the requested target", () =>
    Effect.gen(function* () {
      const root = fixture();
      const packageDir = NodePath.join(
        root,
        "node_modules/@fails-components/webtransport-transport-http3-quiche",
      );
      NodeFS.mkdirSync(NodePath.join(packageDir, "build_linux_x64/Release"), { recursive: true });
      NodeFS.copyFileSync(
        NodePath.join(root, "install-prebuilt.js"),
        NodePath.join(packageDir, "install-prebuilt.js"),
      );
      NodeFS.writeFileSync(NodePath.join(packageDir, "package.json"), '{"type":"module"}');
      NodeFS.writeFileSync(
        NodePath.join(packageDir, "build_linux_x64/Release/webtransport.node"),
        "host-cache",
      );
      fakeDownloader(packageDir);
      yield* stageWebTransportPrebuilt({
        nodeModulesDir: NodePath.join(root, "node_modules"),
        platform: "win",
        arch: "x64",
      });
      assert.isFalse(NodeFS.existsSync(NodePath.join(packageDir, "build_linux_x64")));
      assert.equal(
        NodeFS.readFileSync(
          NodePath.join(packageDir, "build_win32_x64/Release/webtransport.node"),
          "utf8",
        ),
        "x64",
      );
    }),
  );
  it.effect("keeps packaging successful when an optional target download fails", () =>
    Effect.gen(function* () {
      const root = fixture();
      const packageDir = NodePath.join(
        root,
        "node_modules/@fails-components/webtransport-transport-http3-quiche",
      );
      NodeFS.mkdirSync(packageDir, { recursive: true });
      NodeFS.copyFileSync(
        NodePath.join(root, "install-prebuilt.js"),
        NodePath.join(packageDir, "install-prebuilt.js"),
      );
      NodeFS.writeFileSync(NodePath.join(packageDir, "package.json"), '{"type":"module"}');
      NodeFS.writeFileSync(NodePath.join(packageDir, "fail-download"), "");
      fakeDownloader(packageDir);
      yield* stageWebTransportPrebuilt({
        nodeModulesDir: NodePath.join(root, "node_modules"),
        platform: "linux",
        arch: "arm64",
      });
      assert.isFalse(NodeFS.existsSync(NodePath.join(packageDir, "build_linux_arm64")));
      assert.isFalse(NodeFS.existsSync(NodePath.join(packageDir, "third_party")));
    }),
  );
});
