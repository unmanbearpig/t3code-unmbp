// @effect-diagnostics nodeBuiltinImport:off - executes the actual optional backend in an isolated child.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import { assert, it } from "@effect/vitest";

const repoRoot = NodeURL.fileURLToPath(new URL("../../", import.meta.url));
const moduleDirectory = NodePath.join(repoRoot, "apps/server/node_modules");
const serverRequire = NodeModule.createRequire(NodePath.join(repoRoot, "apps/server/package.json"));
const nativeAvailable = (() => {
  try {
    const nativeEntry = serverRequire.resolve(
      "@fails-components/webtransport-transport-http3-quiche",
    );
    const nativeRoot = NodePath.dirname(NodePath.dirname(nativeEntry));
    for (const name of NodeFS.readdirSync(nativeRoot)) {
      if (name !== "build" && !name.startsWith("build_")) continue;
      try {
        serverRequire(NodePath.join(nativeRoot, name, "Release/webtransport.node"));
        return true;
      } catch {
        // Optional foreign binaries and old-libc hosts are not executable here.
      }
    }
    return false;
  } catch {
    return false;
  }
})();

// The real TLS fixture also needs OpenSSL to issue its disposable certificate.
const runtimeAvailable =
  nativeAvailable &&
  NodeChildProcess.spawnSync("openssl", ["version"], { timeout: 5_000 }).status === 0;

/** Copies the wrapper and its runtime closure, with no pnpm store or external symlinks. */
const stageRuntime = (directory: string) => {
  const nativeRequire = NodeModule.createRequire(
    serverRequire.resolve("@fails-components/webtransport-transport-http3-quiche"),
  );
  for (const name of [
    "@fails-components/webtransport",
    "@fails-components/webtransport-transport-http3-quiche",
    "debug",
    "ms",
    "bindings",
    "file-uri-to-path",
  ]) {
    const source = name.startsWith("@fails-components/")
      ? NodeFS.realpathSync(NodePath.join(moduleDirectory, name))
      : NodePath.dirname(
          (name === "ms"
            ? NodeModule.createRequire(nativeRequire.resolve("debug"))
            : name === "file-uri-to-path"
              ? NodeModule.createRequire(nativeRequire.resolve("bindings"))
              : nativeRequire
          ).resolve(`${name}/package.json`),
        );
    NodeFS.cpSync(source, NodePath.join(directory, "node_modules", name), {
      recursive: true,
      dereference: true,
    });
  }
  NodeFS.writeFileSync(NodePath.join(directory, "package.json"), '{"private":true}');
};

for (const mode of ["default", "byob"] as const) {
  it.skipIf(!runtimeAvailable)(
    `real staged backend bounds a paused ${mode} reader, resumes and settles stream lifecycle`,
    () => {
      const directory = NodeFS.mkdtempSync(
        NodePath.join(NodeOS.homedir(), "codex-tmp", "t3-quic-reader-test-"),
      );
      try {
        stageRuntime(directory);
        const child = NodeChildProcess.spawnSync(
          process.execPath,
          [
            NodeURL.fileURLToPath(
              new URL("./webtransport-backpressure.fixture.mjs", import.meta.url),
            ),
            NodePath.join(directory, "node_modules"),
            mode,
            directory,
          ],
          { encoding: "utf8", timeout: 30_000 },
        );
        assert.equal(child.status, 0, child.stderr || child.stdout || String(child.error));
        const line = child.stdout.split("\n").find((value) => value.startsWith('{"mode"'));
        assert.ok(line, child.stdout);
        const report = JSON.parse(line);
        assert.equal(report.mode, mode);
        assert.equal(report.bytes, 16 * 1024 * 1024);
        assert.equal(report.probes, 34);
        assert.isAtMost(report.paused.queuedBytes, report.flowWindow);
        assert.isAtMost(report.paused.backingUpperBound, 2 * report.flowWindow);
        assert.equal(report.resumed.bytes, report.bytes);
        assert.equal(report.resumed.writes, 256);
        for (const milestone of [
          "emptyFin",
          "cancel",
          "cancelWhileBackpressured",
          "reset",
          "sessionClose",
        ]) {
          assert.equal(report[milestone], true, milestone);
        }
      } finally {
        NodeFS.rmSync(directory, { recursive: true, force: true });
      }
    },
  );
}
