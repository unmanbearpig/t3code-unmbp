// @effect-diagnostics nodeBuiltinImport:off - exercises the real installed backend JS with isolated UDP/native doubles.
import * as NodeChildProcess from "node:child_process";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import { assert, it } from "@effect/vitest";

const repoRoot = NodeURL.fileURLToPath(new URL("../../", import.meta.url));
const require = NodeModule.createRequire(NodePath.join(repoRoot, "apps/server/package.json"));
const nativeRoot = (() => {
  try {
    return NodePath.dirname(
      NodePath.dirname(require.resolve("@fails-components/webtransport-transport-http3-quiche")),
    );
  } catch {
    // A failed optional prebuilt install may remove the whole package.
    return undefined;
  }
})();
const fixture = NodeURL.fileURLToPath(
  new URL("./webtransport-shutdown.fixture.mjs", import.meta.url),
);

it
  .skipIf(!nativeRoot)
  .each(["server-stop", "buffered-processing", "client-close", "send-completion"])(
  "keeps queued native socket work inside the backend lifetime: %s",
  (mode) => {
    assert.ok(nativeRoot);
    const child = NodeChildProcess.spawnSync(process.execPath, [fixture, nativeRoot, mode], {
      encoding: "utf8",
      timeout: 10_000,
    });
    assert.equal(child.status, 0, child.stderr || child.stdout || String(child.error));
    const report = JSON.parse(child.stdout.trim());
    assert.equal(report.mode, mode);
    assert.equal(report.closed, true);
    assert.equal(report.closeFramesSent, 1);
  },
);
