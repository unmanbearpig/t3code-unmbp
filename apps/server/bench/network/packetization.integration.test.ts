// @effect-diagnostics nodeBuiltinImport:off - Optional Linux integration proof runs the real isolated packet emulator.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import { it } from "@effect/vitest";

it.runIf(process.env.NETBENCH_INTEGRATION === "1")(
  "drops and delays exactly datagrams 3, 6 and 9 within a ten-datagram UDP GSO batch",
  () => {
    NodeChildProcess.execFileSync(
      "unshare",
      [
        "--user",
        "--map-root-user",
        "--net",
        process.execPath,
        NodePath.join(import.meta.dirname, "packetizationCheck.ts"),
        "--check",
      ],
      {
        env: { ...process.env, NETBENCH_HOST_NETNS: NodeFS.readlinkSync("/proc/self/ns/net") },
        timeout: 10000,
        stdio: "pipe",
      },
    );
  },
);
