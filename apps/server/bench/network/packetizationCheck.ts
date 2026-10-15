// @effect-diagnostics nodeBuiltinImport:off - CLI preflight validates the real packet emulator in an isolated namespace.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { applyScenario, prepareLoopback } from "./impairments.ts";

/** Priming uses UDP with no replies; waiting for receipt leaves no setup traffic queued. */
export function primePacketPhase(count: number) {
  if (count === 0) return;
  NodeChildProcess.execFileSync(
    "python3",
    [NodePath.join(import.meta.dirname, "packetPhase.py"), String(count)],
    { stdio: "pipe", timeout: 5000 },
  );
}

/** Receiver assertions catch GSO batching that packet counters alone cannot detect. */
export function verifyPacketImpairments() {
  for (const mode of ["drop", "delay"] as const) {
    for (const phase of [0, 1, 2]) {
      applyScenario(
        mode === "drop"
          ? { name: "packetization-check-drop", nthDrop: 3 }
          : {
              name: "packetization-check-delay",
              delayMs: 25,
              nthDelay: { every: 3, extraMs: 300 },
            },
        104729,
        true,
      );
      primePacketPhase(phase);
      NodeChildProcess.execFileSync(
        "python3",
        [NodePath.join(import.meta.dirname, "packetizationCheck.py"), mode, String(phase)],
        { stdio: "pipe", timeout: 5000 },
      );
    }
  }
  applyScenario({ name: "clean" }, undefined, true);
}

if (process.argv.includes("--check")) {
  const scratch = NodeFS.mkdtempSync(
    NodePath.join(NodeOS.homedir(), "codex-tmp", "netbench-packets-"),
  );
  try {
    prepareLoopback(scratch, true);
    verifyPacketImpairments();
  } finally {
    NodeFS.rmSync(scratch, { recursive: true, force: true });
  }
}
